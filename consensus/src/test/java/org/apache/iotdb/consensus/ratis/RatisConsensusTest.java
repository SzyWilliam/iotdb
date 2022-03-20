/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.consensus.ratis;

import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.*;
import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.response.ConsensusReadResponse;
import org.apache.iotdb.consensus.common.response.ConsensusWriteResponse;
import org.apache.iotdb.consensus.statemachine.IStateMachine;
import org.apache.iotdb.service.rpc.thrift.TSStatus;

import org.apache.ratis.util.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class RatisConsensusTest {

  private static class TestDataSet implements DataSet {
    private int number;

    public void setNumber(int number) {
      this.number = number;
    }

    public int getNumber() {
      return number;
    }
  }

  private static class TestRequest {
    private int cmd;

    public TestRequest(ByteBuffer buffer) {
      cmd = buffer.getInt();
    }

    public boolean isIncr() {
      return cmd == 1;
    }
  }

  private static class IntegerCounter implements IStateMachine {
    AtomicInteger integer;

    @Override
    public void start() {
      integer = new AtomicInteger(0);
    }

    @Override
    public void stop() {}

    @Override
    public TSStatus write(IConsensusRequest IConsensusRequest) {
      ByteBufferConsensusRequest request = (ByteBufferConsensusRequest) IConsensusRequest;
      TestRequest testRequest = new TestRequest(request.getContent());
      if (testRequest.isIncr()) {
        integer.incrementAndGet();
      }
      return new TSStatus(200);
    }

    @Override
    public DataSet read(IConsensusRequest IConsensusRequest) {
      TestDataSet dataSet = new TestDataSet();
      dataSet.setNumber(integer.get());
      return dataSet;
    }
  }

  private static class TestSerializer implements IRatisSerializer {

    @Override
    public ByteBuffer serializeDataSet(DataSet dataSet) {
      TestDataSet testDataSet = (TestDataSet) dataSet;
      ByteBuffer buffer = ByteBuffer.allocate(4);
      buffer.putInt(testDataSet.getNumber());
      buffer.flip();
      return buffer;
    }

    @Override
    public DataSet deserializeDataSet(ByteBuffer buffer) {
      TestDataSet dataSet = new TestDataSet();
      dataSet.setNumber(buffer.getInt());
      return dataSet;
    }
  }

  @Test
  public void basicConsensus() throws Exception {

    // 1. construct a consensus group of 3 peers
    ConsensusGroupId gid = new ConsensusGroupId(GroupType.Config, 1L);

    List<Peer> peers = new ArrayList<>();
    Peer peer0 = new Peer(gid, new Endpoint("127.0.0.1", 6000));
    Peer peer1 = new Peer(gid, new Endpoint("127.0.0.1", 6001));
    Peer peer2 = new Peer(gid, new Endpoint("127.0.0.1", 6002));
    peers.add(peer0);
    peers.add(peer1);
    peers.add(peer2);

    List<File> peersStorage = new ArrayList<>();
    peersStorage.add(new File("./target/1/"));
    peersStorage.add(new File("./target/2/"));
    peersStorage.add(new File("./target/3/"));
    for (File dir : peersStorage) {
      dir.mkdirs();
    }

    ConsensusGroup group = new ConsensusGroup(gid, peers);

    // 2. Start 3 Consensus Service of each endpoint
    List<IConsensus> servers = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      servers.add(
          RatisConsensus.newBuilder()
              .setEndpoint(peers.get(i).getEndpoint())
              .setStateMachineRegistry(groupId -> new IntegerCounter())
              .setStorageDir(peersStorage.get(i))
              .setSerializer(new TestSerializer())
              .build());
      servers.get(i).start();
      ;
    }

    // 4. Add a new group
    servers.get(0).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(1).addConsensusGroup(group.getGroupId(), group.getPeers());
    servers.get(2).addConsensusGroup(group.getGroupId(), group.getPeers());

    // 5. Do Consensus 10
    doConsensus(servers.get(0), group.getGroupId(), 10, 10);

    // 6. Remove two Peers from Group (peer 0 and peer 2)
    servers.get(0).transferLeader(gid, peer1);
    // first use removePeer to inform the group leader of configuration change
    servers.get(1).removePeer(gid, peer0);
    servers.get(1).removePeer(gid, peer2);
    // then use removeConsensusGroup to clean up removed Consensus-Peer's states
    servers.get(0).removeConsensusGroup(gid);
    servers.get(2).removeConsensusGroup(gid);

    // 7. try consensus again with one peer
    doConsensus(servers.get(1), gid, 10, 20);

    // 8. add two peers back
    // first notify these new peers, let them initialize
    servers.get(0).addConsensusGroup(gid, peers);
    servers.get(2).addConsensusGroup(gid, peers);
    // then use addPeer to inform the group leader of configuration change
    servers.get(1).addPeer(gid, peer0);
    servers.get(1).addPeer(gid, peer2);

    // 9. try consensus with all 3 peers
    doConsensus(servers.get(2), gid, 10, 30);

    // 10. again, group contains only peer0
    servers.get(0).changePeer(group.getGroupId(), Collections.singletonList(peer0));
    servers.get(1).removeConsensusGroup(group.getGroupId());
    servers.get(2).removeConsensusGroup(group.getGroupId());

    // 11. try consensus with only peer0
    doConsensus(servers.get(0), gid, 10, 40);

    // 12. wrap up and delete temp files
    for (File file : peersStorage) {
      FileUtils.deleteFully(file);
    }
  }

  private void doConsensus(IConsensus consensus, ConsensusGroupId gid, int count, int target)
      throws Exception {

    // do write
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    for (int i = 0; i < count; i++) {
      executorService.submit(
          () -> {
            ByteBuffer incr = ByteBuffer.allocate(4);
            incr.putInt(1);
            incr.flip();
            ByteBufferConsensusRequest incrReq = new ByteBufferConsensusRequest(incr);

            ConsensusWriteResponse response = consensus.write(gid, incrReq);
            if (response.getException() != null) {
              response.getException().printStackTrace(System.out);
            }
            Assert.assertEquals(response.getStatus(), new TSStatus(200));
          });
    }
    executorService.shutdown();
    executorService.awaitTermination(count * 500L, TimeUnit.MILLISECONDS);

    ByteBuffer get = ByteBuffer.allocate(4);
    get.putInt(2);
    get.flip();
    ByteBufferConsensusRequest getReq = new ByteBufferConsensusRequest(get);

    // Check we reached a consensus
    ConsensusReadResponse response = consensus.read(gid, getReq);
    TestDataSet result = (TestDataSet) response.getDataset();
    Assert.assertEquals(target, result.getNumber());
  }
}
