/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.ratis;

import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.NetUtils;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Scanner;

public class IoTDBServer {
  public static void main(String[] args) throws Exception {
    RaftPeer currentPeer = IoTDBConfig.PEERS.get(Integer.parseInt(args[0]));
    RaftProperties properties = new RaftProperties();

    // set the storage directory (different for each peer) in RaftProperty object
    File raftStorageDir = new File("./server/" + currentPeer.getId().toString());
    RaftServerConfigKeys.setStorageDir(properties, Collections.singletonList(raftStorageDir));

    // set the port which server listen to in RaftProperty object
    final int port = NetUtils.createSocketAddr(currentPeer.getAddress()).getPort();
    GrpcConfigKeys.Server.setPort(properties, port);

    ServerStateMachine serverStateMachine = new ServerStateMachine();

    RaftServer server =
        RaftServer.newBuilder()
            .setGroup(IoTDBConfig.RAFT_GROUP)
            .setProperties(properties)
            .setServerId(currentPeer.getId())
            .setStateMachine(serverStateMachine)
            .build();
    server.start();

    // exit when any input entered
    Scanner scanner = new Scanner(System.in);
    scanner.nextLine();
    server.close();
  }
}
