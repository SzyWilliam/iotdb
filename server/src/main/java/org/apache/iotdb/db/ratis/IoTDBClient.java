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
import org.apache.iotdb.db.qp.physical.crud.QueryIndexPlan;
import org.apache.iotdb.db.qp.physical.crud.QueryPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

import java.io.IOException;
import java.nio.ByteBuffer;

public class IoTDBClient {
  private RaftClient client;

  private RaftClient buildClient() {
    RaftProperties raftProperties = new RaftProperties();
    RaftClient.Builder builder =
        RaftClient.newBuilder()
            .setProperties(raftProperties)
            .setRaftGroup(IoTDBConfig.RAFT_GROUP)
            .setClientRpc(
                new GrpcFactory(new Parameters())
                    .newRaftClientRpc(ClientId.randomId(), raftProperties));
    return builder.build();
  }

  public IoTDBClient() {
    client = buildClient();
  }



  public boolean submitPhysicalPlanSync(PhysicalPlan plan) throws IOException{
    Message message = IoTDBConfig.PlanMessage.getMessage(plan, plan.getClass());
    RaftClientReply reply = client.io().send(message);
    System.out.println("reply for " + plan.toString() + ": " + reply.getMessage().getContent().toString());
    return true;
  }

  public QueryDataSet submitQueryPlanAsync(PhysicalPlan plan) throws IOException{
    Message message = IoTDBConfig.PlanMessage.getMessage(plan, plan.getClass());
    RaftClientReply reply = client.io().send(message);
    System.out.println("reply for " + plan.toString() + ": " + reply.getMessage().getContent().toString());
    return null;
  }

  public static void main(String[] args) throws Exception{
    IoTDBClient client = new IoTDBClient();
    client.submitPhysicalPlanSync(new ChangeAliasPlan(new PartialPath("root.ln.d0.s0"), "alias"));
    client.submitQueryPlanAsync(new ChangeAliasPlan(new PartialPath("root.ln.d0.s0"), "query"));
  }
}
