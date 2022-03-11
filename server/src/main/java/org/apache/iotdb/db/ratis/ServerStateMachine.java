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

import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeAliasPlan;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

public class ServerStateMachine extends BaseStateMachine {
    private final Logger logger = LoggerFactory.getLogger(ServerStateMachine.class);

    @Override
    public CompletableFuture<Message> query(Message request) {
      try {
          PhysicalPlan plan = IoTDBConfig.PlanMessage.getPlanFrom(request);
          logger.info("{}-{} do query {}", getGroupId(), getId(), plan);
      } catch (Exception e) {
          e.printStackTrace();
      }
      return super.query(request);
    }

    @Override
    public void notifyTermIndexUpdated(long term, long index) {
        this.updateLastAppliedTermIndex(term, index);
        logger.info("{}-{} update term {} index {}", getGroupId(), getId(), term, index);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final RaftProtos.LogEntryProto entry = trx.getLogEntry();
        Message message = Message.valueOf(entry.getStateMachineLogEntry().getLogData());
        try {
            PhysicalPlan plan = IoTDBConfig.PlanMessage.getPlanFrom(message);

            updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
            logger.info("{}-{} apply trx {} term {} index {}", getGroupId(), getId(), plan,
                    entry.getTerm(), entry.getIndex());
        } catch (Exception e) {
            e.printStackTrace();
        }
        return super.applyTransaction(trx);
    }
}
