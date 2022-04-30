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

package org.apache.iotdb.consensus.statemachine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;

import java.io.File;
import java.util.function.Function;

public interface IStateMachine {

  interface Registry extends Function<ConsensusGroupId, IStateMachine> {}

  void start();

  void stop();

  /**
   * apply a write-request from user
   *
   * @param IConsensusRequest write request
   */
  TSStatus write(IConsensusRequest IConsensusRequest);

  /**
   * read local data and return
   *
   * @param IConsensusRequest read request
   */
  DataSet read(IConsensusRequest IConsensusRequest);

  boolean takeSnapshot(File snapshotDir);

  void loadSnapshot(File latestSnapshotRootDir);
}
