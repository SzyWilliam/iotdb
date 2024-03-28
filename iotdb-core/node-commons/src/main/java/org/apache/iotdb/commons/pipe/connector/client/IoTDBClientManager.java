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

package org.apache.iotdb.commons.pipe.connector.client;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import java.util.List;

public abstract class IoTDBClientManager {

  protected final List<TEndPoint> endPointList;
  protected long currentClientIndex = 0;

  protected final boolean useLeaderCache;

  // This flag indicates whether the receiver supports mods transferring if
  // it is a DataNode receiver. The flag is useless for configNode receiver.
  protected boolean supportModsIfIsDataNodeReceiver = true;

  protected IoTDBClientManager(List<TEndPoint> endPointList, boolean useLeaderCache) {
    this.endPointList = endPointList;
    this.useLeaderCache = useLeaderCache;
  }

  public boolean supportModsIfIsDataNodeReceiver() {
    return supportModsIfIsDataNodeReceiver;
  }
}
