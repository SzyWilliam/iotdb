/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.mpp.sql.planner.plan.node;

import org.apache.iotdb.db.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.wal.utils.WALWriteUtils;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PlanNodeId {
  private String id;

  public PlanNodeId(String id) {
    this.id = id;
  }

  public String getId() {
    return this.id;
  }

  @Override
  public String toString() {
    return this.id;
  }

  @Override
  public int hashCode() {
    return this.id.hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof PlanNodeId) {
      return this.id.equals(((PlanNodeId) obj).getId());
    }
    return false;
  }

  public static PlanNodeId deserialize(ByteBuffer byteBuffer) {
    return new PlanNodeId(ReadWriteIOUtils.readString(byteBuffer));
  }

  public void serialize(ByteBuffer byteBuffer) {
    ReadWriteIOUtils.write(id, byteBuffer);
  }

  public int serializedSize() {
    return ReadWriteIOUtils.sizeToWrite(id);
  }

  public void serializeToWAL(IWALByteBufferView buffer) {
    WALWriteUtils.write(id, buffer);
  }

  public static PlanNodeId deserialize(DataInputStream stream) throws IOException {
    return new PlanNodeId(ReadWriteIOUtils.readString(stream));
  }
}
