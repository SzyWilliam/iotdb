/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.index.common;


import org.apache.iotdb.db.exception.IoTDBException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.rpc.TSStatusCode;

public class IndexManagerException extends QueryProcessException {

  private static final long serialVersionUID = 1233035822169501915L;

  public IndexManagerException(String message) {
    super(message, TSStatusCode.INDEX_PROCESS_ERROR.getStatusCode());
  }

  public IndexManagerException(String message, int errorCode) {
    super(message, errorCode);
  }

  public IndexManagerException(IoTDBException e) {
    super(e);
  }
}
