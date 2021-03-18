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
package org.apache.iotdb.db.index.common;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.utils.datastructure.TVList;

public class DistSeries {

  public double dist;
  public TVList tvList;
  public PartialPath partialPath;
  public double backDist= Double.NaN;

  public DistSeries(double dist, TVList tvList, PartialPath partialPath) {
    this.dist = dist;
    this.tvList = tvList;
    this.partialPath = partialPath;
  }

  public String toString() {
    return "(" + partialPath + "," + dist + ":" + tvList + ")";
  }
}
