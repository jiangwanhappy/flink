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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.join.interval;

import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

/**
 * Collector to wrap a [[org.apache.flink.table.dataformat.RowData]] and to track whether a row has
 * been emitted by the inner collector.
 */
class EmitAwareCollector implements Collector<RowData> {

    private boolean emitted = false;//标识是否已经发送到下游
    private Collector<RowData> innerCollector;//用于输出数据的Collector

    void reset() {
        emitted = false;
    }

    boolean isEmitted() {
        return emitted;
    }

    void setInnerCollector(Collector<RowData> innerCollector) {
        this.innerCollector = innerCollector;
    }

    @Override
    public void collect(RowData record) {
        emitted = true;
        innerCollector.collect(record);
    }

    @Override
    public void close() {
        innerCollector.close();
    }
}
