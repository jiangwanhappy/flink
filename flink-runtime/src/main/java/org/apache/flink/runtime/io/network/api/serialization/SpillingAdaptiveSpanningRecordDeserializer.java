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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.CloseableIterator;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.INTERMEDIATE_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.LAST_RECORD_FROM_BUFFER;
import static org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer.DeserializationResult.PARTIAL_RECORD;

/** @param <T> The type of the record to be deserialized. */
public class SpillingAdaptiveSpanningRecordDeserializer<T extends IOReadableWritable>
        implements RecordDeserializer<T> {
    public static final int DEFAULT_THRESHOLD_FOR_SPILLING = 5 * 1024 * 1024; // 5 MiBytes
    public static final int DEFAULT_FILE_BUFFER_SIZE = 2 * 1024 * 1024;
    private static final int MIN_THRESHOLD_FOR_SPILLING = 100 * 1024; // 100 KiBytes
    private static final int MIN_FILE_BUFFER_SIZE = 50 * 1024; // 50 KiBytes

    static final int LENGTH_BYTES = Integer.BYTES;

    private final NonSpanningWrapper nonSpanningWrapper;//主要存放数据的地方，先从这找

    private final SpanningWrapper spanningWrapper;

    @Nullable private Buffer currentBuffer;//需要被反序列化的buffer

    public SpillingAdaptiveSpanningRecordDeserializer(String[] tmpDirectories) {
        this(tmpDirectories, DEFAULT_THRESHOLD_FOR_SPILLING, DEFAULT_FILE_BUFFER_SIZE);
    }

    public SpillingAdaptiveSpanningRecordDeserializer(
            String[] tmpDirectories, int thresholdForSpilling, int fileBufferSize) {
        nonSpanningWrapper = new NonSpanningWrapper();
        spanningWrapper =
                new SpanningWrapper(
                        tmpDirectories,
                        Math.max(thresholdForSpilling, MIN_THRESHOLD_FOR_SPILLING),
                        Math.max(fileBufferSize, MIN_FILE_BUFFER_SIZE));
    }
//根据buffer中的数据的长度初始化nonSpanningWrapper（或spanningWrapper）的position和limit
    @Override
    public void setNextBuffer(Buffer buffer) throws IOException {
        currentBuffer = buffer;

        int offset = buffer.getMemorySegmentOffset(); // 从offset位置开始读取
        MemorySegment segment = buffer.getMemorySegment();
        int numBytes = buffer.getSize();

        // check if some spanning record deserialization is pending
        if (spanningWrapper.getNumGatheredBytes() > 0) {
            spanningWrapper.addNextChunkFromMemorySegment(segment, offset, numBytes);
        } else {
            nonSpanningWrapper.initializeFromMemorySegment(segment, offset, numBytes + offset);
        }
    }

    @Override
    public CloseableIterator<Buffer> getUnconsumedBuffer() throws IOException {
        return nonSpanningWrapper.hasRemaining()
                ? nonSpanningWrapper.getUnconsumedSegment()
                : spanningWrapper.getUnconsumedSegment();
    }
    //反序列化this(nonSpanningWrapper)里的一条记录（这个记录是什么类型，由serializer决定）并保存到target的instance里
    @Override
    public DeserializationResult getNextRecord(T target) throws IOException {
        // always check the non-spanning wrapper first.
        // this should be the majority of the cases for small records
        // for large records, this portion of the work is very small in comparison anyways
//反序列化this(nonSpanningWrapper)里的一条记录（这个记录是什么类型，由target的serializer决定）并保存到target的instance里
        final DeserializationResult result = readNextRecord(target);
        if (result.isBufferConsumed()) {
            currentBuffer.recycleBuffer();
            currentBuffer = null;
        }
        return result;
    }
    //反序列化this(nonSpanningWrapper)里的一条记录（这个记录是什么类型，由target的serializer决定）并保存到target的instance里
    private DeserializationResult readNextRecord(T target) throws IOException {
        if (nonSpanningWrapper.hasCompleteLength()) {//未读的部分（limit - position） >= LENGTH_BYTES
            return readNonSpanningRecord(target);

        } else if (nonSpanningWrapper.hasRemaining()) {
            nonSpanningWrapper.transferTo(spanningWrapper.lengthBuffer);
            return PARTIAL_RECORD;

        } else if (spanningWrapper.hasFullRecord()) {
            target.read(spanningWrapper.getInputView());
            spanningWrapper.transferLeftOverTo(nonSpanningWrapper);
            return nonSpanningWrapper.hasRemaining()
                    ? INTERMEDIATE_RECORD_FROM_BUFFER
                    : LAST_RECORD_FROM_BUFFER;

        } else {
            return PARTIAL_RECORD;
        }
    }

    private DeserializationResult readNonSpanningRecord(T target) throws IOException {
        // following three calls to nonSpanningWrapper from object oriented design would be better
        // to encapsulate inside nonSpanningWrapper, but then nonSpanningWrapper.readInto equivalent
        // would have to return a tuple of DeserializationResult and recordLen, which would affect
        // performance too much
        int recordLen = nonSpanningWrapper.readInt(); // 读取该条数据的长度
        if (nonSpanningWrapper.canReadRecord(
                recordLen)) { // 看是否recordLen <= (this.limit - this.position)
            return nonSpanningWrapper.readInto(target);//反序列化nonSpanningWrapper里的一条记录（这个记录是什么类型，由target的serializer决定）并保存到target的instance里
        } else {
            spanningWrapper.transferFrom(nonSpanningWrapper, recordLen);
            return PARTIAL_RECORD;
        }
    }

    @Override
    public void clear() {
        if (currentBuffer != null && !currentBuffer.isRecycled()) {
            currentBuffer.recycleBuffer();
            currentBuffer = null;
        }
        nonSpanningWrapper.clear();
        spanningWrapper.clear();
    }
}
