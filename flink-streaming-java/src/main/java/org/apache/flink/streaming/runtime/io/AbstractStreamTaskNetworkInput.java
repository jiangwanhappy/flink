/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.runtime.plugable.DeserializationDelegate;
import org.apache.flink.runtime.plugable.NonReusingDeserializationDelegate;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointedInputGate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamstatus.StatusWatermarkValve;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base class for network-based StreamTaskInput where each channel has a designated {@link
 * RecordDeserializer} for spanning records. Specific implementation bind it to a specific {@link
 * RecordDeserializer}.
 */
public abstract class AbstractStreamTaskNetworkInput<
                T, R extends RecordDeserializer<DeserializationDelegate<StreamElement>>>
        implements StreamTaskInput<T> {
    protected final CheckpointedInputGate checkpointedInputGate;//主要上游所有channel数据保存地
    protected final DeserializationDelegate<StreamElement> deserializationDelegate;//根据deserializationDelegate里的反序列化器反序列化数据并保存到其（deserializationDelegate）instance里
    protected final TypeSerializer<T> inputSerializer;//反序列化器，就是deserializationDelegate里的反序列化器
    protected final Map<InputChannelInfo, R> recordDeserializers;//対应InputChannelInfo的序列化后的数据保存的地方，deserializationDelegate反序列化的就是这里的数据
    protected final Map<InputChannelInfo, Integer> flattenedChannelIndices = new HashMap<>();//key为inputchannelinfo
    /** Valve that controls how watermarks and stream statuses are forwarded. */
    protected final StatusWatermarkValve statusWatermarkValve;//记录inputgate里所有inputchannel的状态，即InputChannelStatus，还有经过所有channel计算的watermark

    protected final int inputIndex;
    private InputChannelInfo lastChannel = null;//获取到上一条数据的channel信息
    private R currentRecordDeserializer = null;

    public AbstractStreamTaskNetworkInput(
            CheckpointedInputGate checkpointedInputGate,
            TypeSerializer<T> inputSerializer,//反序列化器
            StatusWatermarkValve statusWatermarkValve,
            int inputIndex,
            Map<InputChannelInfo, R> recordDeserializers) {
        super();
        this.checkpointedInputGate = checkpointedInputGate;
        deserializationDelegate =
                new NonReusingDeserializationDelegate<>(
                        new StreamElementSerializer<>(inputSerializer));
        this.inputSerializer = inputSerializer;

        for (InputChannelInfo i : checkpointedInputGate.getChannelInfos()) {
            flattenedChannelIndices.put(i, flattenedChannelIndices.size());
        }

        this.statusWatermarkValve = checkNotNull(statusWatermarkValve);
        this.inputIndex = inputIndex;
        this.recordDeserializers = checkNotNull(recordDeserializers);
    }
//反序列化currentRecordDeserializer里的一条记录并进行处理发送到下游
    @Override
    public InputStatus emitNext(DataOutput<T> output) throws Exception {

        while (true) {
            // get the stream element from the deserializer
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result;
                try {//反序列化currentRecordDeserializer(nonSpanningWrapper)里的一条记录（这个记录是什么类型，由target的serializer决定）并保存到deserializationDelegate的instance里
                    result = currentRecordDeserializer.getNextRecord(deserializationDelegate); // 将反序列化的数据保存到deserializationDelegate的Instance中
                } catch (IOException e) {
                    throw new IOException(
                            String.format("Can't get next record for channel %s", lastChannel), e);
                }
                if (result.isBufferConsumed()) { // 是否此buffer的数据都已经消费完成，即此buffer里的所有记录都已经被消费掉
                    currentRecordDeserializer = null;
                }

                if (result.isFullRecord()) { // 如果是一条完整的数据
                    processElement(deserializationDelegate.getInstance(), output);
                    return InputStatus.MORE_AVAILABLE;
                }
            }

            Optional<BufferOrEvent> bufferOrEvent = checkpointedInputGate.pollNext();
            if (bufferOrEvent.isPresent()) { // 第一次currentRecordDeserializer为null，进入这个if判断时，currentRecordDeserializer有赋值
                // return to the mailbox after receiving a checkpoint barrier to avoid processing of
                // data after the barrier before checkpoint is performed for unaligned checkpoint
                // mode
                if (bufferOrEvent.get().isBuffer()) {
                    processBuffer(bufferOrEvent.get());//获取buffer所在channel的反序列化器（如把数据赋值到nonSpanningWrapper） 并初始化反序列器
                } else {
                    return processEvent(bufferOrEvent.get());
                }
            } else {
                if (checkpointedInputGate.isFinished()) {
                    checkState(
                            checkpointedInputGate.getAvailableFuture().isDone(),
                            "Finished BarrierHandler should be available");
                    return InputStatus.END_OF_INPUT;
                }
                return InputStatus.NOTHING_AVAILABLE;
            }
        }
    }

    private void processElement(StreamElement recordOrMark, DataOutput<T> output) throws Exception {
        if (recordOrMark.isRecord()) {
            output.emitRecord(recordOrMark.asRecord());
        } else if (recordOrMark.isWatermark()) {
            statusWatermarkValve.inputWatermark(
                    recordOrMark.asWatermark(), flattenedChannelIndices.get(lastChannel), output);
        } else if (recordOrMark.isLatencyMarker()) {
            output.emitLatencyMarker(recordOrMark.asLatencyMarker());
        } else if (recordOrMark.isStreamStatus()) {
            statusWatermarkValve.inputStreamStatus(
                    recordOrMark.asStreamStatus(),
                    flattenedChannelIndices.get(lastChannel),
                    output);
        } else {
            throw new UnsupportedOperationException("Unknown type of StreamElement");
        }
    }

    protected InputStatus processEvent(BufferOrEvent bufferOrEvent) {
        // Event received
        final AbstractEvent event = bufferOrEvent.getEvent();
        // TODO: with checkpointedInputGate.isFinished() we might not need to support any events on
        // this level.
        if (event.getClass() == EndOfPartitionEvent.class) {
            // release the record deserializer immediately,
            // which is very valuable in case of bounded stream
            releaseDeserializer(bufferOrEvent.getChannelInfo());
        } else if (event.getClass() == EndOfChannelStateEvent.class) {
            if (checkpointedInputGate.allChannelsRecovered()) {
                return InputStatus.END_OF_RECOVERY;
            }
        }
        return InputStatus.MORE_AVAILABLE;
    }
//获取buffer所在channel的反序列化器 并初始化反序列器
    protected void processBuffer(BufferOrEvent bufferOrEvent) throws IOException {
        lastChannel = bufferOrEvent.getChannelInfo();
        checkState(lastChannel != null);
        currentRecordDeserializer = getActiveSerializer(bufferOrEvent.getChannelInfo());
        checkState(
                currentRecordDeserializer != null,
                "currentRecordDeserializer has already been released");

        currentRecordDeserializer.setNextBuffer(bufferOrEvent.getBuffer());//根据buffer中的数据的长度初始化nonSpanningWrapper（或spanningWrapper）的position和limit
    }

    protected R getActiveSerializer(InputChannelInfo channelInfo) {
        return recordDeserializers.get(channelInfo);
    }

    @Override
    public int getInputIndex() {
        return inputIndex;
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        if (currentRecordDeserializer != null) {
            return AVAILABLE;
        }
        return checkpointedInputGate.getAvailableFuture();
    }

    @Override
    public void close() throws IOException {
        // release the deserializers . this part should not ever fail
        for (InputChannelInfo channelInfo : new ArrayList<>(recordDeserializers.keySet())) {
            releaseDeserializer(channelInfo);
        }
    }

    protected void releaseDeserializer(InputChannelInfo channelInfo) {
        R deserializer = recordDeserializers.get(channelInfo);
        if (deserializer != null) {
            // recycle buffers and clear the deserializer.
            deserializer.clear();
            recordDeserializers.remove(channelInfo);
        }
    }
}
