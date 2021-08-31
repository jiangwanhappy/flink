/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.streamstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput.DataOutput;
import org.apache.flink.util.Preconditions;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A {@code StatusWatermarkValve} embodies the logic of how {@link Watermark} and {@link
 * StreamStatus} are propagated to downstream outputs, given a set of one or multiple input channels
 * that continuously receive them. Usages of this class need to define the number of input channels
 * that the valve needs to handle, as well as provide a implementation of {@link DataOutput}, which
 * is called by the valve only when it determines a new watermark or stream status can be
 * propagated.
 *///记录所有inputchannel的状态，即InputChannelStatus，还有经过所有channel计算的watermark
@Internal
public class StatusWatermarkValve {

    // ------------------------------------------------------------------------
    //	Runtime state for watermark & stream status output determination
    // ------------------------------------------------------------------------

    /**
     * Array of current status of all input channels. Changes as watermarks & stream statuses are
     * fed into the valve.
     */
    private final InputChannelStatus[] channelStatuses;
//发送watermark的2种情况:1、lastOutputStreamStatus和此channel的StreamStatus都为active，发送所有已Aligned的channel的最小watermark
    /** The last watermark emitted from the valve. */ // 2、当所有channel都要变成idle时，找出所有channel（不管是否align）的最大waterMark
    private long lastOutputWatermark;//上次发送的watermark（所有已Aligned的channel的最小watermark）

    /** The last stream status emitted from the valve. *///可以看出，当所有都为idle时，才发送idle，当有一个（不需要全部）从idle变为active（之后再有其他的变为active，就不发送了，因为lastOutputStreamStatus是active，没有变动），就发送active
    private StreamStatus lastOutputStreamStatus;//上次发送的StreamStatus
//当lastOutputStreamStatus有变动时，即从idle变为active，或从active变成idle且是所有channel都变为idle时，才需要重新发送lastOutputStreamStatus发送到下游
    /**
     * Returns a new {@code StatusWatermarkValve}.
     *
     * @param numInputChannels the number of input channels that this valve will need to handle
     */
    public StatusWatermarkValve(int numInputChannels) {
        checkArgument(numInputChannels > 0);
        this.channelStatuses = new InputChannelStatus[numInputChannels];
        for (int i = 0; i < numInputChannels; i++) {
            channelStatuses[i] = new InputChannelStatus();
            channelStatuses[i].watermark = Long.MIN_VALUE;
            channelStatuses[i].streamStatus = StreamStatus.ACTIVE;
            channelStatuses[i].isWatermarkAligned = true;
        }

        this.lastOutputWatermark = Long.MIN_VALUE;
        this.lastOutputStreamStatus = StreamStatus.ACTIVE;
    }

    /**
     * Feed a {@link Watermark} into the valve. If the input triggers the valve to output a new
     * Watermark, {@link DataOutput#emitWatermark(Watermark)} will be called to process the new
     * Watermark.
     *只有当lastOutputStreamStatus为active且此channel的streamStatus为active，才有可能发送watermark
     * @param watermark the watermark to feed to the valve
     * @param channelIndex the index of the channel that the fed watermark belongs to (index
     *     starting from 0)
     *///当此watermark大于対应channel的watermark，则重新对比所有isWatermarkAligned为true的channel的watermark的最小值，当大于lastOutputWatermark，才发送新的最小值
    public void inputWatermark(Watermark watermark, int channelIndex, DataOutput<?> output)
            throws Exception {
        // ignore the input watermark if its input channel, or all input channels are idle (i.e.
        // overall the valve is idle).
        if (lastOutputStreamStatus.isActive()
                && channelStatuses[channelIndex].streamStatus.isActive()) {
            long watermarkMillis = watermark.getTimestamp();

            // if the input watermark's value is less than the last received watermark for its input
            // channel, ignore it also.
            if (watermarkMillis > channelStatuses[channelIndex].watermark) {
                channelStatuses[channelIndex].watermark = watermarkMillis;

                // previously unaligned input channels are now aligned if its watermark has caught
                // up
                if (!channelStatuses[channelIndex].isWatermarkAligned
                        && watermarkMillis >= lastOutputWatermark) {
                    channelStatuses[channelIndex].isWatermarkAligned = true;
                }

                // now, attempt to find a new min watermark across all aligned channels
                findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
            }
        }
    }

    /**
     * Feed a {@link StreamStatus} into the valve. This may trigger the valve to output either a new
     * Stream Status, for which {@link DataOutput#emitStreamStatus(StreamStatus)} will be called, or
     * a new Watermark, for which {@link DataOutput#emitWatermark(Watermark)} will be called.
     *
     * @param streamStatus the stream status to feed to the valve
     * @param channelIndex the index of the channel that the fed stream status belongs to (index
     *     starting from 0)
     */
    public void inputStreamStatus(StreamStatus streamStatus, int channelIndex, DataOutput<?> output)
            throws Exception {
        // only account for stream status inputs that will result in a status change for the input
        // channel
        if (streamStatus.isIdle() && channelStatuses[channelIndex].streamStatus.isActive()) {
            // handle active -> idle toggle for the input channel
            channelStatuses[channelIndex].streamStatus = StreamStatus.IDLE;

            // the channel is now idle, therefore not aligned
            channelStatuses[channelIndex].isWatermarkAligned = false;

            // if all input channels of the valve are now idle, we need to output an idle stream
            // status from the valve (this also marks the valve as idle)
            if (!InputChannelStatus.hasActiveChannels(channelStatuses)) {
//只有此channel是active的（也要改成idle了），找出所有channel中的最大watermark，且大于lastOutputWatermark，则发送
                // now that all input channels are idle and no channels will continue to advance its
                // watermark,
                // we should "flush" all watermarks across all channels; effectively, this means
                // emitting现在因为所以channel都是idle了，所以需要flush所有channel的watermark，即发送所有channel中的watermark最大值就可以
                // the max watermark across all channels as the new watermark. Also, since we
                // already try to advance
                // the min watermark as channels individually become IDLE, here we only need to
                // perform the flush因为所有的channel在变成IDLE之前都已发送所有channel中的最小watermark
                // if the watermark of the last active channel that just became idle is the current
                // min watermark. 如果此最后active的channel（之后要变成idle）的watermark==lastOutputWatermark，则执行flush（发送所有channel的watermark最大值）
                if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {//这里只有可能是==，因为在channel变为idle之前，都会发送所有AlignedChannel中找到最小watermark，而现在就只有此channel（也要变成idle）是active，所以肯定是==
                    findAndOutputMaxWatermarkAcrossAllChannels(output);
                }

                lastOutputStreamStatus = StreamStatus.IDLE;
                output.emitStreamStatus(lastOutputStreamStatus);
            } else if (channelStatuses[channelIndex].watermark == lastOutputWatermark) {//如果还有channel是active的，且此active的channel（之后要变成idle）的watermark==lastOutputWatermark
                // if the watermark of the channel that just became idle equals the last output证明之前所有channel的最小watermark是此要变成idle的channel，所有要重新在所有AlignedChannel中找到最小watermark（findAndOutputNewMinWatermarkAcrossAlignedChannels）
                // watermark (the previous overall min watermark), we may be able to find a new
                // min watermark from the remaining aligned channels //还有一种可能是channelStatuses[channelIndex].watermark > lastOutputWatermark,这种情况就不需要再找一遍所有channel最小watermark了
                findAndOutputNewMinWatermarkAcrossAlignedChannels(output);
            }
        } else if (streamStatus.isActive() && channelStatuses[channelIndex].streamStatus.isIdle()) {
            // handle idle -> active toggle for the input channel
            channelStatuses[channelIndex].streamStatus = StreamStatus.ACTIVE;

            // if the last watermark（最终的watermark） of the input channel, before it was marked idle, is still
            // larger than
            // the overall last output watermark of the valve, then we can set the channel to be
            // aligned already.
            if (channelStatuses[channelIndex].watermark >= lastOutputWatermark) {
                channelStatuses[channelIndex].isWatermarkAligned = true;
            }

            // if the valve was previously marked to be idle, mark it as active and output an active
            // stream
            // status because at least one of the input channels is now active
            if (lastOutputStreamStatus.isIdle()) {
                lastOutputStreamStatus = StreamStatus.ACTIVE;
                output.emitStreamStatus(lastOutputStreamStatus);
            }
        }
    }
//所有AlignedChannel（isWatermarkAligned为true的channel）中找到最小watermark，如果此watermark大于lastOutputWatermark，就发送到下游
    private void findAndOutputNewMinWatermarkAcrossAlignedChannels(DataOutput<?> output)
            throws Exception {
        long newMinWatermark = Long.MAX_VALUE;
        boolean hasAlignedChannels = false;

        // determine new overall watermark by considering only watermark-aligned channels across all
        // channels 获取所有isWatermarkAligned为true的channel的最小值
        for (InputChannelStatus channelStatus : channelStatuses) {
            if (channelStatus.isWatermarkAligned) {
                hasAlignedChannels = true;
                newMinWatermark = Math.min(channelStatus.watermark, newMinWatermark);
            }
        }

        // we acknowledge and output the new overall watermark if it really is aggregated
        // from some remaining aligned channel, and is also larger than the last output watermark
        if (hasAlignedChannels && newMinWatermark > lastOutputWatermark) {
            lastOutputWatermark = newMinWatermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }
//找出所有channel中的最大watermark，大于lastOutputWatermark，则发送
    private void findAndOutputMaxWatermarkAcrossAllChannels(DataOutput<?> output) throws Exception {
        long maxWatermark = Long.MIN_VALUE;

        for (InputChannelStatus channelStatus : channelStatuses) {
            maxWatermark = Math.max(channelStatus.watermark, maxWatermark);
        }

        if (maxWatermark > lastOutputWatermark) {
            lastOutputWatermark = maxWatermark;
            output.emitWatermark(new Watermark(lastOutputWatermark));
        }
    }

    /**
     * An {@code InputChannelStatus} keeps track of an input channel's last watermark, stream
     * status, and whether or not the channel's current watermark is aligned with the overall
     * watermark output from the valve.
     *
     * <p>There are 2 situations where a channel's watermark is not considered aligned:
     *
     * <ul>
     *   <li>the current stream status of the channel is idle
     *   <li>the stream status has resumed to be active, but the watermark of the channel hasn't
     *       caught up to the last output watermark from the valve yet.
     * </ul>
     *///记录inputchannel的状态，如连接此channel的输入流的状态，watermark等
    @VisibleForTesting
    protected static class InputChannelStatus {
        protected long watermark;//此channel的watermark
        protected StreamStatus streamStatus;//此输入流的状态，是空闲状态idle还是有数据状态active
        protected boolean isWatermarkAligned;//当新的watermark赋值时，isWatermarkAligned为true
//即当isWatermarkAligned为true时，watermark肯定大于lastOutputWatermark
        /**
         * Utility to check if at least one channel in a given array of input channels is active.
         *///判断channelStatuses是否有channel是active的
        private static boolean hasActiveChannels(InputChannelStatus[] channelStatuses) {
            for (InputChannelStatus status : channelStatuses) {
                if (status.streamStatus.isActive()) {
                    return true;
                }
            }
            return false;
        }
    }

    @VisibleForTesting
    protected InputChannelStatus getInputChannelStatus(int channelIndex) {
        Preconditions.checkArgument(
                channelIndex >= 0 && channelIndex < channelStatuses.length,
                "Invalid channel index. Number of input channels: " + channelStatuses.length);

        return channelStatuses[channelIndex];
    }
}
