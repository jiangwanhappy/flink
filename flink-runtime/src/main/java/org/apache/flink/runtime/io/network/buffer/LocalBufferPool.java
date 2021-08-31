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

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferListener.NotificationResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayDeque;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the {@link
 * NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure dead-lock free operation of
 * the network stack by limiting the number of buffers per local buffer pool. It also implements the
 * default mechanism for buffer recycling, which ensures that every buffer is ultimately returned to
 * the network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to match
 * its new size.
 *
 * <p>Availability is defined as returning a segment on a subsequent {@link #requestBuffer()}/
 * {@link #requestBufferBuilder()} and heaving a non-blocking {@link
 * #requestBufferBuilderBlocking(int)}. In particular,
 *
 * <ul>
 *   <li>There is at least one {@link #availableMemorySegments}.
 *   <li>No subpartitions has reached {@link #maxBuffersPerChannel}.
 * </ul>
 *
 * <p>To ensure this contract, the implementation eagerly fetches additional memory segments from
 * {@link NetworkBufferPool} as long as it hasn't reached {@link #maxNumberOfMemorySegments} or one
 * subpartition reached the quota.
 *///LocalBufferPool是NetworkBufferPool的包装。负责分配和回收NetworkBufferPool中的一部分buffer对象。NetworkBufferPool是一个固定大小的缓存池。将一个NetworkBufferPool的可用缓存划分给多个LocalBufferPool使用，避免网络层同时操作NetworkBufferPool造成死锁。同时LocalBufferPool实现了默认的回收机制，确保每一个buffer最终会返回给NetworkBufferPool
class LocalBufferPool implements BufferPool {
    private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

    private static final int UNKNOWN_CHANNEL = -1;

    /** Global network buffer pool to get buffers from. *///LocalBufferPool是从该 NetworkBufferPool 获取的buffer
    private final NetworkBufferPool networkBufferPool;

    /** The minimum number of required segments for this pool. */
    private final int numberOfRequiredMemorySegments;//最小内存片段数量

    /**
     * The currently available memory segments. These are segments, which have been requested from
     * the network buffer pool and are currently not handed out as Buffer instances.
     *
     * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
     * locks acquired before entering this class vs. locks being acquired during calls to external
     * code inside this class, e.g. with {@link
     * org.apache.flink.runtime.io.network.partition.consumer.BufferManager#bufferQueue} via the
     * {@link #registeredListeners} callback.
     */
    private final ArrayDeque<MemorySegment> availableMemorySegments =
            new ArrayDeque<MemorySegment>();//从networkBufferPool的availableMemorySegments获取的MemorySegment的集合

    /**
     * Buffer availability listeners, which need to be notified when a Buffer becomes available.
     * Listeners can only be registered at a time/state where no Buffer instance was available.
     */
    private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

    /** Maximum number of network buffers to allocate. */
    private final int maxNumberOfMemorySegments;//内存片段数量的上限值

    /** The current size of this pool. */
    @GuardedBy("availableMemorySegments")
    private int currentPoolSize;//当前pool的容量，可以被setNumBuffers方法修改
//numberOfRequestedMemorySegments > currentPoolSize,则视为超出内存，超出内存的部分需要归还给networkBufferPool
    /**
     * Number of all memory segments, which have been requested from the network buffer pool and are
     * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available
     * segments).
     */
    @GuardedBy("availableMemorySegments")
    private int numberOfRequestedMemorySegments;//已请求的内存数量,是需要 <= currentPoolSize

    private final int maxBuffersPerChannel;

    @GuardedBy("availableMemorySegments")
    private final int[] subpartitionBuffersCount;

    private final BufferRecycler[] subpartitionBufferRecyclers;

    @GuardedBy("availableMemorySegments")
    private int unavailableSubpartitionsCount = 0;

    @GuardedBy("availableMemorySegments")
    private boolean isDestroyed;
    //即若availableMemorySegments的size大于0 且 unavailableSubpartitionsCount == 0，则availabilityHelper应该是available状态
    @GuardedBy("availableMemorySegments")
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();

    @GuardedBy("availableMemorySegments")
    private boolean requestingWhenAvailable;

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
     * network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     */
    LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                Integer.MAX_VALUE,
                0,
                Integer.MAX_VALUE);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
     * number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments) {
        this(
                networkBufferPool,
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments,
                0,
                Integer.MAX_VALUE);
    }

    /**
     * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
     * with a minimal and maximal number of network buffers being available.
     *
     * @param networkBufferPool global network buffer pool to get buffers from
     * @param numberOfRequiredMemorySegments minimum number of network buffers
     * @param maxNumberOfMemorySegments maximum number of network buffers to allocate
     * @param numberOfSubpartitions number of subpartitions
     * @param maxBuffersPerChannel maximum number of buffers to use for each channel
     */
    LocalBufferPool(
            NetworkBufferPool networkBufferPool,
            int numberOfRequiredMemorySegments,
            int maxNumberOfMemorySegments,
            int numberOfSubpartitions,
            int maxBuffersPerChannel) {
        checkArgument(
                numberOfRequiredMemorySegments > 0,
                "Required number of memory segments (%s) should be larger than 0.",
                numberOfRequiredMemorySegments);

        checkArgument(
                maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
                "Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
                maxNumberOfMemorySegments,
                numberOfRequiredMemorySegments);

        LOG.debug(
                "Using a local buffer pool with {}-{} buffers",
                numberOfRequiredMemorySegments,
                maxNumberOfMemorySegments);

        this.networkBufferPool = networkBufferPool;
        this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
        this.currentPoolSize = numberOfRequiredMemorySegments;
        this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;

        if (numberOfSubpartitions > 0) {
            checkArgument(
                    maxBuffersPerChannel > 0,
                    "Maximum number of buffers for each channel (%s) should be larger than 0.",
                    maxBuffersPerChannel);
        }

        this.subpartitionBuffersCount = new int[numberOfSubpartitions];
        subpartitionBufferRecyclers = new BufferRecycler[numberOfSubpartitions];
        for (int i = 0; i < subpartitionBufferRecyclers.length; i++) {
            subpartitionBufferRecyclers[i] = new SubpartitionBufferRecycler(i, this);
        }
        this.maxBuffersPerChannel = maxBuffersPerChannel;

        // Lock is only taken, because #checkAvailability asserts it. It's a small penalty for
        // thread safety.
        synchronized (this.availableMemorySegments) {
            if (checkAvailability()) {
                availabilityHelper.resetAvailable();
            }

            checkConsistentAvailability();//看应否应该是available状态且availabilityHelper为available状态
        }
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    @Override
    public boolean isDestroyed() {
        synchronized (availableMemorySegments) {
            return isDestroyed;
        }
    }

    @Override
    public int getNumberOfRequiredMemorySegments() {
        return numberOfRequiredMemorySegments;
    }

    @Override
    public int getMaxNumberOfMemorySegments() {
        return maxNumberOfMemorySegments;
    }

    @Override
    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    @Override
    public int getNumBuffers() {
        synchronized (availableMemorySegments) {
            return currentPoolSize;
        }
    }

    @Override
    public int bestEffortGetNumOfUsedBuffers() {
        return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
    }
//requestBuffer 从availableMemorySegments获取一个MemorySegment并包装成NetworkBuffer返回
    @Override
    public Buffer requestBuffer() {
        return toBuffer(requestMemorySegment());
    }

    @Override
    public BufferBuilder requestBufferBuilder() {
        return toBufferBuilder(requestMemorySegment(UNKNOWN_CHANNEL), UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilder(int targetChannel) {
        return toBufferBuilder(requestMemorySegment(targetChannel), targetChannel);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking() throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(), UNKNOWN_CHANNEL);
    }

    @Override
    public MemorySegment requestMemorySegmentBlocking() throws InterruptedException {
        return requestMemorySegmentBlocking(UNKNOWN_CHANNEL);
    }

    @Override
    public BufferBuilder requestBufferBuilderBlocking(int targetChannel)
            throws InterruptedException {
        return toBufferBuilder(requestMemorySegmentBlocking(targetChannel), targetChannel);
    }

    private Buffer toBuffer(MemorySegment memorySegment) {
        if (memorySegment == null) {
            return null;
        }
        return new NetworkBuffer(memorySegment, this);
    }

    private BufferBuilder toBufferBuilder(MemorySegment memorySegment, int targetChannel) {
        if (memorySegment == null) {
            return null;
        }

        if (targetChannel == UNKNOWN_CHANNEL) {
            return new BufferBuilder(memorySegment, this);
        } else {
            return new BufferBuilder(memorySegment, subpartitionBufferRecyclers[targetChannel]);
        }
    }

    private MemorySegment requestMemorySegmentBlocking(int targetChannel)
            throws InterruptedException {
        MemorySegment segment;
        while ((segment = requestMemorySegment(targetChannel)) == null) {
            try {
                // wait until available
                getAvailableFuture().get();//阻塞主线程直到获取到结果
            } catch (ExecutionException e) {
                LOG.error("The available future is completed exceptionally.", e);
                ExceptionUtils.rethrow(e);
            }
        }
        return segment;
    }
//从availableMemorySegments中获取一个MemorySegment，若numberOfRequestedMemorySegments<currentPoolSize,则再从networkBufferPool拿一个到availableMemorySegments
    @Nullable
    private MemorySegment requestMemorySegment(int targetChannel) {
        MemorySegment segment;
        synchronized (availableMemorySegments) {
            if (isDestroyed) {
                throw new IllegalStateException("Buffer pool is destroyed.");
            }

            // target channel over quota; do not return a segment
            if (targetChannel != UNKNOWN_CHANNEL
                    && subpartitionBuffersCount[targetChannel] >= maxBuffersPerChannel) {
                return null;
            }

            segment = availableMemorySegments.poll();

            if (segment == null) {
                return null;
            }

            if (targetChannel != UNKNOWN_CHANNEL) {
                if (++subpartitionBuffersCount[targetChannel] == maxBuffersPerChannel) {
                    unavailableSubpartitionsCount++;
                }
            }

            if (!checkAvailability()) {
                availabilityHelper.resetUnavailable();
            }

            checkConsistentAvailability();
        }
        return segment;
    }

    @Override
    public MemorySegment requestMemorySegment() {
        return requestMemorySegment(UNKNOWN_CHANNEL);
    }
    //从networkBufferPool的availableMemorySegments获取MemorySegment添加到availableMemorySegments
    private boolean requestMemorySegmentFromGlobal() {
        assert Thread.holdsLock(availableMemorySegments);

        if (isRequestedSizeReached()) {
            return false;
        }

        checkState(
                !isDestroyed,
                "Destroyed buffer pools should never acquire segments - this will lead to buffer leaks.");

        MemorySegment segment = networkBufferPool.requestMemorySegment();//从availableMemorySegments获取一个MemorySegment
        if (segment != null) {
            availableMemorySegments.add(segment);
            numberOfRequestedMemorySegments++;
            return true;
        }
        return false;
    }

    /**
     * Tries to obtain a buffer from global pool as soon as one pool is available. Note that
     * multiple {@link LocalBufferPool}s might wait on the future of the global pool, hence this
     * method double-check if a new buffer is really needed at the time it becomes available.
     */
    private void requestMemorySegmentFromGlobalWhenAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        if (requestingWhenAvailable) {
            return;
        }
        requestingWhenAvailable = true;

        assertNoException(
                networkBufferPool.getAvailableFuture().thenRun(this::onGlobalPoolAvailable));
    }

    private void onGlobalPoolAvailable() {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            requestingWhenAvailable = false;
            if (isDestroyed || availabilityHelper.isApproximatelyAvailable()) {
                // there is currently no benefit to obtain buffer from global; give other pools
                // precedent
                return;
            }

            // Check availability and potentially request the memory segment. The call may also
            // result in invoking
            // #requestMemorySegmentFromGlobalWhenAvailable again if no segment could be fetched
            // because of
            // concurrent requests from different LocalBufferPools.
            if (checkAvailability()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            }
        }
        mayNotifyAvailable(toNotify);
    }

    private boolean shouldBeAvailable() {
        assert Thread.holdsLock(availableMemorySegments);

        return !availableMemorySegments.isEmpty() && unavailableSubpartitionsCount == 0;
    }
//如果availableMemorySegments不为空，则返回unavailableSubpartitionsCount == 0；否则从networkBufferPool拿取一个
    private boolean checkAvailability() {
        assert Thread.holdsLock(availableMemorySegments);

        if (!availableMemorySegments.isEmpty()) {
            return unavailableSubpartitionsCount == 0;
        }
        if (!isRequestedSizeReached()) {
            if (requestMemorySegmentFromGlobal()) {//从networkBufferPool的availableMemorySegments获取MemorySegment添加到availableMemorySegments
                return unavailableSubpartitionsCount == 0;
            } else {
                requestMemorySegmentFromGlobalWhenAvailable();
                return shouldBeAvailable();
            }
        }
        return false;
    }
//检查是否应该是available状态（availableMemorySegments的size大于0 且 unavailableSubpartitionsCount == 0）  且  确实是available状态（availabilityHelper为available状态）
    private void checkConsistentAvailability() {
        assert Thread.holdsLock(availableMemorySegments);
//即若availableMemorySegments的size大于0 且 unavailableSubpartitionsCount == 0，则返回true，代表availabilityHelper应为available状态
        final boolean shouldBeAvailable = shouldBeAvailable();
        checkState(
                availabilityHelper.isApproximatelyAvailable() == shouldBeAvailable,
                "Inconsistent availability: expected " + shouldBeAvailable);
    }

    @Override
    public void recycle(MemorySegment segment) {
        recycle(segment, UNKNOWN_CHANNEL);
    }

    private void recycle(MemorySegment segment, int channel) {
        BufferListener listener;
        CompletableFuture<?> toNotify = null;
        NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;// 创建一个默认的NotificationResult，为缓存未使用
        while (!notificationResult.isBufferUsed()) {// 一直循环，确保如果listener返回内存不再使用的时候，再次执行这段逻辑将其回收
            synchronized (availableMemorySegments) {
                if (channel != UNKNOWN_CHANNEL) {
                    if (subpartitionBuffersCount[channel]-- == maxBuffersPerChannel) {
                        unavailableSubpartitionsCount--;
                    }
                }
                // 如果已请求的内存片段数量多于当前pool的大小，需要将内存归还// currentPoolSize可能会在运行的过程中调整(NetworkBufferPool的redistributeBuffers方法)
                if (isDestroyed || hasExcessBuffers()) {
                    returnMemorySegment(segment);  // 返还内存给networkBufferPool处理, numberOfRequestedMemorySegments 减1,调用networkBufferPool的recycle方法
                    return;
                } else {
                    listener = registeredListeners.poll();// 取出一个注册的listener
                    if (listener == null) {
                        availableMemorySegments.add(segment);
                        // only need to check unavailableSubpartitionsCount here because
                        // availableMemorySegments is not empty
                        if (!availabilityHelper.isApproximatelyAvailable()
                                && unavailableSubpartitionsCount == 0) {
                            toNotify = availabilityHelper.getUnavailableToResetAvailable();
                        }
                        break;
                    }
                }

                checkConsistentAvailability();
            }
            notificationResult = fireBufferAvailableNotification(listener, segment);
        }

        mayNotifyAvailable(toNotify);
    }

    private NotificationResult fireBufferAvailableNotification(
            BufferListener listener, MemorySegment segment) {
        // We do not know which locks have been acquired before the recycle() or are needed in the
        // notification and which other threads also access them.
        // -> call notifyBufferAvailable() outside of the synchronized block to avoid a deadlock
        // (FLINK-9676)
        NotificationResult notificationResult =
                listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
        if (notificationResult.needsMoreBuffers()) {
            synchronized (availableMemorySegments) {
                if (isDestroyed) {
                    // cleanup tasks how they would have been done if we only had one synchronized
                    // block
                    listener.notifyBufferDestroyed();
                } else {
                    registeredListeners.add(listener);
                }
            }
        }
        return notificationResult;
    }
//将availableMemorySegments的资源都返还给networkBufferPool的availableMemorySegments；并设置isDestroyed = true
    /** Destroy is called after the produce or consume phase of a task finishes. */
    @Override
    public void lazyDestroy() {
        // NOTE: if you change this logic, be sure to update recycle() as well!
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (!isDestroyed) {
                MemorySegment segment;
                while ((segment = availableMemorySegments.poll()) != null) {//将availableMemorySegments的所有资源都返还给networkBufferPool的availableMemorySegments
                    returnMemorySegment(segment);
                }

                BufferListener listener;
                while ((listener = registeredListeners.poll()) != null) {
                    listener.notifyBufferDestroyed();
                }

                if (!isAvailable()) {//看是否是不可用的，如果是不可用的，则要获取这个不可用，以便mayNotifyAvailable方法可以执行不可用时该做的事
                    toNotify = availabilityHelper.getAvailableFuture();
                }

                isDestroyed = true;
            }
        }

        mayNotifyAvailable(toNotify);//因为要destory了，这个资源池不可用了，则要把不 可用要做的事 做了

        networkBufferPool.destroyBufferPool(this);
    }

    @Override
    public boolean addBufferListener(BufferListener listener) {
        synchronized (availableMemorySegments) {
            if (!availableMemorySegments.isEmpty() || isDestroyed) {
                return false;
            }

            registeredListeners.add(listener);
            return true;
        }
    }
//重新分配currentPoolSize（重新分配的值要 >= numberOfRequiredMemorySegments），若numberOfRequestedMemorySegments > currentPoolSize，则视为超出，则要把超出的内存返还给networkBufferPool
    @Override
    public void setNumBuffers(int numBuffers) {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            checkArgument( // 检查重新设定的pool size必须要大于或等于numberOfRequiredMemorySegments
                    numBuffers >= numberOfRequiredMemorySegments,
                    "Buffer pool needs at least %s buffers, but tried to set to %s",
                    numberOfRequiredMemorySegments,
                    numBuffers);

            currentPoolSize = Math.min(numBuffers, maxNumberOfMemorySegments);
            // 归还超出部分的内存给NetworkBufferPool
            returnExcessMemorySegments();

            if (isDestroyed) {
                // FLINK-19964: when two local buffer pools are released concurrently, one of them
                // gets buffers assigned
                // make sure that checkAvailability is not called as it would pro-actively acquire
                // one buffer from NetworkBufferPool
                return;
            }

            if (checkAvailability()) {
                toNotify = availabilityHelper.getUnavailableToResetAvailable();
            } else {
                availabilityHelper.resetUnavailable();
            }

            checkConsistentAvailability();
        }

        mayNotifyAvailable(toNotify);
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    @Override
    public String toString() {
        synchronized (availableMemorySegments) {
            return String.format(
                    "[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d,"
                            + "subpartitions: %d, maxBuffersPerChannel: %d, destroyed: %s]",
                    currentPoolSize,
                    numberOfRequiredMemorySegments,
                    numberOfRequestedMemorySegments,
                    availableMemorySegments.size(),
                    maxNumberOfMemorySegments,
                    registeredListeners.size(),
                    subpartitionBuffersCount.length,
                    maxBuffersPerChannel,
                    isDestroyed);
        }
    }

    // ------------------------------------------------------------------------

    /**
     * Notifies the potential segment consumer of the new available segments by completing the
     * previous uncompleted future.
     */
    private void mayNotifyAvailable(@Nullable CompletableFuture<?> toNotify) {
        if (toNotify != null) {
            toNotify.complete(null);
        }
    }
//将此MemorySegment返还给networkBufferPool的availableMemorySegments
    private void returnMemorySegment(MemorySegment segment) {
        assert Thread.holdsLock(availableMemorySegments);

        numberOfRequestedMemorySegments--;//已请求的内存数量-1
        networkBufferPool.recycle(segment);// 调用NetworkBufferPool的recycle方法，回收这一段内存
    }

    private void returnExcessMemorySegments() {
        assert Thread.holdsLock(availableMemorySegments);

        while (hasExcessBuffers()) {
            MemorySegment segment = availableMemorySegments.poll();
            if (segment == null) {
                return;
            }
//归还这个内存
            returnMemorySegment(segment);
        }
    }

    private boolean hasExcessBuffers() {
        return numberOfRequestedMemorySegments > currentPoolSize;
    }

    private boolean isRequestedSizeReached() {
        return numberOfRequestedMemorySegments >= currentPoolSize;
    }

    private static class SubpartitionBufferRecycler implements BufferRecycler {

        private int channel;
        private LocalBufferPool bufferPool;

        SubpartitionBufferRecycler(int channel, LocalBufferPool bufferPool) {
            this.channel = channel;
            this.bufferPool = bufferPool;
        }

        @Override
        public void recycle(MemorySegment memorySegment) {
            bufferPool.recycle(memorySegment, channel);
        }
    }
}
