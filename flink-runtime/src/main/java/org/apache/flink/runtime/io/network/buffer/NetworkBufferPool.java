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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The NetworkBufferPool is a fixed size pool of {@link MemorySegment} instances for the network
 * stack.
 *
 * <p>The NetworkBufferPool creates {@link LocalBufferPool}s from which the individual tasks draw
 * the buffers for the network data transfer. When new local buffer pools are created, the
 * NetworkBufferPool dynamically redistributes the buffers between the pools.
 *///NetworkBufferPool是供网络层使用的，固定大小的缓存池。每个task并非直接从NetworkBufferPool获取内存，而是使用从NetworkBufferPool创建出的LocalBufferPool来分配内存。
public class NetworkBufferPool
        implements BufferPoolFactory, MemorySegmentProvider, AvailabilityProvider {

    private static final Logger LOG = LoggerFactory.getLogger(NetworkBufferPool.class);

    private final int totalNumberOfMemorySegments;//总MemorySegment数量

    private final int memorySegmentSize;//每个MemorySegment的大小

    private final ArrayDeque<MemorySegment> availableMemorySegments;//一个队列，用来存放可用的MemorySegment(可用的=总的 - 已被申请的)

    private volatile boolean isDestroyed;

    // ---- Managed buffer pools ----------------------------------------------

    private final Object factoryLock = new Object();
//存放 从NetworkBufferPool申请资源的LocalBufferPool 的集合
    private final Set<LocalBufferPool> allBufferPools = new HashSet<>();//用来存放基于此NetworkBufferPool创建的LocalBufferPool

    private int numTotalRequiredBuffers;//已经分配给其它bufferpool如LocalBufferPool的buffer数量

    private final Duration requestSegmentsTimeout;//请求内存的最大等待时间（超时时间）
//当availableMemorySegments的size > 0 时，认为availabilityHelper应为available的状态
    private final AvailabilityHelper availabilityHelper = new AvailabilityHelper();
//分配指定数量的MemorySegment
    @VisibleForTesting
    public NetworkBufferPool(int numberOfSegmentsToAllocate, int segmentSize) {
        this(numberOfSegmentsToAllocate, segmentSize, Duration.ofMillis(Integer.MAX_VALUE));
    }

    /** Allocates all {@link MemorySegment} instances managed by this pool. */
    public NetworkBufferPool(
            int numberOfSegmentsToAllocate, int segmentSize, Duration requestSegmentsTimeout) {
        this.totalNumberOfMemorySegments = numberOfSegmentsToAllocate;
        this.memorySegmentSize = segmentSize;

        Preconditions.checkNotNull(requestSegmentsTimeout);
        checkArgument(
                requestSegmentsTimeout.toMillis() > 0,
                "The timeout for requesting exclusive buffers should be positive.");
        this.requestSegmentsTimeout = requestSegmentsTimeout;

        final long sizeInLong = (long) segmentSize;

        try {
            this.availableMemorySegments = new ArrayDeque<>(numberOfSegmentsToAllocate);
        } catch (OutOfMemoryError err) {
            throw new OutOfMemoryError(
                    "Could not allocate buffer queue of length "
                            + numberOfSegmentsToAllocate
                            + " - "
                            + err.getMessage());
        }

        try {
            for (int i = 0; i < numberOfSegmentsToAllocate; i++) {
                availableMemorySegments.add(
                        MemorySegmentFactory.allocateUnpooledOffHeapMemory(segmentSize, null));
            }
        } catch (OutOfMemoryError err) {
            int allocated = availableMemorySegments.size();

            // free some memory
            availableMemorySegments.clear();

            long requiredMb = (sizeInLong * numberOfSegmentsToAllocate) >> 20;
            long allocatedMb = (sizeInLong * allocated) >> 20;
            long missingMb = requiredMb - allocatedMb;

            throw new OutOfMemoryError(
                    "Could not allocate enough memory segments for NetworkBufferPool "
                            + "(required (Mb): "
                            + requiredMb
                            + ", allocated (Mb): "
                            + allocatedMb
                            + ", missing (Mb): "
                            + missingMb
                            + "). Cause: "
                            + err.getMessage());
        }

        availabilityHelper.resetAvailable();//availableFuture = AVAILABLE,即返回一个有结果值null已完成的CompletableFuture

        long allocatedMb = (sizeInLong * availableMemorySegments.size()) >> 20;//右移20位，即除以2^20,1MB=2^20B

        LOG.info(
                "Allocated {} MB for network buffer pool (number of memory segments: {}, bytes per segment: {}).",
                allocatedMb,
                availableMemorySegments.size(),
                segmentSize);
    }

    @Nullable
    public MemorySegment requestMemorySegment() {
        synchronized (availableMemorySegments) {
            return internalRequestMemorySegment();//从availableMemorySegments获取一个MemorySegment
        }
    }

    public void recycle(MemorySegment segment) {
        // Adds the segment back to the queue, which does not immediately free the memory
        // however, since this happens when references to the global pool are also released,
        // making the availableMemorySegments queue and its contained object reclaimable
        internalRecycleMemorySegments(Collections.singleton(checkNotNull(segment)));
    }
//从NetworkBufferPool申请numberOfSegmentsToRequest个数量的MemorySegment，则需要重新分配给allBufferPools集合里的LocalBufferPool
    @Override
    public List<MemorySegment> requestMemorySegments(int numberOfSegmentsToRequest)
            throws IOException {
        checkArgument(
                numberOfSegmentsToRequest >= 0,
                "Number of buffers to request must be non-negative.");

        synchronized (factoryLock) {
            if (isDestroyed) {
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }

            if (numberOfSegmentsToRequest == 0) {
                return Collections.emptyList();
            }

            tryRedistributeBuffers(numberOfSegmentsToRequest);//需要重新分配给allBufferPools集合里的LocalBufferPool
        }

        final List<MemorySegment> segments = new ArrayList<>(numberOfSegmentsToRequest);
        try {
            final Deadline deadline = Deadline.fromNow(requestSegmentsTimeout);
            while (true) {
                if (isDestroyed) {
                    throw new IllegalStateException("Buffer pool is destroyed.");
                }

                MemorySegment segment;
                synchronized (availableMemorySegments) {
                    if ((segment = internalRequestMemorySegment()) == null) {
                        availableMemorySegments.wait(2000);
                    }
                }
                if (segment != null) {
                    segments.add(segment);
                }

                if (segments.size() >= numberOfSegmentsToRequest) {
                    break;
                }

                if (!deadline.hasTimeLeft()) {
                    throw new IOException(
                            String.format(
                                    "Timeout triggered when requesting exclusive buffers: %s, "
                                            + " or you may increase the timeout which is %dms by setting the key '%s'.",
                                    getConfigDescription(),
                                    requestSegmentsTimeout.toMillis(),
                                    NettyShuffleEnvironmentOptions
                                            .NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS
                                            .key()));
                }
            }
        } catch (Throwable e) {
            recycleMemorySegments(segments, numberOfSegmentsToRequest);
            ExceptionUtils.rethrowIOException(e);
        }

        return segments;
    }
//从availableMemorySegments获取一个MemorySegment
    @Nullable
    private MemorySegment internalRequestMemorySegment() {
        assert Thread.holdsLock(availableMemorySegments);

        final MemorySegment segment = availableMemorySegments.poll();
        if (availableMemorySegments.isEmpty() && segment != null) {
            availabilityHelper.resetUnavailable();
        }
        return segment;
    }

    @Override
    public void recycleMemorySegments(Collection<MemorySegment> segments) {
        recycleMemorySegments(segments, segments.size());
    }

    private void recycleMemorySegments(Collection<MemorySegment> segments, int size) {
        internalRecycleMemorySegments(segments);

        synchronized (factoryLock) {
            numTotalRequiredBuffers -= size;

            // note: if this fails, we're fine for the buffer pool since we already recycled the
            // segments
            redistributeBuffers();
        }
    }

    private void internalRecycleMemorySegments(Collection<MemorySegment> segments) {
        CompletableFuture<?> toNotify = null;
        synchronized (availableMemorySegments) {
            if (availableMemorySegments.isEmpty() && !segments.isEmpty()) {//即若availableMemorySegments将变为有值的情况，则应将availableFuture 置为 AVAILABLE;
                toNotify = availabilityHelper.getUnavailableToResetAvailable();//返回availableFuture在未置为AVAILABLE的状态
            }
            availableMemorySegments.addAll(segments);
            availableMemorySegments.notifyAll();//notifyAll() 方法用于唤醒在该对象上等待的所有线程
        }

        if (toNotify != null) {
            toNotify.complete(null);//返回一个结果值null，让toNotify接收到结果并做应做的事
        }
    }

    public void destroy() {
        synchronized (factoryLock) {
            isDestroyed = true;
        }

        synchronized (availableMemorySegments) {
            MemorySegment segment;
            while ((segment = availableMemorySegments.poll()) != null) {
                segment.free();
            }
        }
    }

    public boolean isDestroyed() {
        return isDestroyed;
    }

    public int getTotalNumberOfMemorySegments() {
        return isDestroyed() ? 0 : totalNumberOfMemorySegments;
    }

    public long getTotalMemory() {
        return (long) getTotalNumberOfMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfAvailableMemorySegments() {
        synchronized (availableMemorySegments) {
            return availableMemorySegments.size();
        }
    }

    public long getAvailableMemory() {
        return (long) getNumberOfAvailableMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfUsedMemorySegments() {
        return getTotalNumberOfMemorySegments() - getNumberOfAvailableMemorySegments();
    }

    public long getUsedMemory() {
        return (long) getNumberOfUsedMemorySegments() * memorySegmentSize;
    }

    public int getNumberOfRegisteredBufferPools() {
        synchronized (factoryLock) {
            return allBufferPools.size();
        }
    }

    public int countBuffers() {
        int buffers = 0;

        synchronized (factoryLock) {
            for (BufferPool bp : allBufferPools) {
                buffers += bp.getNumBuffers();
            }
        }

        return buffers;
    }

    /** Returns a future that is completed when there are free segments in this pool. */
    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return availabilityHelper.getAvailableFuture();
    }

    // ------------------------------------------------------------------------
    // BufferPoolFactory
    // ------------------------------------------------------------------------
    //创建LocalBufferPool实例并返回
    @Override
    public BufferPool createBufferPool(int numRequiredBuffers, int maxUsedBuffers)
            throws IOException {
        return internalCreateBufferPool(numRequiredBuffers, maxUsedBuffers, 0, Integer.MAX_VALUE);
    }

    @Override
    public BufferPool createBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel)
            throws IOException {
        return internalCreateBufferPool(
                numRequiredBuffers, maxUsedBuffers, numSubpartitions, maxBuffersPerChannel);
    }
    //创建LocalBufferPool实例并返回
    private BufferPool internalCreateBufferPool(
            int numRequiredBuffers,
            int maxUsedBuffers,
            int numSubpartitions,
            int maxBuffersPerChannel)
            throws IOException {

        // It is necessary to use a separate lock from the one used for buffer
        // requests to ensure deadlock freedom for failure cases.
        synchronized (factoryLock) {
            if (isDestroyed) {
                throw new IllegalStateException("Network buffer pool has already been destroyed.");
            }

            // Ensure that the number of required buffers can be satisfied.
            // With dynamic memory management this should become obsolete.
            if (numTotalRequiredBuffers + numRequiredBuffers > totalNumberOfMemorySegments) {
                throw new IOException(
                        String.format(
                                "Insufficient number of network buffers: "
                                        + "required %d, but only %d available. %s.",
                                numRequiredBuffers,
                                totalNumberOfMemorySegments - numTotalRequiredBuffers,
                                getConfigDescription()));
            }

            this.numTotalRequiredBuffers += numRequiredBuffers;

            // We are good to go, create a new buffer pool and redistribute
            // non-fixed size buffers.
            LocalBufferPool localBufferPool =
                    new LocalBufferPool(
                            this,
                            numRequiredBuffers,
                            maxUsedBuffers,
                            numSubpartitions,
                            maxBuffersPerChannel);//创建LocalBufferPool实例

            allBufferPools.add(localBufferPool);

            redistributeBuffers();

            return localBufferPool;
        }
    }

    @Override
    public void destroyBufferPool(BufferPool bufferPool) {
        if (!(bufferPool instanceof LocalBufferPool)) {
            throw new IllegalArgumentException("bufferPool is no LocalBufferPool");
        }

        synchronized (factoryLock) {
            if (allBufferPools.remove(bufferPool)) {
                numTotalRequiredBuffers -= bufferPool.getNumberOfRequiredMemorySegments();

                redistributeBuffers();
            }
        }
    }

    /**
     * Destroys all buffer pools that allocate their buffers from this buffer pool (created via
     * {@link #createBufferPool(int, int)}).
     */
    public void destroyAllBufferPools() {
        synchronized (factoryLock) {
            // create a copy to avoid concurrent modification exceptions
            LocalBufferPool[] poolsCopy =
                    allBufferPools.toArray(new LocalBufferPool[allBufferPools.size()]);

            for (LocalBufferPool pool : poolsCopy) {
                pool.lazyDestroy();
            }

            // some sanity checks
            if (allBufferPools.size() > 0 || numTotalRequiredBuffers > 0) {
                throw new IllegalStateException(
                        "NetworkBufferPool is not empty after destroying all LocalBufferPools");
            }
        }
    }

    // Must be called from synchronized block
    private void tryRedistributeBuffers(int numberOfSegmentsToRequest) throws IOException {
        assert Thread.holdsLock(factoryLock);

        if (numTotalRequiredBuffers + numberOfSegmentsToRequest > totalNumberOfMemorySegments) {
            throw new IOException(
                    String.format(
                            "Insufficient number of network buffers: "
                                    + "required %d, but only %d available. %s.",
                            numberOfSegmentsToRequest,
                            totalNumberOfMemorySegments - numTotalRequiredBuffers,
                            getConfigDescription()));
        }

        this.numTotalRequiredBuffers += numberOfSegmentsToRequest;

        try {
            redistributeBuffers();//重新分配给allBufferPools集合里的LocalBufferPool
        } catch (Throwable t) {
            this.numTotalRequiredBuffers -= numberOfSegmentsToRequest;

            redistributeBuffers();
            ExceptionUtils.rethrow(t);
        }
    }
//算出NetworkBufferPool还有多少可用资源，重新分配给allBufferPools集合里的LocalBufferPool
    // Must be called from synchronized block，如本来allBufferPools里可以分配33个，但是requestMemorySegments方法从NetworkBufferPool申请了2个，则allBufferPools里就只能分配31个
    private void redistributeBuffers() {
        assert Thread.holdsLock(factoryLock);

        // All buffers, which are not among the required ones
        final int numAvailableMemorySegment = totalNumberOfMemorySegments - numTotalRequiredBuffers;

        if (numAvailableMemorySegment == 0) {
            // in this case, we need to redistribute buffers so that every pool gets its minimum
            for (LocalBufferPool bufferPool : allBufferPools) {
                bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments());
            }
            return;
        }

        /*
         * With buffer pools being potentially limited, let's distribute the available memory
         * segments based on the capacity of each buffer pool, i.e. the maximum number of segments
         * an unlimited buffer pool can take is numAvailableMemorySegment, for limited buffer pools
         * it may be less. Based on this and the sum of all these values (totalCapacity), we build
         * a ratio that we use to distribute the buffers.
         */

        long totalCapacity = 0; // long to avoid int overflow

        for (LocalBufferPool bufferPool : allBufferPools) {
            int excessMax =
                    bufferPool.getMaxNumberOfMemorySegments()
                            - bufferPool.getNumberOfRequiredMemorySegments();
            totalCapacity += Math.min(numAvailableMemorySegment, excessMax);
        }

        // no capacity to receive additional buffers?
        if (totalCapacity == 0) {
            return; // necessary to avoid div by zero when nothing to re-distribute
        }

        // since one of the arguments of 'min(a,b)' is a positive int, this is actually
        // guaranteed to be within the 'int' domain
        // (we use a checked downCast to handle possible bugs more gracefully).
        final int memorySegmentsToDistribute =
                MathUtils.checkedDownCast(Math.min(numAvailableMemorySegment, totalCapacity));//强转成int类型

        long totalPartsUsed = 0; // of totalCapacity
        int numDistributedMemorySegment = 0;
        for (LocalBufferPool bufferPool : allBufferPools) {
            int excessMax =
                    bufferPool.getMaxNumberOfMemorySegments()
                            - bufferPool.getNumberOfRequiredMemorySegments();

            // shortcut
            if (excessMax == 0) {
                continue;
            }

            totalPartsUsed += Math.min(numAvailableMemorySegment, excessMax);

            // avoid remaining buffers by looking at the total capacity that should have been
            // re-distributed up until here
            // the downcast will always succeed, because both arguments of the subtraction are in
            // the 'int' domain
            final int mySize =
                    MathUtils.checkedDownCast(
                            memorySegmentsToDistribute * totalPartsUsed / totalCapacity
                                    - numDistributedMemorySegment);

            numDistributedMemorySegment += mySize;
            bufferPool.setNumBuffers(bufferPool.getNumberOfRequiredMemorySegments() + mySize);
        }

        assert (totalPartsUsed == totalCapacity);
        assert (numDistributedMemorySegment == memorySegmentsToDistribute);
    }

    private String getConfigDescription() {
        return String.format(
                "The total number of network buffers is currently set to %d of %d bytes each. "
                        + "You can increase this number by setting the configuration keys '%s', '%s', and '%s'",
                totalNumberOfMemorySegments,
                memorySegmentSize,
                TaskManagerOptions.NETWORK_MEMORY_FRACTION.key(),
                TaskManagerOptions.NETWORK_MEMORY_MIN.key(),
                TaskManagerOptions.NETWORK_MEMORY_MAX.key());
    }
}
