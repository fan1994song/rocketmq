/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.Inet6Address;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.BrokerConfig;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.SystemClock;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.topic.TopicValidator;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.MessageStoreConfig;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.dledger.DLedgerCommitLog;
import org.apache.rocketmq.store.ha.HAService;
import org.apache.rocketmq.store.index.IndexService;
import org.apache.rocketmq.store.index.QueryOffsetResult;
import org.apache.rocketmq.store.schedule.ScheduleMessageService;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

public class DefaultMessageStore implements MessageStore {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 消息仓库数据存储配置信息
    private final MessageStoreConfig messageStoreConfig;
    // CommitLog
    private final CommitLog commitLog;

    // 消息队列存储缓存表
    private final ConcurrentMap<String/* topic */, ConcurrentMap<Integer/* queueId */, ConsumeQueue>> consumeQueueTable;

    // consume文件刷盘线程
    private final FlushConsumeQueueService flushConsumeQueueService;

    // 清理commitLog文件服务
    private final CleanCommitLogService cleanCommitLogService;

    // 清理consumeQueue文件服务
    private final CleanConsumeQueueService cleanConsumeQueueService;

    // index文件实现类
    private final IndexService indexService;

    // MappedFile分配服务
    private final AllocateMappedFileService allocateMappedFileService;

    // CommitLog消息分发，根据CommitLog文件构建ConsumeQueue、Index文件
    private final ReputMessageService reputMessageService;

    // 存储高可用机制
    private final HAService haService;

    private final ScheduleMessageService scheduleMessageService;

    private final StoreStatsService storeStatsService;

    // 消息堆内存缓存
    private final TransientStorePool transientStorePool;

    private final RunningFlags runningFlags = new RunningFlags();
    private final SystemClock systemClock = new SystemClock();

    private final ScheduledExecutorService scheduledExecutorService =
        Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("StoreScheduledThread"));
    private final BrokerStatsManager brokerStatsManager;
    // 在消息拉取长轮询模式下的消息达到监听器
    private final MessageArrivingListener messageArrivingListener;
    // broker配置
    private final BrokerConfig brokerConfig;

    private volatile boolean shutdown = true;

    // 文件刷盘检查点 checkpoint
    private StoreCheckpoint storeCheckpoint;

    private AtomicLong printTimes = new AtomicLong(0);

    private final AtomicInteger lmqConsumeQueueNum = new AtomicInteger(0);

    // CommitLog文件转发请求
    private final LinkedList<CommitLogDispatcher> dispatcherList;

    private RandomAccessFile lockFile;

    private FileLock lock;

    boolean shutDownNormal = false;

    private final ScheduledExecutorService diskCheckScheduledExecutorService =
            Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("DiskCheckScheduledThread"));

    public DefaultMessageStore(final MessageStoreConfig messageStoreConfig, final BrokerStatsManager brokerStatsManager,
        final MessageArrivingListener messageArrivingListener, final BrokerConfig brokerConfig) throws IOException {
        this.messageArrivingListener = messageArrivingListener;
        this.brokerConfig = brokerConfig;
        this.messageStoreConfig = messageStoreConfig;
        this.brokerStatsManager = brokerStatsManager;
        this.allocateMappedFileService = new AllocateMappedFileService(this);
        // 如果开启dleger高可用配置
        if (messageStoreConfig.isEnableDLegerCommitLog()) {
            this.commitLog = new DLedgerCommitLog(this);
        } else {
            this.commitLog = new CommitLog(this);
        }
        this.consumeQueueTable = new ConcurrentHashMap<>(32);

        this.flushConsumeQueueService = new FlushConsumeQueueService();
        this.cleanCommitLogService = new CleanCommitLogService();
        this.cleanConsumeQueueService = new CleanConsumeQueueService();
        this.storeStatsService = new StoreStatsService();
        this.indexService = new IndexService(this);
        // 未开启，仍使用haService主从同步
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService = new HAService(this);
        } else {
            this.haService = null;
        }
        this.reputMessageService = new ReputMessageService();

        this.scheduleMessageService = new ScheduleMessageService(this);

        this.transientStorePool = new TransientStorePool(messageStoreConfig);

        if (messageStoreConfig.isTransientStorePoolEnable()) {
            this.transientStorePool.init();
        }

        this.allocateMappedFileService.start();

        this.indexService.start();

        this.dispatcherList = new LinkedList<>();
        this.dispatcherList.addLast(new CommitLogDispatcherBuildConsumeQueue());
        this.dispatcherList.addLast(new CommitLogDispatcherBuildIndex());

        File file = new File(StorePathConfigHelper.getLockFile(messageStoreConfig.getStorePathRootDir()));
        MappedFile.ensureDirOK(file.getParent());
        MappedFile.ensureDirOK(getStorePathPhysic());
        MappedFile.ensureDirOK(getStorePathLogic());
        lockFile = new RandomAccessFile(file, "rw");
    }

    public void truncateDirtyLogicFiles(long phyOffset) {
        ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

        for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.truncateDirtyLogicFiles(phyOffset);
            }
        }
    }

    /**
     * @throws IOException
     */
    public boolean load() {
        boolean result = true;

        try {
            // 判断上一次退出是否是异常退出
            // 原理：启动时创建了/store/abort文件，正常退出，注册JVM钩子删除该文件，若文件存在说明是异常退出
            boolean lastExitOK = !this.isTempFileExist();
            log.info("last shutdown {}", lastExitOK ? "normally" : "abnormally");

            // load Commit Log
            // 加载commitLog下MappedFile数据
            result = result && this.commitLog.load();

            // load Consume Queue
            // 加载消费队列数据
            result = result && this.loadConsumeQueue();

            if (result) {
                // 加载保存点信息
                this.storeCheckpoint =
                    new StoreCheckpoint(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));

                // 加载index信息
                this.indexService.load(lastExitOK);

                this.recover(lastExitOK);

                log.info("load over, and the max phy offset = {}", this.getMaxPhyOffset());

                // 延迟队列在这里（加载并调用start()启动）
                if (null != scheduleMessageService) {
                    result =  this.scheduleMessageService.load();
                }
            }

        } catch (Exception e) {
            log.error("load exception", e);
            result = false;
        }

        if (!result) {
            this.allocateMappedFileService.shutdown();
        }

        return result;
    }

    /**
     * @throws Exception
     */
    public void start() throws Exception {

        lock = lockFile.getChannel().tryLock(0, 1, false);
        if (lock == null || lock.isShared() || !lock.isValid()) {
            throw new RuntimeException("Lock failed,MQ already started");
        }

        lockFile.getChannel().write(ByteBuffer.wrap("lock".getBytes()));
        lockFile.getChannel().force(true);
        {
            /**
             * 1. Make sure the fast-forward messages to be truncated during the recovering according to the max physical offset of the commitlog;
             * 2. DLedger committedPos may be missing, so the maxPhysicalPosInLogicQueue maybe bigger that maxOffset returned by DLedgerCommitLog, just let it go;
             * 3. Calculate the reput offset according to the consume queue;
             * 4. Make sure the fall-behind messages to be dispatched before starting the commitlog, especially when the broker role are automatically changed.
             */
            long maxPhysicalPosInLogicQueue = commitLog.getMinOffset();
            for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
                for (ConsumeQueue logic : maps.values()) {
                    if (logic.getMaxPhysicOffset() > maxPhysicalPosInLogicQueue) {
                        maxPhysicalPosInLogicQueue = logic.getMaxPhysicOffset();
                    }
                }
            }
            if (maxPhysicalPosInLogicQueue < 0) {
                maxPhysicalPosInLogicQueue = 0;
            }
            if (maxPhysicalPosInLogicQueue < this.commitLog.getMinOffset()) {
                maxPhysicalPosInLogicQueue = this.commitLog.getMinOffset();
                /**
                 * This happens in following conditions:
                 * 1. If someone removes all the consumequeue files or the disk get damaged.
                 * 2. Launch a new broker, and copy the commitlog from other brokers.
                 *
                 * All the conditions has the same in common that the maxPhysicalPosInLogicQueue should be 0.
                 * If the maxPhysicalPosInLogicQueue is gt 0, there maybe something wrong.
                 */
                log.warn("[TooSmallCqOffset] maxPhysicalPosInLogicQueue={} clMinOffset={}", maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset());
            }
            log.info("[SetReputOffset] maxPhysicalPosInLogicQueue={} clMinOffset={} clMaxOffset={} clConfirmedOffset={}",
                maxPhysicalPosInLogicQueue, this.commitLog.getMinOffset(), this.commitLog.getMaxOffset(), this.commitLog.getConfirmOffset());
            this.reputMessageService.setReputFromOffset(maxPhysicalPosInLogicQueue);
            this.reputMessageService.start();

            /**
             *  1. Finish dispatching the messages fall behind, then to start other services.
             *  2. DLedger committedPos may be missing, so here just require dispatchBehindBytes <= 0
             */
            while (true) {
                if (dispatchBehindBytes() <= 0) {
                    break;
                }
                Thread.sleep(1000);
                log.info("Try to finish doing reput the messages fall behind during the starting, reputOffset={} maxOffset={} behind={}", this.reputMessageService.getReputFromOffset(), this.getMaxPhyOffset(), this.dispatchBehindBytes());
            }
            this.recoverTopicQueueTable();
        }

        // 当未开启dleger时
        if (!messageStoreConfig.isEnableDLegerCommitLog()) {
            this.haService.start();
            // 延迟消息处理器启动
            this.handleScheduleMessageService(messageStoreConfig.getBrokerRole());
        }

        this.flushConsumeQueueService.start();
        this.commitLog.start();
        this.storeStatsService.start();

        this.createTempFile();
        this.addScheduleTask();
        this.shutdown = false;
    }

    public void shutdown() {
        if (!this.shutdown) {
            this.shutdown = true;

            this.scheduledExecutorService.shutdown();
            this.diskCheckScheduledExecutorService.shutdown();
            try {

                Thread.sleep(1000);
            } catch (InterruptedException e) {
                log.error("shutdown Exception, ", e);
            }

            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.shutdown();
            }
            if (this.haService != null) {
                this.haService.shutdown();
            }

            this.storeStatsService.shutdown();
            this.indexService.shutdown();
            this.commitLog.shutdown();
            this.reputMessageService.shutdown();
            this.flushConsumeQueueService.shutdown();
            this.allocateMappedFileService.shutdown();
            this.storeCheckpoint.flush();
            this.storeCheckpoint.shutdown();

            if (this.runningFlags.isWriteable() && dispatchBehindBytes() == 0) {
                this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
                shutDownNormal = true;
            } else {
                log.warn("the store may be wrong, so shutdown abnormally, and keep abort file.");
            }
        }

        this.transientStorePool.destroy();

        if (lockFile != null && lock != null) {
            try {
                lock.release();
                lockFile.close();
            } catch (IOException e) {
            }
        }
    }

    public void destroy() {
        this.destroyLogics();
        this.commitLog.destroy();
        this.indexService.destroy();
        this.deleteFile(StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir()));
        this.deleteFile(StorePathConfigHelper.getStoreCheckpoint(this.messageStoreConfig.getStorePathRootDir()));
    }

    public void destroyLogics() {
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.destroy();
            }
        }
    }

    private PutMessageStatus checkMessage(MessageExtBrokerInner msg) {
        if (msg.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + msg.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        if (msg.getPropertiesString() != null && msg.getPropertiesString().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long " + msg.getPropertiesString().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }
        return PutMessageStatus.PUT_OK;
    }

    private PutMessageStatus checkMessages(MessageExtBatch messageExtBatch) {
        if (messageExtBatch.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long " + messageExtBatch.getTopic().length());
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        if (messageExtBatch.getBody().length > messageStoreConfig.getMaxMessageSize()) {
            log.warn("PutMessages body length too long " + messageExtBatch.getBody().length);
            return PutMessageStatus.MESSAGE_ILLEGAL;
        }

        return PutMessageStatus.PUT_OK;
    }

    /**
     * 检查broker当前存储状态，若是关闭状态、slave节点、是否可写入、pageCache是否繁忙 去校验当前消息的写入请求
     * @return
     */
    private PutMessageStatus checkStoreStatus() {
        if (this.shutdown) {
            log.warn("message store has shutdown, so putMessage is forbidden");
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        if (BrokerRole.SLAVE == this.messageStoreConfig.getBrokerRole()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("broke role is slave, so putMessage is forbidden");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        }

        if (!this.runningFlags.isWriteable()) {
            long value = this.printTimes.getAndIncrement();
            if ((value % 50000) == 0) {
                log.warn("the message store is not writable. It may be caused by one of the following reasons: " +
                    "the broker's disk is full, write to logic queue error, write to index file error, etc");
            }
            return PutMessageStatus.SERVICE_NOT_AVAILABLE;
        } else {
            this.printTimes.set(0);
        }

        if (this.isOSPageCacheBusy()) {
            return PutMessageStatus.OS_PAGECACHE_BUSY;
        }
        return PutMessageStatus.PUT_OK;
    }

    private PutMessageStatus checkLmqMessage(MessageExtBrokerInner msg) {
        if (msg.getProperties() != null
            && StringUtils.isNotBlank(msg.getProperty(MessageConst.PROPERTY_INNER_MULTI_DISPATCH))
            && this.isLmqConsumeQueueNumExceeded()) {
            return PutMessageStatus.LMQ_CONSUME_QUEUE_NUM_EXCEEDED;
        }
        return PutMessageStatus.PUT_OK;
    }

    private boolean isLmqConsumeQueueNumExceeded() {
        if (this.getMessageStoreConfig().isEnableLmq() && this.getMessageStoreConfig().isEnableMultiDispatch()
            && this.lmqConsumeQueueNum.get() > this.messageStoreConfig.getMaxLmqConsumeQueueNum()) {
            return true;
        }
        return false;
    }

    @Override
    public CompletableFuture<PutMessageResult> asyncPutMessage(MessageExtBrokerInner msg) {
        // 检查broker存储
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        // 检查消息的topic长度、属性长度
        PutMessageStatus msgCheckStatus = this.checkMessage(msg);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        // 校验消息是否为LMQ消息，超过了LMQ消耗队列数目
        PutMessageStatus lmqMsgCheckStatus = this.checkLmqMessage(msg);
        if (msgCheckStatus == PutMessageStatus.LMQ_CONSUME_QUEUE_NUM_EXCEEDED) {
            return CompletableFuture.completedFuture(new PutMessageResult(lmqMsgCheckStatus, null));
        }


        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> putResultFuture = this.commitLog.asyncPutMessage(msg);

        putResultFuture.thenAccept((result) -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("putMessage not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, msg.getBody().length);
            }
            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return putResultFuture;
    }
    public CompletableFuture<PutMessageResult> asyncPutMessages(MessageExtBatch messageExtBatch) {
        PutMessageStatus checkStoreStatus = this.checkStoreStatus();
        if (checkStoreStatus != PutMessageStatus.PUT_OK) {
            return CompletableFuture.completedFuture(new PutMessageResult(checkStoreStatus, null));
        }

        PutMessageStatus msgCheckStatus = this.checkMessages(messageExtBatch);
        if (msgCheckStatus == PutMessageStatus.MESSAGE_ILLEGAL) {
            return CompletableFuture.completedFuture(new PutMessageResult(msgCheckStatus, null));
        }

        long beginTime = this.getSystemClock().now();
        CompletableFuture<PutMessageResult> resultFuture = this.commitLog.asyncPutMessages(messageExtBatch);

        resultFuture.thenAccept((result) -> {
            long elapsedTime = this.getSystemClock().now() - beginTime;
            if (elapsedTime > 500) {
                log.warn("not in lock elapsed time(ms)={}, bodyLength={}", elapsedTime, messageExtBatch.getBody().length);
            }

            this.storeStatsService.setPutMessageEntireTimeMax(elapsedTime);

            if (null == result || !result.isOk()) {
                this.storeStatsService.getPutMessageFailedTimes().add(1);
            }
        });

        return resultFuture;
    }

    @Override
    public PutMessageResult putMessage(MessageExtBrokerInner msg) {
        return waitForPutResult(asyncPutMessage(msg));
    }

    @Override
    public PutMessageResult putMessages(MessageExtBatch messageExtBatch) {
        return waitForPutResult(asyncPutMessages(messageExtBatch));
    }

    private PutMessageResult waitForPutResult(CompletableFuture<PutMessageResult> putMessageResultFuture) {
        try {
            int putMessageTimeout =
                    Math.max(this.messageStoreConfig.getSyncFlushTimeout(),
                            this.messageStoreConfig.getSlaveTimeout()) + 5000;
            return putMessageResultFuture.get(putMessageTimeout, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException e) {
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        } catch (TimeoutException e) {
            log.error("usually it will never timeout, putMessageTimeout is much bigger than slaveTimeout and "
                    + "flushTimeout so the result can be got anyway, but in some situations timeout will happen like full gc "
                    + "process hangs or other unexpected situations.");
            return new PutMessageResult(PutMessageStatus.UNKNOWN_ERROR, null);
        }
    }

    @Override
    public boolean isOSPageCacheBusy() {
        long begin = this.getCommitLog().getBeginTimeInLock();
        long diff = this.systemClock.now() - begin;

        return diff < 10000000
            && diff > this.messageStoreConfig.getOsPageCacheBusyTimeOutMills();
    }

    @Override
    public long lockTimeMills() {
        return this.commitLog.lockTimeMills();
    }

    public SystemClock getSystemClock() {
        return systemClock;
    }

    public CommitLog getCommitLog() {
        return commitLog;
    }

    /**
     * 获取消息
     * @param group 消息组
     * @param topic topic
     * @param queueId 队列ID
     * @param offset 偏移量
     * @param maxMsgNums 当次查询消息数量，类似:pageSize
     * @param messageFilter 用于筛选所需消息的消息过滤器
     * @return
     */
    public GetMessageResult getMessage(final String group, final String topic, final int queueId, final long offset,
        final int maxMsgNums,
        final MessageFilter messageFilter) {
        // 状态判断
        if (this.shutdown) {
            log.warn("message store has shutdown, so getMessage is forbidden");
            return null;
        }

        // 是否可读
        if (!this.runningFlags.isReadable()) {
            log.warn("message store is not readable, so getMessage is forbidden " + this.runningFlags.getFlagBits());
            return null;
        }

        // %LMQ% TOPIC校验
        if (MixAll.isLmq(topic) && this.isLmqConsumeQueueNumExceeded()) {
            log.warn("message store is not available, broker config enableLmq and enableMultiDispatch, lmq consumeQueue num exceed maxLmqConsumeQueueNum config num");
            return null;
        }

        long beginTime = this.getSystemClock().now();

        // 获取信息状态、下次拉取起始偏移量、最小偏移量、最大偏移量
        GetMessageStatus status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
        long nextBeginOffset = offset;
        long minOffset = 0;
        long maxOffset = 0;

        // lazy init when find msg.
        GetMessageResult getResult = null;

        // 当前commitLog最大偏移量
        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            // 该消息队列最小、最大值
            minOffset = consumeQueue.getMinOffsetInQueue();
            maxOffset = consumeQueue.getMaxOffsetInQueue();

            if (maxOffset == 0) {
                // 最大值为0，无消息，下次开始偏移量
                status = GetMessageStatus.NO_MESSAGE_IN_QUEUE;
                nextBeginOffset = nextOffsetCorrection(offset, 0);
            } else if (offset < minOffset) {
                // 如果请求偏移量小于队列最小偏移量
                status = GetMessageStatus.OFFSET_TOO_SMALL;
                nextBeginOffset = nextOffsetCorrection(offset, minOffset);
            } else if (offset == maxOffset) {
                // 如果请求偏移量等于队列最大偏移量，说明没有最新的消息
                status = GetMessageStatus.OFFSET_OVERFLOW_ONE;
                nextBeginOffset = nextOffsetCorrection(offset, offset);
            } else if (offset > maxOffset) {
                // 如果请求偏移量大于最大偏移量，偏移量溢出严重
                status = GetMessageStatus.OFFSET_OVERFLOW_BADLY;
                // 需要考虑当前队列的偏移量是否为0， 如果当前队列的最小偏移量为0，则使用最小偏移量纠正下次拉取偏移 量。如果当前队列的最小偏移量不为0，则使用该队列的最大偏移量来 纠正下次拉取偏移量
                if (0 == minOffset) {
                    nextBeginOffset = nextOffsetCorrection(offset, minOffset);
                } else {
                    nextBeginOffset = nextOffsetCorrection(offset, maxOffset);
                }
            } else {
                // 获取消息数据（此时可能远大于32条数据）
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(offset);
                if (bufferConsumeQueue != null) {
                    try {
                        status = GetMessageStatus.NO_MATCHED_MESSAGE;

                        long nextPhyFileStartOffset = Long.MIN_VALUE;
                        long maxPhyOffsetPulling = 0;

                        // 最大过滤消息值
                        int i = 0;
                        final int maxFilterMessageCount = Math.max(16000, maxMsgNums * ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        final boolean diskFallRecorded = this.messageStoreConfig.isDiskFallRecorded();

                        getResult = new GetMessageResult(maxMsgNums);

                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        for (; i < bufferConsumeQueue.getSize() && i < maxFilterMessageCount; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            // 获取偏移量、大小、标签code
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            int sizePy = bufferConsumeQueue.getByteBuffer().getInt();
                            long tagsCode = bufferConsumeQueue.getByteBuffer().getLong();

                            maxPhyOffsetPulling = offsetPy;

                            if (nextPhyFileStartOffset != Long.MIN_VALUE) {
                                if (offsetPy < nextPhyFileStartOffset)
                                    continue;
                            }

                            // 在磁盘中
                            boolean isInDisk = checkInDiskByCommitOffset(offsetPy, maxOffsetPy);

                            // 是否已经查找完毕
                            if (this.isTheBatchFull(sizePy, maxMsgNums, getResult.getBufferTotalSize(), getResult.getMessageCount(),
                                isInDisk)) {
                                break;
                            }

                            boolean extRet = false, isTagsCodeLegal = true;
                            if (consumeQueue.isExtAddr(tagsCode)) {
                                extRet = consumeQueue.getExt(tagsCode, cqExtUnit);
                                if (extRet) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    // can't find ext content.Client will filter messages by tag also.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}, topic={}, group={}",
                                        tagsCode, offsetPy, sizePy, topic, group);
                                    isTagsCodeLegal = false;
                                }
                            }

                            // 消息过滤器过滤(如果消息根据ConsumeQueue条目进行过滤，则需要从 CommitLog文件中加载整个消息体，然后根据属性进行过滤。当然如果 过滤方式是TAG模式，该方法默认返回true)
                            if (messageFilter != null
                                && !messageFilter.isMatchedByConsumeQueue(isTagsCodeLegal ? tagsCode : null, extRet ? cqExtUnit : null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }

                                continue;
                            }

                            // 查找单个完整消息
                            SelectMappedBufferResult selectResult = this.commitLog.getMessage(offsetPy, sizePy);
                            if (null == selectResult) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.MESSAGE_WAS_REMOVING;
                                }

                                // 查找下个文件，从起始偏移量开始
                                nextPhyFileStartOffset = this.commitLog.rollNextFile(offsetPy);
                                continue;
                            }

                            // 根据单个完整消息(过滤具体属性信息)
                            if (messageFilter != null
                                && !messageFilter.isMatchedByCommitLog(selectResult.getByteBuffer().slice(), null)) {
                                if (getResult.getBufferTotalSize() == 0) {
                                    status = GetMessageStatus.NO_MATCHED_MESSAGE;
                                }
                                // release...
                                // 引用释放
                                selectResult.release();
                                continue;
                            }

                            // 拉取消息时获取消息计数器
                            this.storeStatsService.getGetMessageTransferedMsgCount().add(1);
                            getResult.addMessage(selectResult);
                            status = GetMessageStatus.FOUND;
                            nextPhyFileStartOffset = Long.MIN_VALUE;
                        }

                        // 默认为true
                        if (diskFallRecorded) {
                            // commitLog最大偏移量-本次拉取最大物理偏移量 的差值（计算后续预计要拉取的消息偏移量）
                            long fallBehind = maxOffsetPy - maxPhyOffsetPulling;
                            // 预计是统计相关，忽略
                            brokerStatsManager.recordDiskFallBehindSize(group, topic, queueId, fallBehind);
                        }

                        // 下次拉取请求的起始偏移量
                        nextBeginOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                        // commitLog最大偏移量-本次拉取最大物理偏移量 的差值（计算后续预计要拉取的消息偏移量）
                        long diff = maxOffsetPy - maxPhyOffsetPulling;
                        // 总物理内存40%的数据量  表示当前需要拉取的消息已经超出了常驻 内存的大小，表示主服务器繁忙，此时才建议从从服务器拉取消息
                        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE
                            * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
                        // diff>40%,去从服务器拉取
                        getResult.setSuggestPullingFromSlave(diff > memory);
                    } finally {
                        // 释放引用
                        bufferConsumeQueue.release();
                    }
                } else {
                    // 访问队列，没找到对应数据,此处应该是异常，下次从下个文件的起始偏移量拉取
                    status = GetMessageStatus.OFFSET_FOUND_NULL;
                    nextBeginOffset = nextOffsetCorrection(offset, consumeQueue.rollNextFile(offset));
                    log.warn("consumer request topic: " + topic + "offset: " + offset + " minOffset: " + minOffset + " maxOffset: "
                        + maxOffset + ", but access logic queue failed.");
                }
            }
        } else {
            // 不存在的队列，下次从0拉取
            status = GetMessageStatus.NO_MATCHED_LOGIC_QUEUE;
            nextBeginOffset = nextOffsetCorrection(offset, 0);
        }

        // 估计是监控平台数据
        if (GetMessageStatus.FOUND == status) {
            this.storeStatsService.getGetMessageTimesTotalFound().add(1);
        } else {
            this.storeStatsService.getGetMessageTimesTotalMiss().add(1);
        }
        // 监控->记录最大消耗时长
        long elapsedTime = this.getSystemClock().now() - beginTime;
        this.storeStatsService.setGetMessageEntireTimeMax(elapsedTime);

        // lazy init no data found.
        if (getResult == null) {
            getResult = new GetMessageResult(0);
        }

        getResult.setStatus(status);
        getResult.setNextBeginOffset(nextBeginOffset);
        getResult.setMaxOffset(maxOffset);
        getResult.setMinOffset(minOffset);
        return getResult;
    }

    public long getMaxOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            long offset = logic.getMaxOffsetInQueue();
            return offset;
        }

        return 0;
    }

    public long getMinOffsetInQueue(String topic, int queueId) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getMinOffsetInQueue();
        }

        return -1;
    }

    @Override
    public long getCommitLogOffsetInQueue(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeQueueOffset);
            if (bufferConsumeQueue != null) {
                try {
                    long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                    return offsetPy;
                } finally {
                    bufferConsumeQueue.release();
                }
            }
        }

        return 0;
    }

    public long getOffsetInQueueByTime(String topic, int queueId, long timestamp) {
        ConsumeQueue logic = this.findConsumeQueue(topic, queueId);
        if (logic != null) {
            return logic.getOffsetInQueueByTime(timestamp);
        }

        return 0;
    }

    public MessageExt lookMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return lookMessageByOffset(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, 4);
        if (null != sbr) {
            try {
                // 1 TOTALSIZE
                int size = sbr.getByteBuffer().getInt();
                return this.commitLog.getMessage(commitLogOffset, size);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    @Override
    public SelectMappedBufferResult selectOneMessageByOffset(long commitLogOffset, int msgSize) {
        return this.commitLog.getMessage(commitLogOffset, msgSize);
    }

    public String getRunningDataInfo() {
        return this.storeStatsService.toString();
    }

    public String getStorePathPhysic() {
        String storePathPhysic;
        if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog()) {
            storePathPhysic = ((DLedgerCommitLog)DefaultMessageStore.this.getCommitLog()).getdLedgerServer().getdLedgerConfig().getDataStorePath();
        } else {
            storePathPhysic = DefaultMessageStore.this.getMessageStoreConfig().getStorePathCommitLog();
        }
        return storePathPhysic;
    }

    public String getStorePathLogic() {
        return StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir());
    }

    @Override
    public HashMap<String, String> getRuntimeInfo() {
        HashMap<String, String> result = this.storeStatsService.getRuntimeInfo();

        {
            double minPhysicsUsedRatio = Double.MAX_VALUE;
            String commitLogStorePath = getStorePathPhysic();
            String[] paths = commitLogStorePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
            for (String clPath : paths) {
                double physicRatio = UtilAll.isPathExists(clPath) ?
                        UtilAll.getDiskPartitionSpaceUsedPercent(clPath) : -1;
                result.put(RunningStats.commitLogDiskRatio.name() + "_" + clPath, String.valueOf(physicRatio));
                minPhysicsUsedRatio = Math.min(minPhysicsUsedRatio, physicRatio);
            }
            result.put(RunningStats.commitLogDiskRatio.name(), String.valueOf(minPhysicsUsedRatio));
        }

        {
            double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(getStorePathLogic());
            result.put(RunningStats.consumeQueueDiskRatio.name(), String.valueOf(logicsRatio));
        }

        {
            if (this.scheduleMessageService != null) {
                this.scheduleMessageService.buildRunningStats(result);
            }
        }

        result.put(RunningStats.commitLogMinOffset.name(), String.valueOf(DefaultMessageStore.this.getMinPhyOffset()));
        result.put(RunningStats.commitLogMaxOffset.name(), String.valueOf(DefaultMessageStore.this.getMaxPhyOffset()));

        return result;
    }

    @Override
    public long getMaxPhyOffset() {
        return this.commitLog.getMaxOffset();
    }

    @Override
    public long getMinPhyOffset() {
        return this.commitLog.getMinOffset();
    }

    @Override
    public long getEarliestMessageTime(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            long minLogicOffset = logicQueue.getMinLogicOffset();

            SelectMappedBufferResult result = logicQueue.getIndexBuffer(minLogicOffset / ConsumeQueue.CQ_STORE_UNIT_SIZE);
            return getStoreTime(result);
        }

        return -1;
    }

    private long getStoreTime(SelectMappedBufferResult result) {
        if (result != null) {
            try {
                final long phyOffset = result.getByteBuffer().getLong();
                final int size = result.getByteBuffer().getInt();
                long storeTime = this.getCommitLog().pickupStoreTimestamp(phyOffset, size);
                return storeTime;
            } catch (Exception e) {
            } finally {
                result.release();
            }
        }
        return -1;
    }

    @Override
    public long getEarliestMessageTime() {
        final long minPhyOffset = this.getMinPhyOffset();
        final int size = this.messageStoreConfig.getMaxMessageSize() * 2;
        return this.getCommitLog().pickupStoreTimestamp(minPhyOffset, size);
    }

    @Override
    public long getMessageStoreTimeStamp(String topic, int queueId, long consumeQueueOffset) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            SelectMappedBufferResult result = logicQueue.getIndexBuffer(consumeQueueOffset);
            return getStoreTime(result);
        }

        return -1;
    }

    @Override
    public long getMessageTotalInQueue(String topic, int queueId) {
        ConsumeQueue logicQueue = this.findConsumeQueue(topic, queueId);
        if (logicQueue != null) {
            return logicQueue.getMessageTotalInQueue();
        }

        return -1;
    }

    @Override
    public SelectMappedBufferResult getCommitLogData(final long offset) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so getPhyQueueData is forbidden");
            return null;
        }

        return this.commitLog.getData(offset);
    }

    @Override
    public boolean appendToCommitLog(long startOffset, byte[] data, int dataStart, int dataLength) {
        if (this.shutdown) {
            log.warn("message store has shutdown, so appendToPhyQueue is forbidden");
            return false;
        }

        boolean result = this.commitLog.appendData(startOffset, data, dataStart, dataLength);
        if (result) {
            this.reputMessageService.wakeup();
        } else {
            log.error("appendToPhyQueue failed " + startOffset + " " + data.length);
        }

        return result;
    }

    @Override
    public void executeDeleteFilesManually() {
        this.cleanCommitLogService.executeDeleteFilesManually();
    }

    @Override
    public QueryMessageResult queryMessage(String topic, String key, int maxNum, long begin, long end) {
        QueryMessageResult queryMessageResult = new QueryMessageResult();

        long lastQueryMsgTime = end;

        for (int i = 0; i < 3; i++) {
            QueryOffsetResult queryOffsetResult = this.indexService.queryOffset(topic, key, maxNum, begin, lastQueryMsgTime);
            if (queryOffsetResult.getPhyOffsets().isEmpty()) {
                break;
            }

            Collections.sort(queryOffsetResult.getPhyOffsets());

            queryMessageResult.setIndexLastUpdatePhyoffset(queryOffsetResult.getIndexLastUpdatePhyoffset());
            queryMessageResult.setIndexLastUpdateTimestamp(queryOffsetResult.getIndexLastUpdateTimestamp());

            for (int m = 0; m < queryOffsetResult.getPhyOffsets().size(); m++) {
                long offset = queryOffsetResult.getPhyOffsets().get(m);

                try {

                    boolean match = true;
                    MessageExt msg = this.lookMessageByOffset(offset);
                    if (0 == m) {
                        lastQueryMsgTime = msg.getStoreTimestamp();
                    }

//                    String[] keyArray = msg.getKeys().split(MessageConst.KEY_SEPARATOR);
//                    if (topic.equals(msg.getTopic())) {
//                        for (String k : keyArray) {
//                            if (k.equals(key)) {
//                                match = true;
//                                break;
//                            }
//                        }
//                    }

                    if (match) {
                        SelectMappedBufferResult result = this.commitLog.getData(offset, false);
                        if (result != null) {
                            int size = result.getByteBuffer().getInt(0);
                            result.getByteBuffer().limit(size);
                            result.setSize(size);
                            queryMessageResult.addMessage(result);
                        }
                    } else {
                        log.warn("queryMessage hash duplicate, {} {}", topic, key);
                    }
                } catch (Exception e) {
                    log.error("queryMessage exception", e);
                }
            }

            if (queryMessageResult.getBufferTotalSize() > 0) {
                break;
            }

            if (lastQueryMsgTime < begin) {
                break;
            }
        }

        return queryMessageResult;
    }

    @Override
    public void updateHaMasterAddress(String newAddr) {
        this.haService.updateMasterAddress(newAddr);
    }

    @Override
    public long slaveFallBehindMuch() {
        return this.commitLog.getMaxOffset() - this.haService.getPush2SlaveMaxOffset().get();
    }

    @Override
    public long now() {
        return this.systemClock.now();
    }

    @Override
    public int cleanUnusedTopic(Set<String> topics) {
        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();

            if (!topics.contains(topic) && !topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)
                    && !topic.equals(TopicValidator.RMQ_SYS_TRANS_OP_HALF_TOPIC)
                    && !MixAll.isLmq(topic)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                for (ConsumeQueue cq : queueTable.values()) {
                    cq.destroy();
                    log.info("cleanUnusedTopic: {} {} ConsumeQueue cleaned",
                        cq.getTopic(),
                        cq.getQueueId()
                    );

                    this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                }
                it.remove();

                if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                    this.brokerStatsManager.onTopicDeleted(topic);
                }

                log.info("cleanUnusedTopic: {},topic destroyed", topic);
            }
        }

        return 0;
    }

    public void cleanExpiredConsumerQueue() {
        long minCommitLogOffset = this.commitLog.getMinOffset();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            String topic = next.getKey();
            if (!topic.equals(TopicValidator.RMQ_SYS_SCHEDULE_TOPIC)) {
                ConcurrentMap<Integer, ConsumeQueue> queueTable = next.getValue();
                Iterator<Entry<Integer, ConsumeQueue>> itQT = queueTable.entrySet().iterator();
                while (itQT.hasNext()) {
                    Entry<Integer, ConsumeQueue> nextQT = itQT.next();
                    long maxCLOffsetInConsumeQueue = nextQT.getValue().getLastOffset();

                    if (maxCLOffsetInConsumeQueue == -1) {
                        log.warn("maybe ConsumeQueue was created just now. topic={} queueId={} maxPhysicOffset={} minLogicOffset={}.",
                            nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId(),
                            nextQT.getValue().getMaxPhysicOffset(),
                            nextQT.getValue().getMinLogicOffset());
                    } else if (maxCLOffsetInConsumeQueue < minCommitLogOffset) {
                        log.info(
                            "cleanExpiredConsumerQueue: {} {} consumer queue destroyed, minCommitLogOffset: {} maxCLOffsetInConsumeQueue: {}",
                            topic,
                            nextQT.getKey(),
                            minCommitLogOffset,
                            maxCLOffsetInConsumeQueue);

                        DefaultMessageStore.this.commitLog.removeQueueFromTopicQueueTable(nextQT.getValue().getTopic(),
                            nextQT.getValue().getQueueId());

                        nextQT.getValue().destroy();
                        itQT.remove();
                    }
                }

                if (queueTable.isEmpty()) {
                    log.info("cleanExpiredConsumerQueue: {},topic destroyed", topic);
                    it.remove();
                }
            }
        }
    }

    public Map<String, Long> getMessageIds(final String topic, final int queueId, long minOffset, long maxOffset,
        SocketAddress storeHost) {
        Map<String, Long> messageIds = new HashMap<String, Long>();
        if (this.shutdown) {
            return messageIds;
        }

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            minOffset = Math.max(minOffset, consumeQueue.getMinOffsetInQueue());
            maxOffset = Math.min(maxOffset, consumeQueue.getMaxOffsetInQueue());

            if (maxOffset == 0) {
                return messageIds;
            }

            long nextOffset = minOffset;
            while (nextOffset < maxOffset) {
                SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(nextOffset);
                if (bufferConsumeQueue != null) {
                    try {
                        int i = 0;
                        for (; i < bufferConsumeQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                            InetSocketAddress inetSocketAddress = (InetSocketAddress) storeHost;
                            int msgIdLength = (inetSocketAddress.getAddress() instanceof Inet6Address) ? 16 + 4 + 8 : 4 + 4 + 8;
                            final ByteBuffer msgIdMemory = ByteBuffer.allocate(msgIdLength);
                            String msgId =
                                MessageDecoder.createMessageId(msgIdMemory, MessageExt.socketAddress2ByteBuffer(storeHost), offsetPy);
                            messageIds.put(msgId, nextOffset++);
                            if (nextOffset > maxOffset) {
                                return messageIds;
                            }
                        }
                    } finally {

                        bufferConsumeQueue.release();
                    }
                } else {
                    return messageIds;
                }
            }
        }
        return messageIds;
    }

    @Override
    public boolean checkInDiskByConsumeOffset(final String topic, final int queueId, long consumeOffset) {

        final long maxOffsetPy = this.commitLog.getMaxOffset();

        ConsumeQueue consumeQueue = findConsumeQueue(topic, queueId);
        if (consumeQueue != null) {
            SelectMappedBufferResult bufferConsumeQueue = consumeQueue.getIndexBuffer(consumeOffset);
            if (bufferConsumeQueue != null) {
                try {
                    for (int i = 0; i < bufferConsumeQueue.getSize(); ) {
                        i += ConsumeQueue.CQ_STORE_UNIT_SIZE;
                        long offsetPy = bufferConsumeQueue.getByteBuffer().getLong();
                        return checkInDiskByCommitOffset(offsetPy, maxOffsetPy);
                    }
                } finally {

                    bufferConsumeQueue.release();
                }
            } else {
                return false;
            }
        }
        return false;
    }

    @Override
    public long dispatchBehindBytes() {
        return this.reputMessageService.behind();
    }

    @Override
    public long flush() {
        return this.commitLog.flush();
    }

    @Override
    public boolean resetWriteOffset(long phyOffset) {
        return this.commitLog.resetOffset(phyOffset);
    }

    @Override
    public long getConfirmOffset() {
        return this.commitLog.getConfirmOffset();
    }

    @Override
    public void setConfirmOffset(long phyOffset) {
        this.commitLog.setConfirmOffset(phyOffset);
    }

    public MessageExt lookMessageByOffset(long commitLogOffset, int size) {
        SelectMappedBufferResult sbr = this.commitLog.getMessage(commitLogOffset, size);
        if (null != sbr) {
            try {
                return MessageDecoder.decode(sbr.getByteBuffer(), true, false);
            } finally {
                sbr.release();
            }
        }

        return null;
    }

    /**
     * 查找topic下的queueId对应消息队列信息
     * @param topic
     * @param queueId
     * @return
     */
    public ConsumeQueue findConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (null == map) {
            ConcurrentMap<Integer, ConsumeQueue> newMap = new ConcurrentHashMap<Integer, ConsumeQueue>(128);
            ConcurrentMap<Integer, ConsumeQueue> oldMap = consumeQueueTable.putIfAbsent(topic, newMap);
            if (oldMap != null) {
                map = oldMap;
            } else {
                map = newMap;
            }
        }

        ConsumeQueue logic = map.get(queueId);
        if (null == logic) {
            ConsumeQueue newLogic = new ConsumeQueue(
                topic,
                queueId,
                StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                this);
            ConsumeQueue oldLogic = map.putIfAbsent(queueId, newLogic);
            if (oldLogic != null) {
                logic = oldLogic;
            } else {
                if (MixAll.isLmq(topic)) {
                    lmqConsumeQueueNum.getAndIncrement();
                }
                logic = newLogic;
            }
        }

        return logic;
    }

    /**
     * 如果当前Broker为主节点，下次拉取偏移量为 newOffset。如果当前Broker为从节点并且offsetCheckInSlave为true，设置下次拉取偏移量为newOffset
     * @param oldOffset
     * @param newOffset
     * @return
     */
    private long nextOffsetCorrection(long oldOffset, long newOffset) {
        long nextOffset = oldOffset;
        if (this.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE || this.getMessageStoreConfig().isOffsetCheckInSlave()) {
            nextOffset = newOffset;
        }
        return nextOffset;
    }

    private boolean checkInDiskByCommitOffset(long offsetPy, long maxOffsetPy) {
        // memory大小最大是物理磁盘的40%，读取偏移量差值大于40%，则认为是要查询磁盘
        long memory = (long) (StoreUtil.TOTAL_PHYSICAL_MEMORY_SIZE * (this.messageStoreConfig.getAccessMessageInMemoryMaxRatio() / 100.0));
        return (maxOffsetPy - offsetPy) > memory;
    }

    /**
     *
     * @param sizePy
     * @param maxMsgNums 最大消息数值(默认32)
     * @param bufferTotal 消息buffer总计数值
     * @param messageTotal 已找到的消息数量
     * @param isInDisk 是否在磁盘
     * @return
     */
    private boolean isTheBatchFull(int sizePy, int maxMsgNums, int bufferTotal, int messageTotal, boolean isInDisk) {

        if (0 == bufferTotal || 0 == messageTotal) {
            return false;
        }

        if (maxMsgNums <= messageTotal) {
            return true;
        }

        if (isInDisk) {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInDisk()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInDisk() - 1) {
                return true;
            }
        } else {
            if ((bufferTotal + sizePy) > this.messageStoreConfig.getMaxTransferBytesOnMessageInMemory()) {
                return true;
            }

            if (messageTotal > this.messageStoreConfig.getMaxTransferCountOnMessageInMemory() - 1) {
                return true;
            }
        }

        return false;
    }

    private void deleteFile(final String fileName) {
        File file = new File(fileName);
        boolean result = file.delete();
        log.info(fileName + (result ? " delete OK" : " delete Failed"));
    }

    /**
     * @throws IOException
     */
    private void createTempFile() throws IOException {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        MappedFile.ensureDirOK(file.getParent());
        boolean result = file.createNewFile();
        log.info(fileName + (result ? " create OK" : " already exists"));
    }

    /**
     * 存储定时任务
     */
    private void addScheduleTask() {

        /**
         * 每隔十秒执行cleanFilesPeriodically。检查是否需要清理过期文件
         */
        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.cleanFilesPeriodically();
            }
        }, 1000 * 60, this.messageStoreConfig.getCleanResourceInterval(), TimeUnit.MILLISECONDS);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                DefaultMessageStore.this.checkSelf();
            }
        }, 1, 10, TimeUnit.MINUTES);

        this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                if (DefaultMessageStore.this.getMessageStoreConfig().isDebugLockEnable()) {
                    try {
                        if (DefaultMessageStore.this.commitLog.getBeginTimeInLock() != 0) {
                            long lockTime = System.currentTimeMillis() - DefaultMessageStore.this.commitLog.getBeginTimeInLock();
                            if (lockTime > 1000 && lockTime < 10000000) {

                                String stack = UtilAll.jstack();
                                final String fileName = System.getProperty("user.home") + File.separator + "debug/lock/stack-"
                                    + DefaultMessageStore.this.commitLog.getBeginTimeInLock() + "-" + lockTime;
                                MixAll.string2FileNotSafe(stack, fileName);
                            }
                        }
                    } catch (Exception e) {
                    }
                }
            }
        }, 1, 1, TimeUnit.SECONDS);

        // this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
        // @Override
        // public void run() {
        // DefaultMessageStore.this.cleanExpiredConsumerQueue();
        // }
        // }, 1, 1, TimeUnit.HOURS);
        this.diskCheckScheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            public void run() {
                DefaultMessageStore.this.cleanCommitLogService.isSpaceFull();
            }
        }, 1000L, 10000L, TimeUnit.MILLISECONDS);
    }

    /**
     * 清理commitLog、consumeQueue过期文件
     */
    private void cleanFilesPeriodically() {
        this.cleanCommitLogService.run();
        this.cleanConsumeQueueService.run();
    }

    private void checkSelf() {
        this.commitLog.checkSelf();

        Iterator<Entry<String, ConcurrentMap<Integer, ConsumeQueue>>> it = this.consumeQueueTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, ConcurrentMap<Integer, ConsumeQueue>> next = it.next();
            Iterator<Entry<Integer, ConsumeQueue>> itNext = next.getValue().entrySet().iterator();
            while (itNext.hasNext()) {
                Entry<Integer, ConsumeQueue> cq = itNext.next();
                cq.getValue().checkSelf();
            }
        }
    }

    private boolean isTempFileExist() {
        String fileName = StorePathConfigHelper.getAbortFile(this.messageStoreConfig.getStorePathRootDir());
        File file = new File(fileName);
        return file.exists();
    }

    private boolean loadConsumeQueue() {
        File dirLogic = new File(StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()));
        File[] fileTopicList = dirLogic.listFiles();
        if (fileTopicList != null) {

            for (File fileTopic : fileTopicList) {
                String topic = fileTopic.getName();

                File[] fileQueueIdList = fileTopic.listFiles();
                if (fileQueueIdList != null) {
                    for (File fileQueueId : fileQueueIdList) {
                        int queueId;
                        try {
                            queueId = Integer.parseInt(fileQueueId.getName());
                        } catch (NumberFormatException e) {
                            continue;
                        }
                        ConsumeQueue logic = new ConsumeQueue(
                            topic,
                            queueId,
                            StorePathConfigHelper.getStorePathConsumeQueue(this.messageStoreConfig.getStorePathRootDir()),
                            this.getMessageStoreConfig().getMappedFileSizeConsumeQueue(),
                            this);
                        this.putConsumeQueue(topic, queueId, logic);
                        if (!logic.load()) {
                            return false;
                        }
                    }
                }
            }
        }

        log.info("load logics queue all over, OK");

        return true;
    }

    /**
     * 根据broker是否为正常停止，执行不同的恢复策略
     * @param lastExitOK
     */
    private void recover(final boolean lastExitOK) {
        // 获取最大的consumeQueue偏移量
        long maxPhyOffsetOfConsumeQueue = this.recoverConsumeQueue();

        if (lastExitOK) {
            // 正常退出操作
            this.commitLog.recoverNormally(maxPhyOffsetOfConsumeQueue);
        } else {
            // 异常退出操作
            this.commitLog.recoverAbnormally(maxPhyOffsetOfConsumeQueue);
        }

        // 恢复ConsumeQueue文件后，将在CommitLog实例中保存每个消息消费队列当前的存储逻辑偏移量，
        // 这也是消息队列ID中不仅存储主题、消息队列ID还存储了消息队列偏移量的关键所在
        this.recoverTopicQueueTable();
    }

    public MessageStoreConfig getMessageStoreConfig() {
        return messageStoreConfig;
    }

    public TransientStorePool getTransientStorePool() {
        return transientStorePool;
    }

    private void putConsumeQueue(final String topic, final int queueId, final ConsumeQueue consumeQueue) {
        ConcurrentMap<Integer/* queueId */, ConsumeQueue> map = this.consumeQueueTable.get(topic);
        if (null == map) {
            map = new ConcurrentHashMap<Integer/* queueId */, ConsumeQueue>();
            map.put(queueId, consumeQueue);
            this.consumeQueueTable.put(topic, map);
            if (MixAll.isLmq(topic)) {
                this.lmqConsumeQueueNum.getAndIncrement();
            }
        } else {
            map.put(queueId, consumeQueue);
        }
    }

    private long recoverConsumeQueue() {
        long maxPhysicOffset = -1;
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                logic.recover();
                if (logic.getMaxPhysicOffset() > maxPhysicOffset) {
                    maxPhysicOffset = logic.getMaxPhysicOffset();
                }
            }
        }

        return maxPhysicOffset;
    }

    public void recoverTopicQueueTable() {
        HashMap<String/* topic-queueid */, Long/* offset */> table = new HashMap<String, Long>(1024);
        long minPhyOffset = this.commitLog.getMinOffset();
        for (ConcurrentMap<Integer, ConsumeQueue> maps : this.consumeQueueTable.values()) {
            for (ConsumeQueue logic : maps.values()) {
                String key = logic.getTopic() + "-" + logic.getQueueId();
                table.put(key, logic.getMaxOffsetInQueue());
                logic.correctMinOffset(minPhyOffset);
            }
        }

        this.commitLog.setTopicQueueTable(table);
    }

    public AllocateMappedFileService getAllocateMappedFileService() {
        return allocateMappedFileService;
    }

    public StoreStatsService getStoreStatsService() {
        return storeStatsService;
    }

    public RunningFlags getAccessRights() {
        return runningFlags;
    }

    public ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> getConsumeQueueTable() {
        return consumeQueueTable;
    }

    public StoreCheckpoint getStoreCheckpoint() {
        return storeCheckpoint;
    }

    public HAService getHaService() {
        return haService;
    }

    public ScheduleMessageService getScheduleMessageService() {
        return scheduleMessageService;
    }

    public RunningFlags getRunningFlags() {
        return runningFlags;
    }

    public void doDispatch(DispatchRequest req) {
        for (CommitLogDispatcher dispatcher : this.dispatcherList) {
            dispatcher.dispatch(req);
        }
    }

    public void putMessagePositionInfo(DispatchRequest dispatchRequest) {
        // 查找到消息队列
        ConsumeQueue cq = this.findConsumeQueue(dispatchRequest.getTopic(), dispatchRequest.getQueueId());
        cq.putMessagePositionInfoWrapper(dispatchRequest, checkMultiDispatchQueue(dispatchRequest));
    }

    private boolean checkMultiDispatchQueue(DispatchRequest dispatchRequest) {
        if (!this.messageStoreConfig.isEnableMultiDispatch()) {
            return false;
        }
        Map<String, String> prop = dispatchRequest.getPropertiesMap();
        if (prop == null || prop.isEmpty()) {
            return false;
        }
        String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
        String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
        if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
            return false;
        }
        return true;
    }

    @Override
    public BrokerStatsManager getBrokerStatsManager() {
        return brokerStatsManager;
    }

    @Override
    public void handleScheduleMessageService(final BrokerRole brokerRole) {
        if (this.scheduleMessageService != null) {
            if (brokerRole == BrokerRole.SLAVE) {
                this.scheduleMessageService.shutdown();
            } else {
                this.scheduleMessageService.start();
            }
        }

    }

    @Override
    public void cleanUnusedLmqTopic(String topic) {
        if (this.consumeQueueTable.containsKey(topic)) {
            ConcurrentMap<Integer, ConsumeQueue> map = this.consumeQueueTable.get(topic);
            if (map != null) {
                ConsumeQueue cq = map.get(0);
                cq.destroy();
                log.info("cleanUnusedLmqTopic: {} {} ConsumeQueue cleaned",
                    cq.getTopic(),
                    cq.getQueueId()
                );

                this.commitLog.removeQueueFromTopicQueueTable(cq.getTopic(), cq.getQueueId());
                this.lmqConsumeQueueNum.getAndDecrement();
            }
            this.consumeQueueTable.remove(topic);
            if (this.brokerConfig.isAutoDeleteUnusedStats()) {
                this.brokerStatsManager.onTopicDeleted(topic);
            }
            log.info("cleanUnusedLmqTopic: {},topic destroyed", topic);
        }
    }

    public int remainTransientStoreBufferNumbs() {
        return this.transientStorePool.availableBufferNums();
    }

    @Override
    public boolean isTransientStorePoolDeficient() {
        return remainTransientStoreBufferNumbs() == 0;
    }

    @Override
    public LinkedList<CommitLogDispatcher> getDispatcherList() {
        return this.dispatcherList;
    }

    @Override
    public ConsumeQueue getConsumeQueue(String topic, int queueId) {
        ConcurrentMap<Integer, ConsumeQueue> map = consumeQueueTable.get(topic);
        if (map == null) {
            return null;
        }
        return map.get(queueId);
    }

    public void unlockMappedFile(final MappedFile mappedFile) {
        this.scheduledExecutorService.schedule(new Runnable() {
            @Override
            public void run() {
                mappedFile.munlock();
            }
        }, 6, TimeUnit.SECONDS);
    }

    /**
     * ReputMessageService线程将新加入消息通过该方法，通知给consumeQueue去构建消息消费队列信息
     */
    class CommitLogDispatcherBuildConsumeQueue implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            final int tranType = MessageSysFlag.getTransactionValue(request.getSysFlag());
            switch (tranType) {
                // 不是事务消息、事务消息提交，将消息写入consumerQueue的日志中
                case MessageSysFlag.TRANSACTION_NOT_TYPE:
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                    DefaultMessageStore.this.putMessagePositionInfo(request);
                    break;
                    // 事物预消息、回滚事物，不构建consumeQueue，所以消费队列中查不到
                case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    break;
            }
        }
    }

    class CommitLogDispatcherBuildIndex implements CommitLogDispatcher {

        @Override
        public void dispatch(DispatchRequest request) {
            // 默认启用构建index信息
            if (DefaultMessageStore.this.messageStoreConfig.isMessageIndexEnable()) {
                DefaultMessageStore.this.indexService.buildIndex(request);
            }
        }
    }

    class CleanCommitLogService {

        private final static int MAX_MANUAL_DELETE_FILE_TIMES = 20;
        private final double diskSpaceWarningLevelRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceWarningLevelRatio", "0.90"));

        private final double diskSpaceCleanForciblyRatio =
            Double.parseDouble(System.getProperty("rocketmq.broker.diskSpaceCleanForciblyRatio", "0.85"));
        private long lastRedeleteTimestamp = 0;

        private volatile int manualDeleteFileSeveralTimes = 0;

        private volatile boolean cleanImmediately = false;

        public void executeDeleteFilesManually() {
            this.manualDeleteFileSeveralTimes = MAX_MANUAL_DELETE_FILE_TIMES;
            DefaultMessageStore.log.info("executeDeleteFilesManually was invoked");
        }

        public void run() {
            try {
                this.deleteExpiredFiles();
                // 重新删除延迟删除的文件（兜底）
                this.redeleteHangedFile();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            int deleteCount = 0;
            // 配置，默认值72小时，文件保留时间
            long fileReservedTime = DefaultMessageStore.this.getMessageStoreConfig().getFileReservedTime();
            // 删除物理文件间隔时间，默认100
            int deletePhysicFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteCommitLogFilesInterval();
            // 表示第一次拒绝删除之后能保留文件的最大时间，默认值：两分钟
            int destroyMapedFileIntervalForcibly = DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();

            // 凌晨4点这个时间点执行删除操作
            boolean timeup = this.isTimeToDelete();
            // 磁盘空间不充足
            boolean spacefull = this.isSpaceToDelete();
            // 预留手工触发机制
            boolean manualDelete = this.manualDeleteFileSeveralTimes > 0;

            if (timeup || spacefull || manualDelete) {

                if (manualDelete)
                    this.manualDeleteFileSeveralTimes--;

                boolean cleanAtOnce = DefaultMessageStore.this.getMessageStoreConfig().isCleanFileForciblyEnable() && this.cleanImmediately;

                log.info("begin to delete before {} hours file. timeup: {} spacefull: {} manualDeleteFileSeveralTimes: {} cleanAtOnce: {}",
                    fileReservedTime,
                    timeup,
                    spacefull,
                    manualDeleteFileSeveralTimes,
                    cleanAtOnce);

                fileReservedTime *= 60 * 60 * 1000;

                deleteCount = DefaultMessageStore.this.commitLog.deleteExpiredFile(fileReservedTime, deletePhysicFilesInterval,
                    destroyMapedFileIntervalForcibly, cleanAtOnce);
                if (deleteCount > 0) {
                } else if (spacefull) {
                    log.warn("disk space will be full soon, but delete file failed.");
                }
            }
        }

        private void redeleteHangedFile() {
            int interval = DefaultMessageStore.this.getMessageStoreConfig().getRedeleteHangedFileInterval();
            long currentTimestamp = System.currentTimeMillis();
            if ((currentTimestamp - this.lastRedeleteTimestamp) > interval) {
                this.lastRedeleteTimestamp = currentTimestamp;
                int destroyMapedFileIntervalForcibly =
                    DefaultMessageStore.this.getMessageStoreConfig().getDestroyMapedFileIntervalForcibly();
                if (DefaultMessageStore.this.commitLog.retryDeleteFirstFile(destroyMapedFileIntervalForcibly)) {
                }
            }
        }

        public String getServiceName() {
            return CleanCommitLogService.class.getSimpleName();
        }

        private boolean isTimeToDelete() {
            String when = DefaultMessageStore.this.getMessageStoreConfig().getDeleteWhen();
            if (UtilAll.isItTimeToDo(when)) {
                DefaultMessageStore.log.info("it's time to reclaim disk space, " + when);
                return true;
            }

            return false;
        }

        private boolean isSpaceToDelete() {
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;

            cleanImmediately = false;

            {
                String commitLogStorePath = DefaultMessageStore.this.getStorePathPhysic();
                String[] storePaths = commitLogStorePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
                Set<String> fullStorePath = new HashSet<>();
                double minPhysicRatio = 100;
                String minStorePath = null;
                for (String storePathPhysic : storePaths) {
                    double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);
                    if (minPhysicRatio > physicRatio) {
                        minPhysicRatio =  physicRatio;
                        minStorePath = storePathPhysic;
                    }
                    if (physicRatio > diskSpaceCleanForciblyRatio) {
                        fullStorePath.add(storePathPhysic);
                    }
                }
                DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
                if (minPhysicRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("physic disk maybe full soon " + minPhysicRatio +
                                ", so mark disk full, storePathPhysic=" + minStorePath);
                    }

                    cleanImmediately = true;
                } else if (minPhysicRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("physic disk space OK " + minPhysicRatio +
                                ", so mark disk ok, storePathPhysic=" + minStorePath);
                    }
                }

                if (minPhysicRatio < 0 || minPhysicRatio > ratio) {
                    DefaultMessageStore.log.info("physic disk maybe full soon, so reclaim space, "
                            + minPhysicRatio + ", storePathPhysic=" + minStorePath);
                    return true;
                }
            }

            {
                String storePathLogics = DefaultMessageStore.this.getStorePathLogic();
                double logicsRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogics);
                if (logicsRatio > diskSpaceWarningLevelRatio) {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                    if (diskok) {
                        DefaultMessageStore.log.error("logics disk maybe full soon " + logicsRatio + ", so mark disk full");
                    }

                    cleanImmediately = true;
                } else if (logicsRatio > diskSpaceCleanForciblyRatio) {
                    cleanImmediately = true;
                } else {
                    boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();
                    if (!diskok) {
                        DefaultMessageStore.log.info("logics disk space OK " + logicsRatio + ", so mark disk ok");
                    }
                }

                if (logicsRatio < 0 || logicsRatio > ratio) {
                    DefaultMessageStore.log.info("logics disk maybe full soon, so reclaim space, " + logicsRatio);
                    return true;
                }
            }

            return false;
        }

        public int getManualDeleteFileSeveralTimes() {
            return manualDeleteFileSeveralTimes;
        }

        public void setManualDeleteFileSeveralTimes(int manualDeleteFileSeveralTimes) {
            this.manualDeleteFileSeveralTimes = manualDeleteFileSeveralTimes;
        }

        public double calcStorePathPhysicRatio() {
            Set<String> fullStorePath = new HashSet<>();
            String storePath = getStorePathPhysic();
            String[] paths = storePath.trim().split(MessageStoreConfig.MULTI_PATH_SPLITTER);
            double minPhysicRatio = 100;
            for (String path : paths) {
                double physicRatio = UtilAll.isPathExists(path) ?
                        UtilAll.getDiskPartitionSpaceUsedPercent(path) : -1;
                minPhysicRatio = Math.min(minPhysicRatio, physicRatio);
                if (physicRatio > diskSpaceCleanForciblyRatio) {
                    fullStorePath.add(path);
                }
            }
            DefaultMessageStore.this.commitLog.setFullStorePaths(fullStorePath);
            return minPhysicRatio;

        }

        public boolean isSpaceFull() {
            double physicRatio = calcStorePathPhysicRatio();
            double ratio = DefaultMessageStore.this.getMessageStoreConfig().getDiskMaxUsedSpaceRatio() / 100.0;
            if (physicRatio > ratio) {
                DefaultMessageStore.log.info("physic disk of commitLog used: " + physicRatio);
            }
            if (physicRatio > this.diskSpaceWarningLevelRatio) {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskFull();
                if (diskok) {
                    DefaultMessageStore.log.error("physic disk of commitLog maybe full soon, used " + physicRatio + ", so mark disk full");
                }

                return true;
            } else {
                boolean diskok = DefaultMessageStore.this.runningFlags.getAndMakeDiskOK();

                if (!diskok) {
                    DefaultMessageStore.log.info("physic disk space of commitLog OK " + physicRatio + ", so mark disk ok");
                }

                return false;
            }
        }
    }

    class CleanConsumeQueueService {
        private long lastPhysicalMinOffset = 0;

        public void run() {
            try {
                this.deleteExpiredFiles();
            } catch (Throwable e) {
                DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
            }
        }

        private void deleteExpiredFiles() {
            // 根据commitLog的最小值偏移量来进行consumeQueue的删除
            int deleteLogicsFilesInterval = DefaultMessageStore.this.getMessageStoreConfig().getDeleteConsumeQueueFilesInterval();

            long minOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            if (minOffset > this.lastPhysicalMinOffset) {
                this.lastPhysicalMinOffset = minOffset;

                ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

                for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                    for (ConsumeQueue logic : maps.values()) {
                        int deleteCount = logic.deleteExpiredFile(minOffset);

                        if (deleteCount > 0 && deleteLogicsFilesInterval > 0) {
                            try {
                                Thread.sleep(deleteLogicsFilesInterval);
                            } catch (InterruptedException ignored) {
                            }
                        }
                    }
                }

                DefaultMessageStore.this.indexService.deleteExpiredFile(minOffset);
            }
        }

        public String getServiceName() {
            return CleanConsumeQueueService.class.getSimpleName();
        }
    }

    class FlushConsumeQueueService extends ServiceThread {
        private static final int RETRY_TIMES_OVER = 3;
        private long lastFlushTimestamp = 0;

        private void doFlush(int retryTimes) {
            int flushConsumeQueueLeastPages = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueLeastPages();

            if (retryTimes == RETRY_TIMES_OVER) {
                flushConsumeQueueLeastPages = 0;
            }

            long logicsMsgTimestamp = 0;

            int flushConsumeQueueThoroughInterval = DefaultMessageStore.this.getMessageStoreConfig().getFlushConsumeQueueThoroughInterval();
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis >= (this.lastFlushTimestamp + flushConsumeQueueThoroughInterval)) {
                this.lastFlushTimestamp = currentTimeMillis;
                flushConsumeQueueLeastPages = 0;
                logicsMsgTimestamp = DefaultMessageStore.this.getStoreCheckpoint().getLogicsMsgTimestamp();
            }

            ConcurrentMap<String, ConcurrentMap<Integer, ConsumeQueue>> tables = DefaultMessageStore.this.consumeQueueTable;

            for (ConcurrentMap<Integer, ConsumeQueue> maps : tables.values()) {
                for (ConsumeQueue cq : maps.values()) {
                    boolean result = false;
                    for (int i = 0; i < retryTimes && !result; i++) {
                        result = cq.flush(flushConsumeQueueLeastPages);
                    }
                }
            }

            if (0 == flushConsumeQueueLeastPages) {
                if (logicsMsgTimestamp > 0) {
                    DefaultMessageStore.this.getStoreCheckpoint().setLogicsMsgTimestamp(logicsMsgTimestamp);
                }
                DefaultMessageStore.this.getStoreCheckpoint().flush();
            }
        }

        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 根据配置，定时任务执行异步刷盘，默认是1秒执行一次
                    int interval = DefaultMessageStore.this.getMessageStoreConfig().getFlushIntervalConsumeQueue();
                    this.waitForRunning(interval);
                    this.doFlush(1);
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            // 兜底操作
            this.doFlush(RETRY_TIMES_OVER);

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return FlushConsumeQueueService.class.getSimpleName();
        }

        /**
         * 释放该线程，等待60秒，为了将内存数据写入到磁盘中
         * @return
         */
        @Override
        public long getJointime() {
            return 1000 * 60;
        }
    }

    class ReputMessageService extends ServiceThread {

        /**
         * 推送起始偏移量
         */
        private volatile long reputFromOffset = 0;

        public long getReputFromOffset() {
            return reputFromOffset;
        }

        public void setReputFromOffset(long reputFromOffset) {
            this.reputFromOffset = reputFromOffset;
        }

        @Override
        public void shutdown() {
            // 关闭前，保证 reputFromOffset>commitLog最大值，从而保证正常关闭时，consumeQueue写入磁盘的数据一致性
            for (int i = 0; i < 50 && this.isCommitLogAvailable(); i++) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ignored) {
                }
            }

            if (this.isCommitLogAvailable()) {
                log.warn("shutdown ReputMessageService, but commitlog have not finish to be dispatched, CL: {} reputFromOffset: {}",
                    DefaultMessageStore.this.commitLog.getMaxOffset(), this.reputFromOffset);
            }

            super.shutdown();
        }

        public long behind() {
            return DefaultMessageStore.this.commitLog.getMaxOffset() - this.reputFromOffset;
        }

        private boolean isCommitLogAvailable() {
            return this.reputFromOffset < DefaultMessageStore.this.commitLog.getMaxOffset();
        }

        private void doReput() {
            // 偏移量小于最小偏移量，重置为commitLog最小偏移量
            if (this.reputFromOffset < DefaultMessageStore.this.commitLog.getMinOffset()) {
                log.warn("The reputFromOffset={} is smaller than minPyOffset={}, this usually indicate that the dispatch behind too much and the commitlog has expired.",
                    this.reputFromOffset, DefaultMessageStore.this.commitLog.getMinOffset());
                this.reputFromOffset = DefaultMessageStore.this.commitLog.getMinOffset();
            }
            for (boolean doNext = true; this.isCommitLogAvailable() && doNext; ) {

                // 如果允许消息重复，将重新推送偏移量设置为CommitLog文件的提交偏移量，如果不允许重复推送，则设置重新推送偏移为CommitLog的 当前最大偏移量
                if (DefaultMessageStore.this.getMessageStoreConfig().isDuplicationEnable()
                    && this.reputFromOffset >= DefaultMessageStore.this.getConfirmOffset()) {
                    break;
                }

                // 根据偏移量获取commitLog中，偏移量所属mappedFile的偏移量之后的所有数据返回(单个mappedFile中)
                SelectMappedBufferResult result = DefaultMessageStore.this.commitLog.getData(reputFromOffset);
                if (result != null) {
                    try {
                        this.reputFromOffset = result.getStartOffset();

                        // 循环拆分遍历话题纬度的新消息
                        for (int readSize = 0; readSize < result.getSize() && doNext; ) {
                            // 校验并返回消息信息
                            DispatchRequest dispatchRequest =
                                DefaultMessageStore.this.commitLog.checkMessageAndReturnSize(result.getByteBuffer(), false, false);
                            int size = dispatchRequest.getBufferSize() == -1 ? dispatchRequest.getMsgSize() : dispatchRequest.getBufferSize();

                            if (dispatchRequest.isSuccess()) {
                                if (size > 0) {
                                    // 校验成功且消息数量大于0
                                    DefaultMessageStore.this.doDispatch(dispatchRequest);

                                    // 主节点且开启了长轮训模式且消息到达监听存在
                                    if (BrokerRole.SLAVE != DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole()
                                            && DefaultMessageStore.this.brokerConfig.isLongPollingEnable()
                                            && DefaultMessageStore.this.messageArrivingListener != null) {
                                        // 消息到达通知：立即告知该topic的queueId下的所有休眠线程去匹配执行拉取消息
                                        DefaultMessageStore.this.messageArrivingListener.arriving(dispatchRequest.getTopic(),
                                            dispatchRequest.getQueueId(), dispatchRequest.getConsumeQueueOffset() + 1,
                                            dispatchRequest.getTagsCode(), dispatchRequest.getStoreTimestamp(),
                                            dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());

                                        notifyMessageArrive4MultiQueue(dispatchRequest);
                                    }

                                    this.reputFromOffset += size;
                                    readSize += size;
                                    if (DefaultMessageStore.this.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE) {
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicTimesTotal(dispatchRequest.getTopic()).add(1);
                                        DefaultMessageStore.this.storeStatsService
                                            .getSinglePutMessageTopicSizeTotal(dispatchRequest.getTopic())
                                            .add(dispatchRequest.getMsgSize());
                                    }
                                } else if (size == 0) {
                                    this.reputFromOffset = DefaultMessageStore.this.commitLog.rollNextFile(this.reputFromOffset);
                                    readSize = result.getSize();
                                }
                            } else if (!dispatchRequest.isSuccess()) {

                                if (size > 0) {
                                    log.error("[BUG]read total count not equals msg total size. reputFromOffset={}", reputFromOffset);
                                    this.reputFromOffset += size;
                                } else {
                                    doNext = false;
                                    // If user open the dledger pattern or the broker is master node,
                                    // it will not ignore the exception and fix the reputFromOffset variable
                                    if (DefaultMessageStore.this.getMessageStoreConfig().isEnableDLegerCommitLog() ||
                                        DefaultMessageStore.this.brokerConfig.getBrokerId() == MixAll.MASTER_ID) {
                                        log.error("[BUG]dispatch message to consume queue error, COMMITLOG OFFSET: {}",
                                            this.reputFromOffset);
                                        this.reputFromOffset += result.getSize() - readSize;
                                    }
                                }
                            }
                        }
                    } finally {
                        result.release();
                    }
                } else {
                    // 不存在最新数据，跳出循环
                    doNext = false;
                }
            }
        }

        private void notifyMessageArrive4MultiQueue(DispatchRequest dispatchRequest) {
            Map<String, String> prop = dispatchRequest.getPropertiesMap();
            if (prop == null) {
                return;
            }
            String multiDispatchQueue = prop.get(MessageConst.PROPERTY_INNER_MULTI_DISPATCH);
            String multiQueueOffset = prop.get(MessageConst.PROPERTY_INNER_MULTI_QUEUE_OFFSET);
            if (StringUtils.isBlank(multiDispatchQueue) || StringUtils.isBlank(multiQueueOffset)) {
                return;
            }
            String[] queues = multiDispatchQueue.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
            String[] queueOffsets = multiQueueOffset.split(MixAll.MULTI_DISPATCH_QUEUE_SPLITTER);
            if (queues.length != queueOffsets.length) {
                return;
            }
            for (int i = 0; i < queues.length; i++) {
                String queueName = queues[i];
                long queueOffset = Long.parseLong(queueOffsets[i]);
                int queueId = dispatchRequest.getQueueId();
                if (DefaultMessageStore.this.getMessageStoreConfig().isEnableLmq() && MixAll.isLmq(queueName)) {
                    queueId = 0;
                }
                DefaultMessageStore.this.messageArrivingListener.arriving(
                    queueName, queueId, queueOffset + 1, dispatchRequest.getTagsCode(),
                    dispatchRequest.getStoreTimestamp(), dispatchRequest.getBitMap(), dispatchRequest.getPropertiesMap());
            }
        }

        @Override
        public void run() {
            DefaultMessageStore.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 休眠一毫秒，继续尝试推送消息到ConsumeQueue和index文件
                    Thread.sleep(1);
                    // 消息转发
                    this.doReput();
                } catch (Exception e) {
                    DefaultMessageStore.log.warn(this.getServiceName() + " service has exception. ", e);
                }
            }

            DefaultMessageStore.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReputMessageService.class.getSimpleName();
        }

    }
}
