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
package org.apache.rocketmq.client.impl.producer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.QueryResult;
import org.apache.rocketmq.client.Validators;
import org.apache.rocketmq.client.common.ClientErrorCode;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.exception.RequestTimeoutException;
import org.apache.rocketmq.client.hook.CheckForbiddenContext;
import org.apache.rocketmq.client.hook.CheckForbiddenHook;
import org.apache.rocketmq.client.hook.EndTransactionContext;
import org.apache.rocketmq.client.hook.EndTransactionHook;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.impl.CommunicationMode;
import org.apache.rocketmq.client.impl.MQClientManager;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.latency.MQFaultStrategy;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.LocalTransactionExecuter;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.RequestCallback;
import org.apache.rocketmq.client.producer.RequestFutureHolder;
import org.apache.rocketmq.client.producer.RequestResponseFuture;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.ServiceState;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageBatch;
import org.apache.rocketmq.common.message.MessageClientIDSetter;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageType;
import org.apache.rocketmq.common.protocol.NamespaceUtil;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.EndTransactionRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.utils.CorrelationIdUtil;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.exception.RemotingTooMuchRequestException;

public class DefaultMQProducerImpl implements MQProducerInner {
    private final InternalLogger log = ClientLogger.getLog();
    private final Random random = new Random();
    private final DefaultMQProducer defaultMQProducer;
    private final ConcurrentMap<String/* topic */, TopicPublishInfo> topicPublishInfoTable =
        new ConcurrentHashMap<String, TopicPublishInfo>();
    private final ArrayList<SendMessageHook> sendMessageHookList = new ArrayList<SendMessageHook>();
    private final ArrayList<EndTransactionHook> endTransactionHookList = new ArrayList<EndTransactionHook>();
    private final RPCHook rpcHook;
    private final BlockingQueue<Runnable> asyncSenderThreadPoolQueue;
    private final ExecutorService defaultAsyncSenderExecutor;
    protected BlockingQueue<Runnable> checkRequestQueue;
    protected ExecutorService checkExecutor;
    private ServiceState serviceState = ServiceState.CREATE_JUST;
    private MQClientInstance mQClientFactory;
    private ArrayList<CheckForbiddenHook> checkForbiddenHookList = new ArrayList<CheckForbiddenHook>();
    private int zipCompressLevel = Integer.parseInt(System.getProperty(MixAll.MESSAGE_COMPRESS_LEVEL, "5"));
    private MQFaultStrategy mqFaultStrategy = new MQFaultStrategy();
    private ExecutorService asyncSenderExecutor;

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer) {
        this(defaultMQProducer, null);
    }

    public DefaultMQProducerImpl(final DefaultMQProducer defaultMQProducer, RPCHook rpcHook) {
        this.defaultMQProducer = defaultMQProducer;
        this.rpcHook = rpcHook;

        this.asyncSenderThreadPoolQueue = new LinkedBlockingQueue<Runnable>(50000);
        // 线程数就是Java虚拟机可用的处理器数量
        this.defaultAsyncSenderExecutor = new ThreadPoolExecutor(
            Runtime.getRuntime().availableProcessors(),
            Runtime.getRuntime().availableProcessors(),
            1000 * 60,
            TimeUnit.MILLISECONDS,
            this.asyncSenderThreadPoolQueue,
            new ThreadFactory() {
                private AtomicInteger threadIndex = new AtomicInteger(0);

                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "AsyncSenderExecutor_" + this.threadIndex.incrementAndGet());
                }
            });
    }

    public void registerCheckForbiddenHook(CheckForbiddenHook checkForbiddenHook) {
        this.checkForbiddenHookList.add(checkForbiddenHook);
        log.info("register a new checkForbiddenHook. hookName={}, allHookSize={}", checkForbiddenHook.hookName(),
            checkForbiddenHookList.size());
    }

    public void initTransactionEnv() {
        TransactionMQProducer producer = (TransactionMQProducer) this.defaultMQProducer;
        if (producer.getExecutorService() != null) {
            this.checkExecutor = producer.getExecutorService();
        } else {
            this.checkRequestQueue = new LinkedBlockingQueue<Runnable>(producer.getCheckRequestHoldMax());
            this.checkExecutor = new ThreadPoolExecutor(
                producer.getCheckThreadPoolMinSize(),
                producer.getCheckThreadPoolMaxSize(),
                1000 * 60,
                TimeUnit.MILLISECONDS,
                this.checkRequestQueue);
        }
    }

    public void destroyTransactionEnv() {
        if (this.checkExecutor != null) {
            this.checkExecutor.shutdown();
        }
    }

    public void registerSendMessageHook(final SendMessageHook hook) {
        this.sendMessageHookList.add(hook);
        log.info("register sendMessage Hook, {}", hook.hookName());
    }

    public void registerEndTransactionHook(final EndTransactionHook hook) {
        this.endTransactionHookList.add(hook);
        log.info("register endTransaction Hook, {}", hook.hookName());
    }

    public void start() throws MQClientException {
        this.start(true);
    }

    public void start(final boolean startFactory) throws MQClientException {
        switch (this.serviceState) {
            case CREATE_JUST:
                this.serviceState = ServiceState.START_FAILED;

                // check配置，改变实例名称为进程ID
                this.checkConfig();

                if (!this.defaultMQProducer.getProducerGroup().equals(MixAll.CLIENT_INNER_PRODUCER_GROUP)) {
                    this.defaultMQProducer.changeInstanceNameToPID();
                }

                this.mQClientFactory = MQClientManager.getInstance().getOrCreateMQClientInstance(this.defaultMQProducer, rpcHook);

                // 向MQClientInstance注册服务，将当前生产者加入 MQClientInstance管理
                boolean registerOK = mQClientFactory.registerProducer(this.defaultMQProducer.getProducerGroup(), this);
                if (!registerOK) {
                    this.serviceState = ServiceState.CREATE_JUST;
                    throw new MQClientException("The producer group[" + this.defaultMQProducer.getProducerGroup()
                        + "] has been created before, specify another name please." + FAQUrl.suggestTodo(FAQUrl.GROUP_NAME_DUPLICATE_URL),
                        null);
                }

                // 设置默认的创建topic
                this.topicPublishInfoTable.put(this.defaultMQProducer.getCreateTopicKey(), new TopicPublishInfo());

                if (startFactory) {
                    mQClientFactory.start();
                }

                log.info("the producer [{}] start OK. sendMessageWithVIPChannel={}", this.defaultMQProducer.getProducerGroup(),
                    this.defaultMQProducer.isSendMessageWithVIPChannel());
                this.serviceState = ServiceState.RUNNING;
                break;
            case RUNNING:
            case START_FAILED:
            case SHUTDOWN_ALREADY:
                throw new MQClientException("The producer service state not OK, maybe started once, "
                    + this.serviceState
                    + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                    null);
            default:
                break;
        }

        this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();

        RequestFutureHolder.getInstance().startScheduledTask(this);

    }

    private void checkConfig() throws MQClientException {
        Validators.checkGroup(this.defaultMQProducer.getProducerGroup());

        if (null == this.defaultMQProducer.getProducerGroup()) {
            throw new MQClientException("producerGroup is null", null);
        }

        if (this.defaultMQProducer.getProducerGroup().equals(MixAll.DEFAULT_PRODUCER_GROUP)) {
            throw new MQClientException("producerGroup can not equal " + MixAll.DEFAULT_PRODUCER_GROUP + ", please specify another one.",
                null);
        }
    }

    public void shutdown() {
        this.shutdown(true);
    }

    public void shutdown(final boolean shutdownFactory) {
        switch (this.serviceState) {
            case CREATE_JUST:
                break;
            case RUNNING:
                this.mQClientFactory.unregisterProducer(this.defaultMQProducer.getProducerGroup());
                this.defaultAsyncSenderExecutor.shutdown();
                if (shutdownFactory) {
                    this.mQClientFactory.shutdown();
                }
                RequestFutureHolder.getInstance().shutdown(this);
                log.info("the producer [{}] shutdown OK", this.defaultMQProducer.getProducerGroup());
                this.serviceState = ServiceState.SHUTDOWN_ALREADY;
                break;
            case SHUTDOWN_ALREADY:
                break;
            default:
                break;
        }
    }

    @Override
    public Set<String> getPublishTopicList() {
        Set<String> topicList = new HashSet<String>();
        for (String key : this.topicPublishInfoTable.keySet()) {
            topicList.add(key);
        }

        return topicList;
    }

    @Override
    public boolean isPublishTopicNeedUpdate(String topic) {
        TopicPublishInfo prev = this.topicPublishInfoTable.get(topic);

        return null == prev || !prev.ok();
    }

    /**
     * @deprecated This method will be removed in the version 5.0.0 and {@link DefaultMQProducerImpl#getCheckListener} is recommended.
     */
    @Override
    @Deprecated
    public TransactionCheckListener checkListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionCheckListener();
        }

        return null;
    }

    @Override
    public TransactionListener getCheckListener() {
        if (this.defaultMQProducer instanceof TransactionMQProducer) {
            TransactionMQProducer producer = (TransactionMQProducer) defaultMQProducer;
            return producer.getTransactionListener();
        }
        return null;
    }

    @Override
    public void checkTransactionState(final String addr, final MessageExt msg,
        final CheckTransactionStateRequestHeader header) {
        Runnable request = new Runnable() {
            private final String brokerAddr = addr;
            private final MessageExt message = msg;
            private final CheckTransactionStateRequestHeader checkRequestHeader = header;
            private final String group = DefaultMQProducerImpl.this.defaultMQProducer.getProducerGroup();

            @Override
            public void run() {
                // 得到事务监听处理类
                TransactionCheckListener transactionCheckListener = DefaultMQProducerImpl.this.checkListener();
                TransactionListener transactionListener = getCheckListener();
                if (transactionCheckListener != null || transactionListener != null) {
                    LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
                    Throwable exception = null;
                    try {
                        // transactionListener不为空，调用checkLocalTransaction()方法，得到本地事务的处理状态
                        if (transactionCheckListener != null) {
                            localTransactionState = transactionCheckListener.checkLocalTransactionState(message);
                        } else if (transactionListener != null) {
                            log.debug("Used new check API in transaction message");
                            localTransactionState = transactionListener.checkLocalTransaction(message);
                        } else {
                            log.warn("CheckTransactionState, pick transactionListener by group[{}] failed", group);
                        }
                    } catch (Throwable e) {
                        log.error("Broker call checkTransactionState, but checkLocalTransactionState exception", e);
                        exception = e;
                    }

                    // 处理结果，发送最终结果给broker端
                    this.processTransactionState(
                        localTransactionState,
                        group,
                        exception);
                } else {
                    log.warn("CheckTransactionState, pick transactionCheckListener by group[{}] failed", group);
                }
            }

            private void processTransactionState(
                final LocalTransactionState localTransactionState,
                final String producerGroup,
                final Throwable exception) {
                final EndTransactionRequestHeader thisHeader = new EndTransactionRequestHeader();
                thisHeader.setCommitLogOffset(checkRequestHeader.getCommitLogOffset());
                thisHeader.setProducerGroup(producerGroup);
                thisHeader.setTranStateTableOffset(checkRequestHeader.getTranStateTableOffset());
                thisHeader.setFromTransactionCheck(true);

                String uniqueKey = message.getProperties().get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                if (uniqueKey == null) {
                    uniqueKey = message.getMsgId();
                }
                thisHeader.setMsgId(uniqueKey);
                thisHeader.setTransactionId(checkRequestHeader.getTransactionId());
                switch (localTransactionState) {
                    case COMMIT_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                        break;
                    case ROLLBACK_MESSAGE:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                        log.warn("when broker check, client rollback this transaction, {}", thisHeader);
                        break;
                    case UNKNOW:
                        thisHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                        log.warn("when broker check, client does not know this transaction state, {}", thisHeader);
                        break;
                    default:
                        break;
                }

                String remark = null;
                if (exception != null) {
                    remark = "checkLocalTransactionState Exception: " + RemotingHelper.exceptionSimpleDesc(exception);
                }
                doExecuteEndTransactionHook(msg, uniqueKey, brokerAddr, localTransactionState, true);

                try {
                    DefaultMQProducerImpl.this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, thisHeader, remark,
                        3000);
                } catch (Exception e) {
                    log.error("endTransactionOneway exception", e);
                }
            }
        };

        this.checkExecutor.submit(request);
    }

    @Override
    public void updateTopicPublishInfo(final String topic, final TopicPublishInfo info) {
        if (info != null && topic != null) {
            TopicPublishInfo prev = this.topicPublishInfoTable.put(topic, info);
            if (prev != null) {
                log.info("updateTopicPublishInfo prev is not null, " + prev);
            }
        }
    }

    @Override
    public boolean isUnitMode() {
        return this.defaultMQProducer.isUnitMode();
    }

    public void createTopic(String key, String newTopic, int queueNum) throws MQClientException {
        createTopic(key, newTopic, queueNum, 0);
    }

    public void createTopic(String key, String newTopic, int queueNum, int topicSysFlag) throws MQClientException {
        this.makeSureStateOK();
        Validators.checkTopic(newTopic);
        Validators.isSystemTopic(newTopic);

        this.mQClientFactory.getMQAdminImpl().createTopic(key, newTopic, queueNum, topicSysFlag);
    }

    private void makeSureStateOK() throws MQClientException {
        if (this.serviceState != ServiceState.RUNNING) {
            throw new MQClientException("The producer service state not OK, "
                + this.serviceState
                + FAQUrl.suggestTodo(FAQUrl.CLIENT_SERVICE_NOT_OK),
                null);
        }
    }

    public List<MessageQueue> fetchPublishMessageQueues(String topic) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().fetchPublishMessageQueues(topic);
    }

    public long searchOffset(MessageQueue mq, long timestamp) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().searchOffset(mq, timestamp);
    }

    public long maxOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().maxOffset(mq);
    }

    public long minOffset(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().minOffset(mq);
    }

    public long earliestMsgStoreTime(MessageQueue mq) throws MQClientException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().earliestMsgStoreTime(mq);
    }

    public MessageExt viewMessage(
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        this.makeSureStateOK();

        return this.mQClientFactory.getMQAdminImpl().viewMessage(msgId);
    }

    public QueryResult queryMessage(String topic, String key, int maxNum, long begin, long end)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessage(topic, key, maxNum, begin, end);
    }

    public MessageExt queryMessageByUniqKey(String topic, String uniqKey)
        throws MQClientException, InterruptedException {
        this.makeSureStateOK();
        return this.mQClientFactory.getMQAdminImpl().queryMessageByUniqKey(topic, uniqKey);
    }

    /**
     * DEFAULT ASYNC -------------------------------------------------------
     */
    public void send(Message msg,
        SendCallback sendCallback) throws MQClientException, RemotingException, InterruptedException {
        // 发送消息超时时间，默认3秒
        send(msg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @deprecated
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws RejectedExecutionException
     */
    @Deprecated
    public void send(final Message msg, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            sendDefaultImpl(msg, CommunicationMode.ASYNC, sendCallback, timeout - costTime);
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(
                            new RemotingTooMuchRequestException("DEFAULT ASYNC send call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    public MessageQueue selectOneMessageQueue(final TopicPublishInfo tpInfo, final String lastBrokerName) {
        return this.mqFaultStrategy.selectOneMessageQueue(tpInfo, lastBrokerName);
    }

    public void updateFaultItem(final String brokerName, final long currentLatency, boolean isolation) {
        this.mqFaultStrategy.updateFaultItem(brokerName, currentLatency, isolation);
    }

    private void validateNameServerSetting() throws MQClientException {
        List<String> nsList = this.getmQClientFactory().getMQClientAPIImpl().getNameServerAddressList();
        if (null == nsList || nsList.isEmpty()) {
            throw new MQClientException(
                "No name server address, please set it." + FAQUrl.suggestTodo(FAQUrl.NAME_SERVER_ADDR_NOT_EXIST_URL), null).setResponseCode(ClientErrorCode.NO_NAME_SERVER_EXCEPTION);
        }

    }

    /**
     * 发送消息
     */
    private SendResult sendDefaultImpl(
        Message msg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        // 服务器当前状态校验、topic是否属于当前broker、消息体大小校验
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        final long invokeID = random.nextLong();
        long beginTimestampFirst = System.currentTimeMillis();
        long beginTimestampPrev = beginTimestampFirst;
        long endTimestamp = beginTimestampFirst;
        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            boolean callTimeout = false;
            MessageQueue mq = null;
            Exception exception = null;
            SendResult sendResult = null;
            int timesTotal = communicationMode == CommunicationMode.SYNC ? 1 + this.defaultMQProducer.getRetryTimesWhenSendFailed() : 1;
            int times = 0;
            String[] brokersSent = new String[timesTotal];
            for (; times < timesTotal; times++) {
                // 从这里可以看出，避开异常broker只是消息重试时操作，对期间其他发送者的投递消息选择broker无影响，除非其他投递也失败，去重试时会被不可用broker列表影响到
                String lastBrokerName = null == mq ? null : mq.getBrokerName();
                // 根据上次发送的brokerName，来决定本次向哪个broker发送消息
                MessageQueue mqSelected = this.selectOneMessageQueue(topicPublishInfo, lastBrokerName);
                if (mqSelected != null) {
                    mq = mqSelected;
                    brokersSent[times] = mq.getBrokerName();
                    try {
                        beginTimestampPrev = System.currentTimeMillis();
                        if (times > 0) {
                            //Reset topic with namespace during resend.
                            msg.setTopic(this.defaultMQProducer.withNamespace(msg.getTopic()));
                        }
                        long costTime = beginTimestampPrev - beginTimestampFirst;
                        if (timeout < costTime) {
                            callTimeout = true;
                            break;
                        }

                        // 真正的发送消息到broker逻辑
                        sendResult = this.sendKernelImpl(msg, mq, communicationMode, sendCallback, topicPublishInfo, timeout - costTime);
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        switch (communicationMode) {
                            case ASYNC:
                                return null;
                            case ONEWAY:
                                return null;
                            case SYNC:
                                if (sendResult.getSendStatus() != SendStatus.SEND_OK) {
                                    // 同步发送不返回SEND_OK,若配置为true，自行重试(默认为false)
                                    if (this.defaultMQProducer.isRetryAnotherBrokerWhenNotStoreOK()) {
                                        continue;
                                    }
                                }

                                return sendResult;
                            default:
                                break;
                        }
                    } catch (RemotingException e) {
                        endTimestamp = System.currentTimeMillis();
                        // 如果异常，会操作记录失败的broker信息，来规避短时间内对该broker的再次调用
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQClientException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        continue;
                    } catch (MQBrokerException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, true);
                        log.warn(String.format("sendKernelImpl exception, resend at once, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        exception = e;
                        if (this.defaultMQProducer.getRetryResponseCodes().contains(e.getResponseCode())) {
                            continue;
                        } else {
                            if (sendResult != null) {
                                return sendResult;
                            }

                            throw e;
                        }
                    } catch (InterruptedException e) {
                        endTimestamp = System.currentTimeMillis();
                        this.updateFaultItem(mq.getBrokerName(), endTimestamp - beginTimestampPrev, false);
                        log.warn(String.format("sendKernelImpl exception, throw exception, InvokeID: %s, RT: %sms, Broker: %s", invokeID, endTimestamp - beginTimestampPrev, mq), e);
                        log.warn(msg.toString());
                        throw e;
                    }
                } else {
                    break;
                }
            }

            if (sendResult != null) {
                return sendResult;
            }

            String info = String.format("Send [%d] times, still failed, cost [%d]ms, Topic: %s, BrokersSent: %s",
                times,
                System.currentTimeMillis() - beginTimestampFirst,
                msg.getTopic(),
                Arrays.toString(brokersSent));

            info += FAQUrl.suggestTodo(FAQUrl.SEND_MSG_FAILED);

            MQClientException mqClientException = new MQClientException(info, exception);
            if (callTimeout) {
                throw new RemotingTooMuchRequestException("sendDefaultImpl call timeout");
            }

            if (exception instanceof MQBrokerException) {
                mqClientException.setResponseCode(((MQBrokerException) exception).getResponseCode());
            } else if (exception instanceof RemotingConnectException) {
                mqClientException.setResponseCode(ClientErrorCode.CONNECT_BROKER_EXCEPTION);
            } else if (exception instanceof RemotingTimeoutException) {
                mqClientException.setResponseCode(ClientErrorCode.ACCESS_BROKER_TIMEOUT);
            } else if (exception instanceof MQClientException) {
                mqClientException.setResponseCode(ClientErrorCode.BROKER_NOT_EXIST_EXCEPTION);
            }

            throw mqClientException;
        }

        validateNameServerSetting();

        throw new MQClientException("No route info of this topic: " + msg.getTopic() + FAQUrl.suggestTodo(FAQUrl.NO_TOPIC_ROUTE_INFO),
            null).setResponseCode(ClientErrorCode.NOT_FOUND_TOPIC_EXCEPTION);
    }

    private TopicPublishInfo tryToFindTopicPublishInfo(final String topic) {
        TopicPublishInfo topicPublishInfo = this.topicPublishInfoTable.get(topic);
        // 如果没有该topic的路由信息，创建默认topicPublishInfo，向nameServer发送获取该topic对象broker的请求
        if (null == topicPublishInfo || !topicPublishInfo.ok()) {
            this.topicPublishInfoTable.putIfAbsent(topic, new TopicPublishInfo());
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
        }
        // 若从请求中获取到，返回拿到的broker数据
        if (topicPublishInfo.isHaveTopicRouterInfo() || topicPublishInfo.ok()) {
            return topicPublishInfo;
        } else {
            // 否则向nameServer再次发起获取默认topic的路由信息请求
            this.mQClientFactory.updateTopicRouteInfoFromNameServer(topic, true, this.defaultMQProducer);
            topicPublishInfo = this.topicPublishInfoTable.get(topic);
            return topicPublishInfo;
        }
    }

    private SendResult sendKernelImpl(final Message msg,
        final MessageQueue mq,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback,
        final TopicPublishInfo topicPublishInfo,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        if (null == brokerAddr) {
            tryToFindTopicPublishInfo(mq.getTopic());
            brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(mq.getBrokerName());
        }

        SendMessageContext context = null;
        if (brokerAddr != null) {
            brokerAddr = MixAll.brokerVIPChannel(this.defaultMQProducer.isSendMessageWithVIPChannel(), brokerAddr);

            byte[] prevBody = msg.getBody();
            try {
                // 对于MessageBatch，在生成过程中已设置ID
                //for MessageBatch,ID has been set in the generating process
                if (!(msg instanceof MessageBatch)) {
                    // 若不是批量批量消息，设置msg唯一ID
                    MessageClientIDSetter.setUniqID(msg);
                }

                boolean topicWithNamespace = false;
                if (null != this.mQClientFactory.getClientConfig().getNamespace()) {
                    msg.setInstanceId(this.mQClientFactory.getClientConfig().getNamespace());
                    topicWithNamespace = true;
                }

                // 消息体大于4M，进行zip压缩，并记录 sysFlag 系统标识
                int sysFlag = 0;
                boolean msgBodyCompressed = false;
                if (this.tryToCompressMessage(msg)) {
                    sysFlag |= MessageSysFlag.COMPRESSED_FLAG;
                    msgBodyCompressed = true;
                }

                // 标记为事务消息
                final String tranMsg = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                if (Boolean.parseBoolean(tranMsg)) {
                    sysFlag |= MessageSysFlag.TRANSACTION_PREPARED_TYPE;
                }

                if (hasCheckForbiddenHook()) {
                    CheckForbiddenContext checkForbiddenContext = new CheckForbiddenContext();
                    checkForbiddenContext.setNameSrvAddr(this.defaultMQProducer.getNamesrvAddr());
                    checkForbiddenContext.setGroup(this.defaultMQProducer.getProducerGroup());
                    checkForbiddenContext.setCommunicationMode(communicationMode);
                    checkForbiddenContext.setBrokerAddr(brokerAddr);
                    checkForbiddenContext.setMessage(msg);
                    checkForbiddenContext.setMq(mq);
                    checkForbiddenContext.setUnitMode(this.isUnitMode());
                    this.executeCheckForbiddenHook(checkForbiddenContext);
                }

                /**
                 * 调用钩子函数，通知正在发送的消息内容，执行发送前逻辑（可自定义扩展的部分）
                 */
                if (this.hasSendMessageHook()) {
                    context = new SendMessageContext();
                    context.setProducer(this);
                    context.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                    context.setCommunicationMode(communicationMode);
                    context.setBornHost(this.defaultMQProducer.getClientIP());
                    context.setBrokerAddr(brokerAddr);
                    context.setMessage(msg);
                    context.setMq(mq);
                    context.setNamespace(this.defaultMQProducer.getNamespace());
                    String isTrans = msg.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
                    if (isTrans != null && isTrans.equals("true")) {
                        context.setMsgType(MessageType.Trans_Msg_Half);
                    }

                    if (msg.getProperty("__STARTDELIVERTIME") != null || msg.getProperty(MessageConst.PROPERTY_DELAY_TIME_LEVEL) != null) {
                        context.setMsgType(MessageType.Delay_Msg);
                    }
                    this.executeSendMessageHookBefore(context);
                }

                // 构造发送消息请求的header
                SendMessageRequestHeader requestHeader = new SendMessageRequestHeader();
                requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
                requestHeader.setTopic(msg.getTopic());
                requestHeader.setDefaultTopic(this.defaultMQProducer.getCreateTopicKey());
                requestHeader.setDefaultTopicQueueNums(this.defaultMQProducer.getDefaultTopicQueueNums());
                requestHeader.setQueueId(mq.getQueueId());
                requestHeader.setSysFlag(sysFlag);
                requestHeader.setBornTimestamp(System.currentTimeMillis());
                requestHeader.setFlag(msg.getFlag());
                requestHeader.setProperties(MessageDecoder.messageProperties2String(msg.getProperties()));
                requestHeader.setReconsumeTimes(0);
                requestHeader.setUnitMode(this.isUnitMode());
                requestHeader.setBatch(msg instanceof MessageBatch);
                // 若是重试topic，将重试时间、最大重试时间放入header发送，具体目的未知？
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    String reconsumeTimes = MessageAccessor.getReconsumeTime(msg);
                    if (reconsumeTimes != null) {
                        requestHeader.setReconsumeTimes(Integer.valueOf(reconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_RECONSUME_TIME);
                    }

                    String maxReconsumeTimes = MessageAccessor.getMaxReconsumeTimes(msg);
                    if (maxReconsumeTimes != null) {
                        requestHeader.setMaxReconsumeTimes(Integer.valueOf(maxReconsumeTimes));
                        MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_MAX_RECONSUME_TIMES);
                    }
                }

                SendResult sendResult = null;
                switch (communicationMode) {
                    case ASYNC:
                        Message tmpMessage = msg;
                        boolean messageCloned = false;
                        if (msgBodyCompressed) {
                            //If msg body was compressed, msgbody should be reset using prevBody.
                            //Clone new message using commpressed message body and recover origin massage.
                            //Fix bug:https://github.com/apache/rocketmq-externals/issues/66
                            tmpMessage = MessageAccessor.cloneMessage(msg);
                            messageCloned = true;
                            msg.setBody(prevBody);
                        }

                        if (topicWithNamespace) {
                            if (!messageCloned) {
                                tmpMessage = MessageAccessor.cloneMessage(msg);
                                messageCloned = true;
                            }
                            msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
                        }

                        long costTimeAsync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeAsync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            tmpMessage,
                            requestHeader,
                            timeout - costTimeAsync,
                            communicationMode,
                            sendCallback,
                            topicPublishInfo,
                            this.mQClientFactory,
                            this.defaultMQProducer.getRetryTimesWhenSendAsyncFailed(),
                            context,
                            this);
                        break;
                    case ONEWAY:
                    case SYNC:
                        long costTimeSync = System.currentTimeMillis() - beginStartTime;
                        if (timeout < costTimeSync) {
                            throw new RemotingTooMuchRequestException("sendKernelImpl call timeout");
                        }
                        sendResult = this.mQClientFactory.getMQClientAPIImpl().sendMessage(
                            brokerAddr,
                            mq.getBrokerName(),
                            msg,
                            requestHeader,
                            timeout - costTimeSync,
                            communicationMode,
                            context,
                            this);
                        break;
                    default:
                        assert false;
                        break;
                }

                // 执行钩子函数的后置处理
                if (this.hasSendMessageHook()) {
                    context.setSendResult(sendResult);
                    this.executeSendMessageHookAfter(context);
                }

                return sendResult;
            } catch (RemotingException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (MQBrokerException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } catch (InterruptedException e) {
                if (this.hasSendMessageHook()) {
                    context.setException(e);
                    this.executeSendMessageHookAfter(context);
                }
                throw e;
            } finally {
                msg.setBody(prevBody);
                msg.setTopic(NamespaceUtil.withoutNamespace(msg.getTopic(), this.defaultMQProducer.getNamespace()));
            }
        }

        throw new MQClientException("The broker[" + mq.getBrokerName() + "] not exist", null);
    }

    public MQClientInstance getmQClientFactory() {
        return mQClientFactory;
    }

    private boolean tryToCompressMessage(final Message msg) {
        if (msg instanceof MessageBatch) {
            //batch dose not support compressing right now
            return false;
        }
        byte[] body = msg.getBody();
        if (body != null) {
            if (body.length >= this.defaultMQProducer.getCompressMsgBodyOverHowmuch()) {
                try {
                    byte[] data = UtilAll.compress(body, zipCompressLevel);
                    if (data != null) {
                        msg.setBody(data);
                        return true;
                    }
                } catch (IOException e) {
                    log.error("tryToCompressMessage exception", e);
                    log.warn(msg.toString());
                }
            }
        }

        return false;
    }

    public boolean hasCheckForbiddenHook() {
        return !checkForbiddenHookList.isEmpty();
    }

    public void executeCheckForbiddenHook(final CheckForbiddenContext context) throws MQClientException {
        if (hasCheckForbiddenHook()) {
            for (CheckForbiddenHook hook : checkForbiddenHookList) {
                hook.checkForbidden(context);
            }
        }
    }

    public boolean hasSendMessageHook() {
        return !this.sendMessageHookList.isEmpty();
    }

    public void executeSendMessageHookBefore(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageBefore(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookBefore", e);
                }
            }
        }
    }

    public void executeSendMessageHookAfter(final SendMessageContext context) {
        if (!this.sendMessageHookList.isEmpty()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    log.warn("failed to executeSendMessageHookAfter", e);
                }
            }
        }
    }

    public boolean hasEndTransactionHook() {
        return !this.endTransactionHookList.isEmpty();
    }

    public void executeEndTransactionHook(final EndTransactionContext context) {
        if (!this.endTransactionHookList.isEmpty()) {
            for (EndTransactionHook hook : this.endTransactionHookList) {
                try {
                    hook.endTransaction(context);
                } catch (Throwable e) {
                    log.warn("failed to executeEndTransactionHook", e);
                }
            }
        }
    }

    public void doExecuteEndTransactionHook(Message msg, String msgId, String brokerAddr, LocalTransactionState state,
        boolean fromTransactionCheck) {
        if (hasEndTransactionHook()) {
            EndTransactionContext context = new EndTransactionContext();
            context.setProducerGroup(defaultMQProducer.getProducerGroup());
            context.setBrokerAddr(brokerAddr);
            context.setMessage(msg);
            context.setMsgId(msgId);
            context.setTransactionId(msg.getTransactionId());
            context.setTransactionState(state);
            context.setFromTransactionCheck(fromTransactionCheck);
            executeEndTransactionHook(context);
        }
    }

    /**
     * DEFAULT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg) throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendDefaultImpl(msg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * KERNEL SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueue mq)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, mq, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueue mq, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        if (!msg.getTopic().equals(mq.getTopic())) {
            throw new MQClientException("message's topic not equal mq's topic", null);
        }

        long costTime = System.currentTimeMillis() - beginStartTime;
        if (timeout < costTime) {
            throw new RemotingTooMuchRequestException("call timeout");
        }

        return this.sendKernelImpl(msg, mq, CommunicationMode.SYNC, null, null, timeout);
    }

    /**
     * KERNEL ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueue mq, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, mq, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * @deprecated
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param mq
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueue mq, final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        makeSureStateOK();
                        Validators.checkMessage(msg, defaultMQProducer);

                        if (!msg.getTopic().equals(mq.getTopic())) {
                            throw new MQClientException("message's topic not equal mq's topic", null);
                        }
                        long costTime = System.currentTimeMillis() - beginStartTime;
                        if (timeout > costTime) {
                            try {
                                sendKernelImpl(msg, mq, CommunicationMode.ASYNC, sendCallback, null,
                                    timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } else {
                            sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                        }
                    } catch (Exception e) {
                        sendCallback.onException(e);
                    }

                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }

    }

    /**
     * KERNEL ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg,
        MessageQueue mq) throws MQClientException, RemotingException, InterruptedException {
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        try {
            this.sendKernelImpl(msg, mq, CommunicationMode.ONEWAY, null, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    /**
     * SELECT SYNC -------------------------------------------------------
     */
    public SendResult send(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, selector, arg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public SendResult send(Message msg, MessageQueueSelector selector, Object arg, long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendSelectImpl(msg, selector, arg, CommunicationMode.SYNC, null, timeout);
    }

    private SendResult sendSelectImpl(
        Message msg,
        MessageQueueSelector selector,
        Object arg,
        final CommunicationMode communicationMode,
        final SendCallback sendCallback, final long timeout
    ) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginStartTime = System.currentTimeMillis();
        this.makeSureStateOK();
        Validators.checkMessage(msg, this.defaultMQProducer);

        TopicPublishInfo topicPublishInfo = this.tryToFindTopicPublishInfo(msg.getTopic());
        if (topicPublishInfo != null && topicPublishInfo.ok()) {
            MessageQueue mq = null;
            try {
                List<MessageQueue> messageQueueList =
                    mQClientFactory.getMQAdminImpl().parsePublishMessageQueues(topicPublishInfo.getMessageQueueList());
                Message userMessage = MessageAccessor.cloneMessage(msg);
                String userTopic = NamespaceUtil.withoutNamespace(userMessage.getTopic(), mQClientFactory.getClientConfig().getNamespace());
                userMessage.setTopic(userTopic);

                mq = mQClientFactory.getClientConfig().queueWithNamespace(selector.select(messageQueueList, userMessage, arg));
            } catch (Throwable e) {
                throw new MQClientException("select message queue threw exception.", e);
            }

            long costTime = System.currentTimeMillis() - beginStartTime;
            if (timeout < costTime) {
                throw new RemotingTooMuchRequestException("sendSelectImpl call timeout");
            }
            if (mq != null) {
                return this.sendKernelImpl(msg, mq, communicationMode, sendCallback, null, timeout - costTime);
            } else {
                throw new MQClientException("select message queue return null.", null);
            }
        }

        validateNameServerSetting();
        throw new MQClientException("No route info for this topic, " + msg.getTopic(), null);
    }

    /**
     * SELECT ASYNC -------------------------------------------------------
     */
    public void send(Message msg, MessageQueueSelector selector, Object arg, SendCallback sendCallback)
        throws MQClientException, RemotingException, InterruptedException {
        send(msg, selector, arg, sendCallback, this.defaultMQProducer.getSendMsgTimeout());
    }

    /**
     * It will be removed at 4.4.0 cause for exception handling and the wrong Semantics of timeout. A new one will be
     * provided in next version
     *
     * @param msg
     * @param selector
     * @param arg
     * @param sendCallback
     * @param timeout      the <code>sendCallback</code> will be invoked at most time
     * @throws MQClientException
     * @throws RemotingException
     * @throws InterruptedException
     */
    @Deprecated
    public void send(final Message msg, final MessageQueueSelector selector, final Object arg,
        final SendCallback sendCallback, final long timeout)
        throws MQClientException, RemotingException, InterruptedException {
        final long beginStartTime = System.currentTimeMillis();
        ExecutorService executor = this.getAsyncSenderExecutor();
        try {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    long costTime = System.currentTimeMillis() - beginStartTime;
                    if (timeout > costTime) {
                        try {
                            try {
                                sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, sendCallback,
                                    timeout - costTime);
                            } catch (MQBrokerException e) {
                                throw new MQClientException("unknown exception", e);
                            }
                        } catch (Exception e) {
                            sendCallback.onException(e);
                        }
                    } else {
                        sendCallback.onException(new RemotingTooMuchRequestException("call timeout"));
                    }
                }

            });
        } catch (RejectedExecutionException e) {
            throw new MQClientException("executor rejected ", e);
        }
    }

    /**
     * SELECT ONEWAY -------------------------------------------------------
     */
    public void sendOneway(Message msg, MessageQueueSelector selector, Object arg)
        throws MQClientException, RemotingException, InterruptedException {
        try {
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ONEWAY, null, this.defaultMQProducer.getSendMsgTimeout());
        } catch (MQBrokerException e) {
            throw new MQClientException("unknown exception", e);
        }
    }

    public TransactionSendResult sendMessageInTransaction(final Message msg,
        final LocalTransactionExecuter localTransactionExecuter, final Object arg)
        throws MQClientException {
        // 校验发送者的事务监听器
        TransactionListener transactionListener = getCheckListener();
        if (null == localTransactionExecuter && null == transactionListener) {
            throw new MQClientException("tranExecutor is null", null);
        }

        // ignore DelayTimeLevel parameter
        // 忽略延迟属性，事务消息不可延迟
        if (msg.getDelayTimeLevel() != 0) {
            MessageAccessor.clearProperty(msg, MessageConst.PROPERTY_DELAY_TIME_LEVEL);
        }

        // 消息基础校验
        Validators.checkMessage(msg, this.defaultMQProducer);

        // 设置事务属性
        SendResult sendResult = null;
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_PRODUCER_GROUP, this.defaultMQProducer.getProducerGroup());
        try {
            sendResult = this.send(msg);
        } catch (Exception e) {
            throw new MQClientException("send message Exception", e);
        }

        LocalTransactionState localTransactionState = LocalTransactionState.UNKNOW;
        Throwable localException = null;
        switch (sendResult.getSendStatus()) {
            case SEND_OK: {
                try {
                    // 发送半消息成功，
                    if (sendResult.getTransactionId() != null) {
                        msg.putUserProperty("__transactionId__", sendResult.getTransactionId());
                    }
                    String transactionId = msg.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
                    if (null != transactionId && !"".equals(transactionId)) {
                        msg.setTransactionId(transactionId);
                    }

                    // transactionListener的实现不为空，transactionListener执行本地事务，返回处理状态
                    if (null != localTransactionExecuter) {
                        localTransactionState = localTransactionExecuter.executeLocalTransactionBranch(msg, arg);
                    } else if (transactionListener != null) {
                        log.debug("Used new transaction API");
                        localTransactionState = transactionListener.executeLocalTransaction(msg, arg);
                    }
                    // status为null，标记为未知状态，等待broker进行半消息的事务回查
                    if (null == localTransactionState) {
                        localTransactionState = LocalTransactionState.UNKNOW;
                    }

                    // 若不等于提交消息的状态，日志记录一下
                    if (localTransactionState != LocalTransactionState.COMMIT_MESSAGE) {
                        log.info("executeLocalTransactionBranch return {}", localTransactionState);
                        log.info(msg.toString());
                    }
                } catch (Throwable e) {
                    log.info("executeLocalTransactionBranch exception", e);
                    log.info(msg.toString());
                    localException = e;
                }
            }
            break;
            case FLUSH_DISK_TIMEOUT:
            case FLUSH_SLAVE_TIMEOUT:
            case SLAVE_NOT_AVAILABLE:
                localTransactionState = LocalTransactionState.ROLLBACK_MESSAGE;
                break;
            default:
                break;
        }

        try {
            // 结束事务
            this.endTransaction(msg, sendResult, localTransactionState, localException);
        } catch (Exception e) {
            log.warn("local transaction execute " + localTransactionState + ", but end broker transaction failed", e);
        }

        // 返回事务发送结果
        TransactionSendResult transactionSendResult = new TransactionSendResult();
        transactionSendResult.setSendStatus(sendResult.getSendStatus());
        transactionSendResult.setMessageQueue(sendResult.getMessageQueue());
        transactionSendResult.setMsgId(sendResult.getMsgId());
        transactionSendResult.setQueueOffset(sendResult.getQueueOffset());
        transactionSendResult.setTransactionId(sendResult.getTransactionId());
        transactionSendResult.setLocalTransactionState(localTransactionState);
        return transactionSendResult;
    }

    /**
     * DEFAULT SYNC -------------------------------------------------------
     */
    public SendResult send(
        Message msg) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return send(msg, this.defaultMQProducer.getSendMsgTimeout());
    }

    public void endTransaction(
        final Message msg,
        final SendResult sendResult,
        final LocalTransactionState localTransactionState,
        final Throwable localException) throws RemotingException, MQBrokerException, InterruptedException, UnknownHostException {
        final MessageId id;
        if (sendResult.getOffsetMsgId() != null) {
            id = MessageDecoder.decodeMessageId(sendResult.getOffsetMsgId());
        } else {
            id = MessageDecoder.decodeMessageId(sendResult.getMsgId());
        }
        String transactionId = sendResult.getTransactionId();
        final String brokerAddr = this.mQClientFactory.findBrokerAddressInPublish(sendResult.getMessageQueue().getBrokerName());
        EndTransactionRequestHeader requestHeader = new EndTransactionRequestHeader();
        requestHeader.setTransactionId(transactionId);
        requestHeader.setCommitLogOffset(id.getOffset());
        switch (localTransactionState) {
            case COMMIT_MESSAGE:
                // 设置提交类型
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_COMMIT_TYPE);
                break;
            case ROLLBACK_MESSAGE:
                // 设置回滚类型
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_ROLLBACK_TYPE);
                break;
            case UNKNOW:
                // 设置未知类型
                requestHeader.setCommitOrRollback(MessageSysFlag.TRANSACTION_NOT_TYPE);
                break;
            default:
                break;
        }
        // 事务执行结束钩子函数
        doExecuteEndTransactionHook(msg, sendResult.getMsgId(), brokerAddr, localTransactionState, false);
        requestHeader.setProducerGroup(this.defaultMQProducer.getProducerGroup());
        requestHeader.setTranStateTableOffset(sendResult.getQueueOffset());
        requestHeader.setMsgId(sendResult.getMsgId());
        String remark = localException != null ? ("executeLocalTransactionBranch exception: " + localException.toString()) : null;
        this.mQClientFactory.getMQClientAPIImpl().endTransactionOneway(brokerAddr, requestHeader, remark,
            this.defaultMQProducer.getSendMsgTimeout());
    }

    public void setCallbackExecutor(final ExecutorService callbackExecutor) {
        this.mQClientFactory.getMQClientAPIImpl().getRemotingClient().setCallbackExecutor(callbackExecutor);
    }

    public ExecutorService getAsyncSenderExecutor() {
        return null == asyncSenderExecutor ? defaultAsyncSenderExecutor : asyncSenderExecutor;
    }

    public void setAsyncSenderExecutor(ExecutorService asyncSenderExecutor) {
        this.asyncSenderExecutor = asyncSenderExecutor;
    }

    public SendResult send(Message msg,
        long timeout) throws MQClientException, RemotingException, MQBrokerException, InterruptedException {
        return this.sendDefaultImpl(msg, CommunicationMode.SYNC, null, timeout);
    }

    public Message request(Message msg,
        long timeout) throws RequestTimeoutException, MQClientException, RemotingException, MQBrokerException, InterruptedException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(Message msg, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendDefaultImpl(msg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);
    }

    public Message request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final long timeout) throws MQClientException, RemotingException, MQBrokerException,
        InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    public void request(final Message msg, final MessageQueueSelector selector, final Object arg,
        final RequestCallback requestCallback, final long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendSelectImpl(msg, selector, arg, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, timeout - cost);

    }

    public Message request(final Message msg, final MessageQueue mq, final long timeout)
        throws MQClientException, RemotingException, MQBrokerException, InterruptedException, RequestTimeoutException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        try {
            final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, null);
            RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

            long cost = System.currentTimeMillis() - beginTimestamp;
            this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
                @Override
                public void onSuccess(SendResult sendResult) {
                    requestResponseFuture.setSendRequestOk(true);
                }

                @Override
                public void onException(Throwable e) {
                    requestResponseFuture.setSendRequestOk(false);
                    requestResponseFuture.putResponseMessage(null);
                    requestResponseFuture.setCause(e);
                }
            }, null, timeout - cost);

            return waitResponse(msg, timeout, requestResponseFuture, cost);
        } finally {
            RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        }
    }

    private Message waitResponse(Message msg, long timeout, RequestResponseFuture requestResponseFuture,
        long cost) throws InterruptedException, RequestTimeoutException, MQClientException {
        Message responseMessage = requestResponseFuture.waitResponseMessage(timeout - cost);
        if (responseMessage == null) {
            if (requestResponseFuture.isSendRequestOk()) {
                throw new RequestTimeoutException(ClientErrorCode.REQUEST_TIMEOUT_EXCEPTION,
                    "send request message to <" + msg.getTopic() + "> OK, but wait reply message timeout, " + timeout + " ms.");
            } else {
                throw new MQClientException("send request message to <" + msg.getTopic() + "> fail", requestResponseFuture.getCause());
            }
        }
        return responseMessage;
    }

    public void request(final Message msg, final MessageQueue mq, final RequestCallback requestCallback, long timeout)
        throws RemotingException, InterruptedException, MQClientException, MQBrokerException {
        long beginTimestamp = System.currentTimeMillis();
        prepareSendRequest(msg, timeout);
        final String correlationId = msg.getProperty(MessageConst.PROPERTY_CORRELATION_ID);

        final RequestResponseFuture requestResponseFuture = new RequestResponseFuture(correlationId, timeout, requestCallback);
        RequestFutureHolder.getInstance().getRequestFutureTable().put(correlationId, requestResponseFuture);

        long cost = System.currentTimeMillis() - beginTimestamp;
        this.sendKernelImpl(msg, mq, CommunicationMode.ASYNC, new SendCallback() {
            @Override
            public void onSuccess(SendResult sendResult) {
                requestResponseFuture.setSendRequestOk(true);
            }

            @Override
            public void onException(Throwable e) {
                requestResponseFuture.setCause(e);
                requestFail(correlationId);
            }
        }, null, timeout - cost);
    }

    private void requestFail(final String correlationId) {
        RequestResponseFuture responseFuture = RequestFutureHolder.getInstance().getRequestFutureTable().remove(correlationId);
        if (responseFuture != null) {
            responseFuture.setSendRequestOk(false);
            responseFuture.putResponseMessage(null);
            try {
                responseFuture.executeRequestCallback();
            } catch (Exception e) {
                log.warn("execute requestCallback in requestFail, and callback throw", e);
            }
        }
    }

    private void prepareSendRequest(final Message msg, long timeout) {
        String correlationId = CorrelationIdUtil.createCorrelationId();
        String requestClientId = this.getmQClientFactory().getClientId();
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_CORRELATION_ID, correlationId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_REPLY_TO_CLIENT, requestClientId);
        MessageAccessor.putProperty(msg, MessageConst.PROPERTY_MESSAGE_TTL, String.valueOf(timeout));

        boolean hasRouteData = this.getmQClientFactory().getTopicRouteTable().containsKey(msg.getTopic());
        if (!hasRouteData) {
            long beginTimestamp = System.currentTimeMillis();
            this.tryToFindTopicPublishInfo(msg.getTopic());
            this.getmQClientFactory().sendHeartbeatToAllBrokerWithLock();
            long cost = System.currentTimeMillis() - beginTimestamp;
            if (cost > 500) {
                log.warn("prepare send request for <{}> cost {} ms", msg.getTopic(), cost);
            }
        }
    }

    public ConcurrentMap<String, TopicPublishInfo> getTopicPublishInfoTable() {
        return topicPublishInfoTable;
    }

    public int getZipCompressLevel() {
        return zipCompressLevel;
    }

    public void setZipCompressLevel(int zipCompressLevel) {
        this.zipCompressLevel = zipCompressLevel;
    }

    public ServiceState getServiceState() {
        return serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        this.serviceState = serviceState;
    }

    public long[] getNotAvailableDuration() {
        return this.mqFaultStrategy.getNotAvailableDuration();
    }

    public void setNotAvailableDuration(final long[] notAvailableDuration) {
        this.mqFaultStrategy.setNotAvailableDuration(notAvailableDuration);
    }

    public long[] getLatencyMax() {
        return this.mqFaultStrategy.getLatencyMax();
    }

    public void setLatencyMax(final long[] latencyMax) {
        this.mqFaultStrategy.setLatencyMax(latencyMax);
    }

    public boolean isSendLatencyFaultEnable() {
        return this.mqFaultStrategy.isSendLatencyFaultEnable();
    }

    public void setSendLatencyFaultEnable(final boolean sendLatencyFaultEnable) {
        this.mqFaultStrategy.setSendLatencyFaultEnable(sendLatencyFaultEnable);
    }

    public DefaultMQProducer getDefaultMQProducer() {
        return defaultMQProducer;
    }
}
