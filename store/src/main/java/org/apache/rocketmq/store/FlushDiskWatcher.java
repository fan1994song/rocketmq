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


import java.util.concurrent.LinkedBlockingQueue;

import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.CommitLog.GroupCommitRequest;

public class FlushDiskWatcher extends ServiceThread {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    private final LinkedBlockingQueue<GroupCommitRequest> commitRequests = new LinkedBlockingQueue<>();

    @Override
    public String getServiceName() {
        return FlushDiskWatcher.class.getSimpleName();
    }

    @Override
    public void run() {
        // 刷盘监听线程
        while (!isStopped()) {
            GroupCommitRequest request = null;
            try {
                request = commitRequests.take();
            } catch (InterruptedException e) {
                log.warn("take flush disk commit request, but interrupted, this may caused by shutdown");
                continue;
            }
            while (!request.future().isDone()) {
                long now = System.nanoTime();
                // 若超过刷盘时间，设置刷盘结果为刷盘超时
                if (now - request.getDeadLine() >= 0) {
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                // 若死亡时间不到一秒钟，返回刷盘超时
                // To avoid frequent thread switching, replace future.get with sleep here,
                long sleepTime = (request.getDeadLine() - now) / 1_000_000;
                sleepTime = Math.min(10, sleepTime);
                if (sleepTime == 0) {
                    request.wakeupCustomer(PutMessageStatus.FLUSH_DISK_TIMEOUT);
                    break;
                }
                try {
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    log.warn(
                            "An exception occurred while waiting for flushing disk to complete. this may caused by shutdown");
                    break;
                }
            }
        }
    }

    public void add(GroupCommitRequest request) {
        commitRequests.add(request);
    }

    public int queueSize() {
        return commitRequests.size();
    }
}
