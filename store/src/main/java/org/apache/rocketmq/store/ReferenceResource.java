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

import java.util.concurrent.atomic.AtomicLong;

public abstract class ReferenceResource {
    // refCount表示引用数量，当refCount小于等于0时，表示该资源可以释放，没有任何其他程序依赖它了。初始值为1
    protected final AtomicLong refCount = new AtomicLong(1);
    // true表示资源可用，false表示资源不可用
    protected volatile boolean available = true;
    // 是否已经清理，默认为false。当执行子类对象的cleanup()方法后，该值设置为true，表示资源已经全部释放了
    protected volatile boolean cleanupOver = false;
    // 第一次关闭资源的时间（因为第一次关闭资源可能会失败，比如外部程序还依赖当前资源，故refCount大于0，此时会记录初次关闭资源的时间，当以后再次关闭资源时会传递interval参数，如果系统当前时间 - firstShutdownTimestamp > interval，就执行强制关闭）
    private volatile long firstShutdownTimestamp = 0;

    /**
     * 增加引用计数
     */
    public synchronized boolean hold() {
        if (this.isAvailable()) {
            if (this.refCount.getAndIncrement() > 0) {
                return true;
            } else {
                // 数据越界的情况
                this.refCount.getAndDecrement();
            }
        }

        return false;
    }

    public boolean isAvailable() {
        return this.available;
    }

    public void shutdown(final long intervalForcibly) {
        if (this.available) {
            this.available = false;
            // 保存初次关闭时的时间
            this.firstShutdownTimestamp = System.currentTimeMillis();
            this.release();
        } else if (this.getRefCount() > 0) {
            if ((System.currentTimeMillis() - this.firstShutdownTimestamp) >= intervalForcibly) {
                // 强制设置引用计数为负数··/
                this.refCount.set(-1000 - this.getRefCount());
                this.release();
            }
        }
    }

    /**
     * 减少引用计数
     */
    public void release() {
        long value = this.refCount.decrementAndGet();
        if (value > 0)
            return;

        synchronized (this) {
            // 说明当前资源已经没有被外部程序依赖了，可以释放资源
            this.cleanupOver = this.cleanup(value);
        }
    }

    public long getRefCount() {
        return this.refCount.get();
    }

    public abstract boolean cleanup(final long currentRef);

    public boolean isCleanupOver() {
        return this.refCount.get() <= 0 && this.cleanupOver;
    }
}
