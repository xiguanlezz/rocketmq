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
package org.apache.rocketmq.store.ha;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import org.apache.rocketmq.common.ServiceThread;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.store.SelectMappedBufferResult;

public class HAConnection {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    // 上层对象
    private final HAService haService;
    // master与slave之间会话通信的socketChannel
    private final SocketChannel socketChannel;
    // 客户端地址
    private final String clientAddr;
    // 写数据服务
    private WriteSocketService writeSocketService;
    // 读数据服务
    private ReadSocketService readSocketService;

    // 在slave上报过本地的进度后被赋值，当slaveRequestOffset >= 0后，才会启动同步数据的逻辑
    // 为什么？因为master节点不知道slave节点当前消息存储进度在哪，没办法给slave推送数据
    private volatile long slaveRequestOffset = -1;
    // 保存最新的slave上报的offset信息，slaveAckOffset之前的数据都可以认为slave全部已经同步完成了，对应的“生产者线程”需要被唤醒！
    private volatile long slaveAckOffset = -1;

    public HAConnection(final HAService haService, final SocketChannel socketChannel) throws IOException {
        this.haService = haService;
        this.socketChannel = socketChannel;
        this.clientAddr = this.socketChannel.socket().getRemoteSocketAddress().toString();
        this.socketChannel.configureBlocking(false);
        this.socketChannel.socket().setSoLinger(false, -1);
        this.socketChannel.socket().setTcpNoDelay(true);
        // 设置socket读写缓冲区为64KB
        this.socketChannel.socket().setReceiveBufferSize(1024 * 64);
        this.socketChannel.socket().setSendBufferSize(1024 * 64);
        // 创建读写服务
        this.writeSocketService = new WriteSocketService(this.socketChannel);
        this.readSocketService = new ReadSocketService(this.socketChannel);
        // 数量自增
        this.haService.getConnectionCount().incrementAndGet();
    }

    public void start() {
        // 启动读数据服务
        this.readSocketService.start();
        // 启动写数据服务
        this.writeSocketService.start();
    }

    public void shutdown() {
        this.writeSocketService.shutdown(true);
        this.readSocketService.shutdown(true);
        this.close();
    }

    public void close() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }
        }
    }

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }

    /**
     * 处理slave上报数据到当前master节点的读服务，上报过来的格式为：[long][long][long][long]
     */
    class ReadSocketService extends ServiceThread {
        // 1MB
        private static final int READ_MAX_BUFFER_SIZE = 1024 * 1024;
        // 多路复用器
        private final Selector selector;
        // master与slave之间的会话，就是HAConnection对象中的channel
        private final SocketChannel socketChannel;
        // 读取缓冲区，1MB
        private final ByteBuffer byteBufferRead = ByteBuffer.allocate(READ_MAX_BUFFER_SIZE);
        // 缓冲区处理位点
        private int processPosition = 0;
        // 用于维护长连接的
        private volatile long lastReadTimestamp = System.currentTimeMillis();

        // slave向master传输的帧格式：
        // [long][long][long]
        // slave向master上报的是slave的同步进度

        public ReadSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // 将socketChannel注册到多路复用器，关注“OP_READ”事件
            this.socketChannel.register(this.selector, SelectionKey.OP_READ);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 多路复用器阻塞，最长1秒钟
                    this.selector.select(1000);
                    // 1. 读事件就绪；2. 超时
                    boolean ok = this.processReadEvent();
                    if (!ok) {
                        HAConnection.log.error("processReadEvent error");
                        break;
                    }

                    long interval = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastReadTimestamp;
                    if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaHousekeepingInterval()) {
                        // 长时间未发生通信，跳出循环，关闭HAConnection连接
                        log.warn("ha housekeeping, found this connection[" + HAConnection.this.clientAddr + "] expired, " + interval);
                        break;
                    }
                } catch (Exception e) {
                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            // 设置ServiceThread状态为stopped
            this.makeStop();
            // 将读服务对应的写服务的状态也设置为stopped
            writeSocketService.makeStop();
            // 从haService中移除当前HAConnection对象
            haService.removeConnection(HAConnection.this);
            // 计数减一
            HAConnection.this.haService.getConnectionCount().decrementAndGet();

            // 关闭socket
            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }
            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        @Override
        public String getServiceName() {
            return ReadSocketService.class.getSimpleName();
        }

        /**
         * 处理读事件
         * @return true表示正常，false表示socket处于半关闭状态，需要上层重建当前HAConnection对象
         */
        private boolean processReadEvent() {
            int readSizeZeroTimes = 0;

            if (!this.byteBufferRead.hasRemaining()) {
                // 走到这里，说明byteBufferRead中没有剩余可用空间了
                // 清理操作，将pos设置为0
                this.byteBufferRead.flip();
                // 清空位点信息
                this.processPosition = 0;
            }

            while (this.byteBufferRead.hasRemaining()) {
                try {
                    // 到socket缓冲区加载数据，readSize表示加载的数据大小
                    int readSize = this.socketChannel.read(this.byteBufferRead);
                    if (readSize > 0) {
                        // case1:加载成功
                        readSizeZeroTimes = 0;
                        this.lastReadTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                        if ((this.byteBufferRead.position() - this.processPosition) >= 8) {
                            // 走到这里，说明byteBufferRead中可读数据最少包含一个数据帧

                            // pos表示byteBufferRead可读帧中，最后一个帧数据（前面的帧可以不要，只要客户端上报的最后一个offset即可）
                            int pos = this.byteBufferRead.position() - (this.byteBufferRead.position() % 8);
                            // 读取最后一帧数据，slave端当前的同步进度信息
                            long readOffset = this.byteBufferRead.getLong(pos - 8);
                            // 更新处理位点
                            this.processPosition = pos;

                            HAConnection.this.slaveAckOffset = readOffset;
                            if (HAConnection.this.slaveRequestOffset < 0) {
                                // 走到这里，说明第一次给slaveRequestOffset赋值，slaveRequestOffset在写数据服务的时候会用到
                                HAConnection.this.slaveRequestOffset = readOffset;
                                log.info("slave[" + HAConnection.this.clientAddr + "] request offset " + readOffset);
                            }

                            // 唤醒阻塞的“生产者线程”
                            HAConnection.this.haService.notifyTransferSome(HAConnection.this.slaveAckOffset);
                        }
                    } else if (readSize == 0) {
                        // case2：加载失败，第缓冲区没有数据可加载
                        if (++readSizeZeroTimes >= 3) {
                            // 一般从这里跳出循环
                            break;
                        }
                    } else {
                        // case3：socket处于半关闭状态，需要上层关闭HAConnection对象
                        log.error("read socket[" + HAConnection.this.clientAddr + "] < 0");
                        return false;
                    }
                } catch (IOException e) {
                    log.error("processReadEvent exception", e);
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * 处理master将要同步的数据发给slave节点的写服务，上报过来的格式为：{[phyOffset][size][data...]}{[phyOffset][size][data...]}{[phyOffset][size][data...]}
     * phyOffset为8字节，size为4字节，这俩组成了协议头
     */
    class WriteSocketService extends ServiceThread {
        // 多路复用器
        private final Selector selector;
        // master与slave之间的会话，就是HAConnection对象中的channel
        private final SocketChannel socketChannel;

        // 协议头大小：12
        private final int headerSize = 8 + 4;
        // 帧头的缓冲区
        private final ByteBuffer byteBufferHeader = ByteBuffer.allocate(headerSize);

        // 下一次传输同步数据的位置信息（master需要知道此时给slave同步的位点）
        private long nextTransferFromWhere = -1;

        // mappedFile封装的数据对象
        private SelectMappedBufferResult selectMappedBufferResult;

        // 上一轮数据是否传输完毕
        private boolean lastWriteOver = true;
        // 维护长连接
        private long lastWriteTimestamp = System.currentTimeMillis();

        public WriteSocketService(final SocketChannel socketChannel) throws IOException {
            this.selector = RemotingUtil.openSelector();
            this.socketChannel = socketChannel;
            // 将socketChannel注册到多路复用器，关注“OP_WRITE”事件
            this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            this.setDaemon(true);
        }

        @Override
        public void run() {
            HAConnection.log.info(this.getServiceName() + " service started");

            while (!this.isStopped()) {
                try {
                    // 多路复用器阻塞，最长1秒钟
                    this.selector.select(1000);

                    // 1. 写缓冲区有空间可写；2. 超时

                    if (-1 == HAConnection.this.slaveRequestOffset) {
                        // 说明此时slave上报到master节点的数据还未处理，也可能是slave节点还未上报
                        Thread.sleep(10);
                        continue;
                    }

                    if (-1 == this.nextTransferFromWhere) {
                        // 初始化nextTransferFromWhere的逻辑
                        if (0 == HAConnection.this.slaveRequestOffset) {
                            // 如果slaveRequestOffset为0，那就从最后一个正在顺序写的commitLog文件开始同步

                            // 获取master最大的offset
                            long masterOffset = HAConnection.this.haService.getDefaultMessageStore().getCommitLog().getMaxOffset();
                            // 计算当前offset所属的commitLog文件的起始offset
                            masterOffset =
                                masterOffset
                                    - (masterOffset % HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                                    .getMappedFileSizeCommitLog());

                            if (masterOffset < 0) {
                                masterOffset = 0;
                            }

                            this.nextTransferFromWhere = masterOffset;
                        } else {
                            // 一般都是从slave节点上报给master同步进度的位置开始同步
                            this.nextTransferFromWhere = HAConnection.this.slaveRequestOffset;
                        }

                        log.info("master transfer data from " + this.nextTransferFromWhere + " to slave[" + HAConnection.this.clientAddr
                            + "], and slave request " + HAConnection.this.slaveRequestOffset);
                    }

                    if (this.lastWriteOver) {
                        // 走到这里，说明上一轮的待发送数据全部发送完毕
                        long interval =
                            HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now() - this.lastWriteTimestamp;

                        if (interval > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig()
                            .getHaSendHeartbeatInterval()) {

                            // 构建一个header，当作心跳包（size为0），用于维持长连接
                            // Build Header
                            this.byteBufferHeader.position(0);
                            this.byteBufferHeader.limit(headerSize);
                            this.byteBufferHeader.putLong(this.nextTransferFromWhere);
                            this.byteBufferHeader.putInt(0);
                            this.byteBufferHeader.flip();

                            this.lastWriteOver = this.transferData();
                            if (!this.lastWriteOver)
                                continue;
                        }
                    } else {
                        // 走到这里，说明上一轮的待发送数据未全部发送完毕
                        this.lastWriteOver = this.transferData();
                        if (!this.lastWriteOver)
                            continue;
                    }

                    // 到commitLog中查询从nextTransferFromWhere开始的数据
                    SelectMappedBufferResult selectResult =
                        HAConnection.this.haService.getDefaultMessageStore().getCommitLogData(this.nextTransferFromWhere);
                    if (selectResult != null) {
                        int size = selectResult.getSize();
                        if (size > HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize()) {
                            // 将size设置为32KB
                            size = HAConnection.this.haService.getDefaultMessageStore().getMessageStoreConfig().getHaTransferBatchSize();
                        }

                        long thisOffset = this.nextTransferFromWhere;
                        // nextTransferFromWhere增加size
                        this.nextTransferFromWhere += size;
                        // 设置byteBuffer可访问数据区间为：[pos, size]
                        selectResult.getByteBuffer().limit(size);
                        this.selectMappedBufferResult = selectResult;

                        // Build Header
                        this.byteBufferHeader.position(0);
                        this.byteBufferHeader.limit(headerSize);
                        this.byteBufferHeader.putLong(thisOffset);
                        this.byteBufferHeader.putInt(size);
                        this.byteBufferHeader.flip();

                        this.lastWriteOver = this.transferData();
                    } else {

                        HAConnection.this.haService.getWaitNotifyObject().allWaitForRunning(100);
                    }
                } catch (Exception e) {

                    HAConnection.log.error(this.getServiceName() + " service has exception.", e);
                    break;
                }
            }

            HAConnection.this.haService.getWaitNotifyObject().removeFromWaitingThreadTable();

            if (this.selectMappedBufferResult != null) {
                this.selectMappedBufferResult.release();
            }

            this.makeStop();

            readSocketService.makeStop();

            haService.removeConnection(HAConnection.this);

            SelectionKey sk = this.socketChannel.keyFor(this.selector);
            if (sk != null) {
                sk.cancel();
            }

            try {
                this.selector.close();
                this.socketChannel.close();
            } catch (IOException e) {
                HAConnection.log.error("", e);
            }

            HAConnection.log.info(this.getServiceName() + " service end");
        }

        private boolean transferData() throws Exception {
            int writeSizeZeroTimes = 0;
            // Write Header
            while (this.byteBufferHeader.hasRemaining()) {
                // 往socket写缓冲区写入协议头数据，writeSize为写成功的数据量
                int writeSize = this.socketChannel.write(this.byteBufferHeader);
                if (writeSize > 0) {
                    // case1：写成功
                    writeSizeZeroTimes = 0;
                    this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                } else if (writeSize == 0) {
                    // case2：写失败
                    if (++writeSizeZeroTimes >= 3) {
                        break;
                    }
                } else {
                    throw new Exception("ha master write header error < 0");
                }
            }

            // selectMappedBufferResult保存的是本轮master节点要同步到slave节点的数据
            if (null == this.selectMappedBufferResult) {
                // 如果是心跳包，会走到这里，返回值就是心跳包有没有全部发送完成
                return !this.byteBufferHeader.hasRemaining();
            }

            writeSizeZeroTimes = 0;

            // Write Body
            if (!this.byteBufferHeader.hasRemaining()) {
                // 只有header全部发完了，才能发送body
                while (this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                    // 往socket写缓冲区写入同步数据，writeSize为写成功的数据量
                    int writeSize = this.socketChannel.write(this.selectMappedBufferResult.getByteBuffer());
                    if (writeSize > 0) {
                        // case1：谢成功，不代表selectMappedBufferResult中数据玩不写完了
                        writeSizeZeroTimes = 0;
                        this.lastWriteTimestamp = HAConnection.this.haService.getDefaultMessageStore().getSystemClock().now();
                    } else if (writeSize == 0) {
                        // case2：写失败，因为socket写缓冲区写满了
                        if (++writeSizeZeroTimes >= 3) {
                            break;
                        }
                    } else {
                        throw new Exception("ha master write body error < 0");
                    }
                }
            }

            // result为true表示本轮数据同步完成（header + smbr）；result为false表示本轮同步未完成（header或smbr其中一个未同步完成都会返回false）
            boolean result = !this.byteBufferHeader.hasRemaining() && !this.selectMappedBufferResult.getByteBuffer().hasRemaining();

            if (!this.selectMappedBufferResult.getByteBuffer().hasRemaining()) {
                // 释放
                this.selectMappedBufferResult.release();
                this.selectMappedBufferResult = null;
            }

            return result;
        }

        @Override
        public String getServiceName() {
            return WriteSocketService.class.getSimpleName();
        }

        @Override
        public void shutdown() {
            super.shutdown();
        }
    }
}
