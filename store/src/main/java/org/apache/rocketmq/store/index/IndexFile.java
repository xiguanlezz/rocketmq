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
package org.apache.rocketmq.store.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.List;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.store.MappedFile;

public class IndexFile {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);
    // 每个hash桶的大小：4byte
    private static int hashSlotSize = 4;
    // 每个index条目的大小：20byte
    private static int indexSize = 20;
    // 无效索引编号：0 特殊值
    private static int invalidIndex = 0;

    // hash桶数量的上限，默认值为500W
    private final int hashSlotNum;
    // 存储条目数即indexData数量的上限，默认值为2000W
    private final int indexNum;

    // 索引文件使用的mf
    private final MappedFile mappedFile;
    private final FileChannel fileChannel;

    // 从mf中获取的内存映射缓冲区
    private final MappedByteBuffer mappedByteBuffer;
    // 索引头对象
    private final IndexHeader indexHeader;

    /**
     * @param fileName
     * @param hashSlotNum
     * @param indexNum
     * @param endPhyOffset 上个索引文件最后一条消息的索引偏移量
     * @param endTimestamp 上个索引文件最后一条消息的存储时间
     * @throws IOException
     */
    public IndexFile(final String fileName, final int hashSlotNum, final int indexNum,
        final long endPhyOffset, final long endTimestamp) throws IOException {
        // 文件大小：40 + 500W * 4 + 2000W * 20
        int fileTotalSize =
            IndexHeader.INDEX_HEADER_SIZE + (hashSlotNum * hashSlotSize) + (indexNum * indexSize);
        // 创建mf，会在disk上创建文件
        this.mappedFile = new MappedFile(fileName, fileTotalSize);
        this.fileChannel = this.mappedFile.getFileChannel();
        this.mappedByteBuffer = this.mappedFile.getMappedByteBuffer();
        this.hashSlotNum = hashSlotNum;
        this.indexNum = indexNum;

        ByteBuffer byteBuffer = this.mappedByteBuffer.slice();
        // 根据切片创建索引头对象
        this.indexHeader = new IndexHeader(byteBuffer);

        if (endPhyOffset > 0) {
            this.indexHeader.setBeginPhyOffset(endPhyOffset);
            this.indexHeader.setEndPhyOffset(endPhyOffset);
        }

        if (endTimestamp > 0) {
            this.indexHeader.setBeginTimestamp(endTimestamp);
            this.indexHeader.setEndTimestamp(endTimestamp);
        }
    }

    public String getFileName() {
        return this.mappedFile.getFileName();
    }

    public void load() {
        this.indexHeader.load();
    }

    public void flush() {
        long beginTime = System.currentTimeMillis();
        // 引用计数加一，防止被回收
        if (this.mappedFile.hold()) {
            // 更新indexHeader头部的字段
            this.indexHeader.updateByteBuffer();
            // 强迫索引文件落盘
            this.mappedByteBuffer.force();
            // 引用计数减一
            this.mappedFile.release();
            log.info("flush index file elapsed time(ms) " + (System.currentTimeMillis() - beginTime));
        }
    }

    public boolean isWriteFull() {
        return this.indexHeader.getIndexCount() >= this.indexNum;
    }

    public boolean destroy(final long intervalForcibly) {
        // 直接调用mappedFile的destroy方法
        return this.mappedFile.destroy(intervalForcibly);
    }

    /**
     * @param key msg：1. UNIQ_KEY；2. keys="aaa bbb ccc"会分别为aaa、bbb、ccc创建索引
     * @param phyOffset 消息物理偏移量
     * @param storeTimestamp 消息存储时间
     */
    public boolean putKey(final String key, final long phyOffset, final long storeTimestamp) {
        if (this.indexHeader.getIndexCount() < this.indexNum) {
            // 获取key的hash值，确保hash值大于等于0
            int keyHash = indexKeyHashMethod(key);

            // 取模，获取key对应的hash桶下标
            int slotPos = keyHash % this.hashSlotNum;
            // 根据slotPos计算出keyHash桶的开始位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;

            try {

                // fileLock = this.fileChannel.lock(absSlotPos, hashSlotSize,
                // false);
                // 读取hash桶内的原值（当发生hash冲突时才有值，其他情况slotValue时invalidIndex即0）
                // slotValue就像是IndexData的编号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()) {
                    slotValue = invalidIndex;
                }

                // 当前msg存储时间 - 索引文件内第一条消息的存储时间，得到一个差值。
                // 差值只需要4byte存储就够了，直接存storeTimeStamp需要8byte
                long timeDiff = storeTimestamp - this.indexHeader.getBeginTimestamp();
                // 转换成秒
                timeDiff = timeDiff / 1000;

                // 第一条索引插入时，timeDiff为0
                if (this.indexHeader.getBeginTimestamp() <= 0) {
                    timeDiff = 0;
                } else if (timeDiff > Integer.MAX_VALUE) {
                    timeDiff = Integer.MAX_VALUE;
                } else if (timeDiff < 0) {
                    timeDiff = 0;
                }

                // 计算索引条目即IndexData写入的开始位置：40 + 500W * 4 + 索引编号 * 20
                int absIndexPos =
                    IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                        + this.indexHeader.getIndexCount() * indexSize;

                // key hashCode
                this.mappedByteBuffer.putInt(absIndexPos, keyHash);
                // 消息偏移量
                this.mappedByteBuffer.putLong(absIndexPos + 4, phyOffset);
                // 消息存储时间和第一条索引存储时间的差值
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8, (int) timeDiff);
                // hash桶的原值（hash冲突时会用到）
                this.mappedByteBuffer.putInt(absIndexPos + 4 + 8 + 4, slotValue);
                // 向当前key计算出来的hash桶内写入新创建的IndexData的索引编号
                this.mappedByteBuffer.putInt(absSlotPos, this.indexHeader.getIndexCount());

                if (this.indexHeader.getIndexCount() <= 1) {
                    // 如果当前时索引文件中插入的第一天数据，需要记录一些数据
                    this.indexHeader.setBeginPhyOffset(phyOffset);
                    this.indexHeader.setBeginTimestamp(storeTimestamp);
                }

                if (invalidIndex == slotValue) {
                    // hash桶原值为invalidIndex时，此时需要占用一个hash桶
                    this.indexHeader.incHashSlotCount();
                }
                // 索引条目自增
                this.indexHeader.incIndexCount();
                this.indexHeader.setEndPhyOffset(phyOffset);
                this.indexHeader.setEndTimestamp(storeTimestamp);

                return true;
            } catch (Exception e) {
                log.error("putKey exception, Key: " + key + " KeyHashCode: " + key.hashCode(), e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }
            }
        } else {
            log.warn("Over index file capacity: index count = " + this.indexHeader.getIndexCount()
                + "; index max num = " + this.indexNum);
        }

        return false;
    }

    public int indexKeyHashMethod(final String key) {
        int keyHash = key.hashCode();
        int keyHashPositive = Math.abs(keyHash);
        if (keyHashPositive < 0)
            keyHashPositive = 0;
        return keyHashPositive;
    }

    public long getBeginTimestamp() {
        return this.indexHeader.getBeginTimestamp();
    }

    public long getEndTimestamp() {
        return this.indexHeader.getEndTimestamp();
    }

    public long getEndPhyOffset() {
        return this.indexHeader.getEndPhyOffset();
    }

    public boolean isTimeMatched(final long begin, final long end) {
        boolean result = begin < this.indexHeader.getBeginTimestamp() && end > this.indexHeader.getEndTimestamp();
        result = result || (begin >= this.indexHeader.getBeginTimestamp() && begin <= this.indexHeader.getEndTimestamp());
        result = result || (end >= this.indexHeader.getBeginTimestamp() && end <= this.indexHeader.getEndTimestamp());
        return result;
    }

    /**
     * 根据索引查询消息
     * @param phyOffsets 用于存放查询结果
     * @param key 查询key
     * @param maxNum 结果最大数限制
     * @param begin 消息存储的开始时间
     * @param end 消息存储的结束时间
     * @param lock
     */
    public void selectPhyOffset(final List<Long> phyOffsets, final String key, final int maxNum,
        final long begin, final long end, boolean lock) {
        // 引用计数加一
        if (this.mappedFile.hold()) {
            int keyHash = indexKeyHashMethod(key);
            int slotPos = keyHash % this.hashSlotNum;
            // 根据slotPos计算出keyHash桶的开始位置
            int absSlotPos = IndexHeader.INDEX_HEADER_SIZE + slotPos * hashSlotSize;

            FileLock fileLock = null;
            try {
                if (lock) {
                    // fileLock = this.fileChannel.lock(absSlotPos,
                    // hashSlotSize, true);
                }
                // 获取hash桶内的值，可能是无效值也可能是索引编号
                int slotValue = this.mappedByteBuffer.getInt(absSlotPos);
                // if (fileLock != null) {
                // fileLock.release();
                // fileLock = null;
                // }

                if (slotValue <= invalidIndex || slotValue > this.indexHeader.getIndexCount()
                    || this.indexHeader.getIndexCount() <= 1) {
                    // 此时说明查询未命中
                } else {
                    // nextIndexToRead代表下一条要读取的索引编号
                    for (int nextIndexToRead = slotValue; ; ) {
                        if (phyOffsets.size() >= maxNum) {
                            break;
                        }
                        // 计算出索引编号对应索引数据的开始位置
                        int absIndexPos =
                            IndexHeader.INDEX_HEADER_SIZE + this.hashSlotNum * hashSlotSize
                                + nextIndexToRead * indexSize;

                        // 读取索引数据
                        // 读取消息key的hashCode
                        int keyHashRead = this.mappedByteBuffer.getInt(absIndexPos);
                        // 读取消息在commitLog文件中的物理偏移量
                        long phyOffsetRead = this.mappedByteBuffer.getLong(absIndexPos + 4);
                        long timeDiff = (long) this.mappedByteBuffer.getInt(absIndexPos + 4 + 8);
                        int prevIndexRead = this.mappedByteBuffer.getInt(absIndexPos + 4 + 8 + 4);

                        if (timeDiff < 0) {
                            break;
                        }
                        // 转化为毫秒
                        timeDiff *= 1000L;

                        // 计算出msg准确的存储时间
                        long timeRead = this.indexHeader.getBeginTimestamp() + timeDiff;
                        boolean timeMatched = (timeRead >= begin) && (timeRead <= end);

                        if (keyHash == keyHashRead && timeMatched) {
                            // 此时说明查询命中，将消息索引的消息偏移量加入到list集合中
                            // 何为查询命中？1. 时间戳在查询范围内；2. 消息key的hashCode与读取出来的IndexData的hashCode一致
                            phyOffsets.add(phyOffsetRead);
                        }

                        // 判断索引条目即IndexData的前驱索引编号是否是无效的，无效的直接跳出查询逻辑
                        if (prevIndexRead <= invalidIndex
                            || prevIndexRead > this.indexHeader.getIndexCount()
                            || prevIndexRead == nextIndexToRead || timeRead < begin) {
                            break;
                        }

                        // 继续查询，解决哈希冲突
                        nextIndexToRead = prevIndexRead;
                    }
                }
            } catch (Exception e) {
                log.error("selectPhyOffset exception ", e);
            } finally {
                if (fileLock != null) {
                    try {
                        fileLock.release();
                    } catch (IOException e) {
                        log.error("Failed to release the lock", e);
                    }
                }

                // 引用计数加一
                this.mappedFile.release();
            }
        }
    }
}
