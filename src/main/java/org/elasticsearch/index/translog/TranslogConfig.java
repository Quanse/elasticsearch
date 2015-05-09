/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.nio.file.Path;

/**
 *///nocommit - document this
public class TranslogConfig {
    public static final String INDEX_TRANSLOG_DURABILITY = "index.translog.durability";
    public static final String INDEX_TRANSLOG_FS_TYPE = "index.translog.fs.type";
    public static final String INDEX_TRANSLOG_BUFFER_SIZE = "index.translog.fs.buffer_size";
    public static final String INDEX_TRANSLOG_SYNC_INTERVAL = "index.translog.sync_interval";
    public static final ByteSizeValue INACTIVE_SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("1kb");

    private final TimeValue syncInterval;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;
    private volatile Translog.Durabilty durabilty = Translog.Durabilty.REQUEST;
    private volatile TranslogWriter.Type type;
    private final boolean syncOnEachOperation;
    private volatile int bufferSize;

    private final Settings indexSettings;
    private final ShardId shardId;
    private final Path translogPath;


    public TranslogConfig(Translog.Durabilty durabilty, BigArrays bigArrays, ThreadPool threadPool, @IndexSettings Settings indexSettings, ShardId shardId, Path translogPath) {
        this.indexSettings = indexSettings;
        this.shardId = shardId;
        this.translogPath = translogPath;
        this.durabilty = durabilty;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.type = TranslogWriter.Type.fromString(indexSettings.get(INDEX_TRANSLOG_FS_TYPE, TranslogWriter.Type.BUFFERED.name()));
        this.bufferSize = (int) indexSettings.getAsBytesSize(INDEX_TRANSLOG_BUFFER_SIZE, ByteSizeValue.parseBytesSizeValue("64k")).bytes(); // Not really interesting, updated by IndexingMemoryController...

        syncInterval = indexSettings.getAsTime(INDEX_TRANSLOG_SYNC_INTERVAL, TimeValue.timeValueSeconds(5));
        if (syncInterval.millis() > 0 && threadPool != null) {
            syncOnEachOperation = false;
        } else if (syncInterval.millis() == 0) {
            syncOnEachOperation = true;
        } else {
            syncOnEachOperation = false;
        }

    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    /**
     * Returns the current durability mode of this translog.
     */
    public Translog.Durabilty getDurabilty() {
        return durabilty;
    }

    public void setDurabilty(Translog.Durabilty durabilty) {
        this.durabilty = durabilty;
    }

    public TranslogWriter.Type getType() {
        return type;
    }

    public void setType(TranslogWriter.Type type) {
        this.type = type;
    }

    public boolean isSyncOnEachOperation() {
        return syncOnEachOperation;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public TimeValue getSyncInterval() {
        return syncInterval;
    }


    public Settings getIndexSettings() {
        return indexSettings;
    }

    public ShardId getShardId() {
        return shardId;
    }

    /**
     * Returns a BigArrays instance for this engine
     */
    public BigArrays getBigArrays() {
        return bigArrays;
    }

    /**
     * Returns the translog path for this engine
     */
    public Path getTranslogPath() {
        return translogPath;
    }

}
