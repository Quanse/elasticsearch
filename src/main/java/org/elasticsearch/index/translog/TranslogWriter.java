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

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.OutputStreamDataOutput;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Channels;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class TranslogWriter extends TranslogReader {

    public static final String TRANSLOG_CODEC = "translog";
    public static final int VERSION_CHECKSUMS = 1;
    public static final int VERSION_CHECKPOINTS = 2; // since 2.0 we have checkpoints?
    public static final int VERSION = VERSION_CHECKPOINTS;
    public static final String CHECKPOINT_FIEL_NAME = "translog.gen";

    private boolean enableCheckpoints = true;
    protected final ShardId shardId;
    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;
    /* the offset in bytes that was written when the file was last synced*/
    protected volatile long lastSyncedOffset;
    /* the number of translog operations written to this file */
    protected volatile int operationCounter;
    /* the offset in bytes written to the file */
    protected volatile long writtenOffset;

    public TranslogWriter(ShardId shardId, long id, ChannelReference channelReference, boolean writeCheckpoints) throws IOException {
        super(id, channelReference);
        this.shardId = shardId;
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC);
        this.writtenOffset = headerLength;
        this.lastSyncedOffset = headerLength;
        this.enableCheckpoints = writeCheckpoints;
    }

    public static TranslogWriter create(Type type, ShardId shardId, long id, Path file, Callback<ChannelReference> onClose, int bufferSize, boolean writeCheckpoints) throws IOException {
        Path pendingFile = file.resolveSibling("pending_" + file.getFileName());
        final int headerLength = CodecUtil.headerLength(TRANSLOG_CODEC);
        /**
         * We first create pending_translog, write the header, fsync it and write a checkpoint. Then we rename the pending file into
         * the actual file such that there is never a file without valid header. If the header is missing it's corrupted
         */
        try (FileChannel channel = FileChannel.open(pendingFile, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            // This OutputStreamDataOutput is intentionally not closed because
            // closing it will close the FileChannel
            OutputStreamDataOutput out = new OutputStreamDataOutput(java.nio.channels.Channels.newOutputStream(channel));
            CodecUtil.writeHeader(out, TRANSLOG_CODEC, VERSION);
            channel.force(false);
            if (writeCheckpoints) {
                writeCheckpoint(headerLength, 0, file.getParent(), id, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
            }
        }
        Files.move(pendingFile, file, StandardCopyOption.ATOMIC_MOVE);
        FileChannel channel = FileChannel.open(file, StandardOpenOption.READ, StandardOpenOption.WRITE);
        boolean success = false;
        try {
            channel.position(headerLength);
            final TranslogWriter writer = type.create(shardId, id, new ChannelReference(file, id, channel, onClose), bufferSize, writeCheckpoints);
            success = true;
            return writer;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(channel);
            }
        }
    }

    public enum Type {

        SIMPLE() {
            @Override
            public TranslogWriter create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize, boolean writeCheckpoints) throws IOException {
                return new TranslogWriter(shardId, id, channelReference, writeCheckpoints);
            }
        },
        BUFFERED() {
            @Override
            public TranslogWriter create(ShardId shardId, long id, ChannelReference channelReference, int bufferSize, boolean writeCheckpoints) throws IOException {
                return new BufferingTranslogWriter(shardId, id, channelReference, bufferSize, writeCheckpoints);
            }
        };

        public abstract TranslogWriter create(ShardId shardId, long id, ChannelReference raf, int bufferSize, boolean writeCheckpoints) throws IOException;

        public static Type fromString(String type) {
            if (SIMPLE.name().equalsIgnoreCase(type)) {
                return SIMPLE;
            } else if (BUFFERED.name().equalsIgnoreCase(type)) {
                return BUFFERED;
            }
            throw new IllegalArgumentException("No translog fs type [" + type + "]");
        }
    }


    /**
     * add the given bytes to the translog and return the location they were written at
     */
    public Translog.Location add(BytesReference data) throws IOException {
        ensureOpen();
        try (ReleasableLock lock = writeLock.acquire()) {
            long position = writtenOffset;
            data.writeTo(channel);
            writtenOffset = writtenOffset + data.length();
            operationCounter = operationCounter + 1;
            return new Translog.Location(id, position, data.length());
        }
    }

    /**
     * change the size of the internal buffer if relevant
     */
    public void updateBufferSize(int bufferSize) throws TranslogException {
    }

    /**
     * write all buffered ops to disk and fsync file
     */
    public void sync() throws IOException {
        // check if we really need to sync here...
        if (syncNeeded()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                lastSyncedOffset = writtenOffset;
                checkpoint(lastSyncedOffset, operationCounter);
            }
        }
    }

    /**
     * returns true if there are buffered ops
     */
    public boolean syncNeeded() {
        return writtenOffset != lastSyncedOffset; // by default nothing is buffered
    }

    @Override
    public int totalOperations() {
        return operationCounter;
    }

    @Override
    public long sizeInBytes() {
        return writtenOffset;
    }

    @Override
    public ChannelSnapshot newChannelSnapshot() {
        return new ChannelSnapshot(immutableReader());
    }

    /**
     * Flushes the buffer if the translog is buffered.
     */
    protected void flush() throws IOException {
    }

    /**
     * returns a new reader that follows the current writes (most importantly allows making
     * repeated snapshots that includes new content)
     */
    public TranslogReader reader() {
        ensureOpen();
        channelReference.incRef();
        boolean success = false;
        try {
            TranslogReader reader = new InnerReader(this.id, channelReference);
            success = true;
            return reader;
        } finally {
            if (!success) {
                channelReference.decRef();
            }
        }
    }


    /**
     * returns a new immutable reader which only exposes the current written operation *
     */
    public ImmutableTranslogReader immutableReader() throws TranslogException {
        ensureOpen();
        if (channelReference.tryIncRef()) {
            try (ReleasableLock lock = writeLock.acquire()) {
                flush();
                ImmutableTranslogReader reader = new ImmutableTranslogReader(this.id, channelReference, writtenOffset, operationCounter);
                channelReference.incRef(); // for new reader
                return reader;
            } catch (Exception e) {
                throw new TranslogException(shardId, "exception while creating an immutable reader", e);
            } finally {
                channelReference.decRef();
            }
        } else {
            throw new TranslogException(shardId, "can't increment channel [" + channelReference + "] ref count");
        }
    }

    boolean assertBytesAtLocation(Translog.Location location, BytesReference expectedBytes) throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(location.size);
        readBytes(buffer, location.translogLocation);
        return new BytesArray(buffer.array()).equals(expectedBytes);
    }

    /**
     * this class is used when one wants a reference to this file which exposes all recently written operation.
     * as such it needs access to the internals of the current reader
     */
    final class InnerReader extends TranslogReader {

        public InnerReader(long id, ChannelReference channelReference) {
            super(id, channelReference);
        }

        @Override
        public long sizeInBytes() {
            return TranslogWriter.this.sizeInBytes();
        }

        @Override
        public int totalOperations() {
            return TranslogWriter.this.totalOperations();
        }

        @Override
        protected void readBytes(ByteBuffer buffer, long position) throws IOException {
            TranslogWriter.this.readBytes(buffer, position);
        }

        @Override
        public ChannelSnapshot newChannelSnapshot() {
            return TranslogWriter.this.newChannelSnapshot();
        }
    }

    /**
     * Syncs the translog up to at least the given offset unless already synced
     *
     * @return <code>true</code> if this call caused an actual sync operation
     */
    public boolean syncUpTo(long offset) throws IOException {
        if (lastSyncedOffset < offset) {
            sync();
            return true;
        }
        return false;
    }

    @Override
    protected final void doClose() throws IOException {
        try (ReleasableLock lock = writeLock.acquire()) {
            // TODO should we write a footer?
            sync();
            Checkpoint checkpoint = new Checkpoint(lastSyncedOffset, operationCounter, this.translogId());
            checkpoint.write(channel);
            channel.force(false);
        } finally {
            super.doClose();
        }
    }

    @Override
    protected void readBytes(ByteBuffer buffer, long position) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            Channels.readFromFileChannelWithEofException(channel, position, buffer);
        }
    }

    protected synchronized void checkpoint(long lastSyncPosition, int operationCounter) throws IOException {
        channel.force(false);
        if (enableCheckpoints) {
            writeCheckpoint(lastSyncPosition, operationCounter, channelReference.getPath().getParent(), channelReference.getTranslogId(), StandardOpenOption.WRITE);
        }
    }

    private static void writeCheckpoint(long syncPosition, int numOperations, Path translogFile, long translogId, OpenOption... options) throws IOException {
        final Path checkpointFile = translogFile.resolve(CHECKPOINT_FIEL_NAME);
        try (FileChannel channel = FileChannel.open(checkpointFile, options)) {
            Checkpoint checkpoint = new Checkpoint(syncPosition, numOperations, translogId);
            checkpoint.write(channel);
            channel.force(false);
        }
    }

}
