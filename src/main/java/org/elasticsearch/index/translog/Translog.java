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

import org.apache.lucene.index.Term;
import org.apache.lucene.index.TwoPhaseCommit;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasablePagedBytesReference;
import org.elasticsearch.common.io.stream.*;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Callback;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 */
public class Translog extends AbstractIndexShardComponent implements IndexShardComponent, Closeable, TwoPhaseCommit {

    public static ByteSizeValue INACTIVE_SHARD_TRANSLOG_BUFFER = ByteSizeValue.parseBytesSizeValue("1kb");
    public static final String TRANSLOG_ID_KEY = "translog_id";
    public static final String INDEX_TRANSLOG_DURABILITY = "index.translog.durability";
    public static final String INDEX_TRANSLOG_FS_TYPE = "index.translog.fs.type";
    public static final String INDEX_TRANSLOG_BUFFER_SIZE = "index.translog.fs.buffer_size";
    public static final String INDEX_TRANSLOG_SYNC_INTERVAL = "index.translog.sync_interval";
    public static final String TRANSLOG_FILE_PREFIX = "translog-";
    public static final String TRANSLOG_FILE_SUFFIX = ".tlog";
    static final Pattern PARSE_ID_PATTERN = Pattern.compile(TRANSLOG_FILE_PREFIX + "(\\d+)((\\.recovering)|(\\.tlog))?$");
    private final TimeValue syncInterval;
    private final ArrayList<ImmutableTranslogReader> recoveredTranslogs;
    private volatile ScheduledFuture<?> syncScheduler;
    private volatile Durabilty durabilty = Durabilty.REQUEST;


    // this is a concurrent set and is not protected by any of the locks. The main reason
    // is that is being accessed by two separate classes (additions & reading are done by FsTranslog, remove by FsView when closed)
    private final Set<FsView> outstandingViews = ConcurrentCollections.newConcurrentSet();


    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            TranslogWriter.Type type = TranslogWriter.Type.fromString(settings.get(INDEX_TRANSLOG_FS_TYPE, Translog.this.type.name()));
            if (type != Translog.this.type) {
                logger.info("updating type from [{}] to [{}]", Translog.this.type, type);
                Translog.this.type = type;
            }

            final Durabilty durabilty = Durabilty.getFromSettings(logger, settings, Translog.this.durabilty);
            if (durabilty != Translog.this.durabilty) {
                logger.info("updating durability from [{}] to [{}]", Translog.this.durabilty, durabilty);
                Translog.this.durabilty = durabilty;
            }
        }
    }

    private final IndexSettingsService indexSettingsService;
    private final BigArrays bigArrays;
    private final ThreadPool threadPool;

    protected final ReleasableLock readLock;
    protected final ReleasableLock writeLock;

    private final Path location;

    private TranslogWriter current;
    // ordered by age
    private volatile ImmutableTranslogReader currentCommittingTranslog;
    private long lastCommittedTranslogId = -1; // -1 is safe as it will not cause an translog deletion.

    private TranslogWriter.Type type;

    private boolean syncOnEachOperation = false;

    private volatile int bufferSize;

    private final ApplySettings applySettings = new ApplySettings();

    private final AtomicBoolean closed = new AtomicBoolean();

    public Translog(ShardId shardId, IndexSettingsService indexSettingsService,
                    BigArrays bigArrays, Path location, ThreadPool threadPool) throws IOException {
        this(shardId, indexSettingsService.getSettings(), indexSettingsService, bigArrays, location, threadPool, OpenMode.RECOVER);
    }


    public Translog(ShardId shardId, IndexSettingsService indexSettingsService,
                    BigArrays bigArrays, Path location, ThreadPool threadPool, OpenMode mode) throws IOException {
        this(shardId, indexSettingsService.getSettings(), indexSettingsService, bigArrays, location, threadPool, mode);
    }

    public Translog(ShardId shardId, @IndexSettings Settings indexSettings,
                    BigArrays bigArrays, Path location) throws IOException {
        this(shardId, indexSettings, null, bigArrays, location, null, OpenMode.RECOVER);
    }

    private Translog(ShardId shardId, @IndexSettings Settings indexSettings, @Nullable IndexSettingsService indexSettingsService,
                     BigArrays bigArrays, Path location, @Nullable ThreadPool threadPool, OpenMode mode) throws IOException {
        super(shardId, indexSettings);
        ReadWriteLock rwl = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(rwl.readLock());
        writeLock = new ReleasableLock(rwl.writeLock());
        this.durabilty = Durabilty.getFromSettings(logger, indexSettings, durabilty);
        this.indexSettingsService = indexSettingsService;
        this.bigArrays = bigArrays;
        this.location = location;
        Files.createDirectories(this.location);
        this.threadPool = threadPool;

        this.type = TranslogWriter.Type.fromString(indexSettings.get(INDEX_TRANSLOG_FS_TYPE, TranslogWriter.Type.BUFFERED.name()));
        this.bufferSize = (int) indexSettings.getAsBytesSize(INDEX_TRANSLOG_BUFFER_SIZE, ByteSizeValue.parseBytesSizeValue("64k")).bytes(); // Not really interesting, updated by IndexingMemoryController...

        syncInterval = indexSettings.getAsTime(INDEX_TRANSLOG_SYNC_INTERVAL, TimeValue.timeValueSeconds(5));
        if (syncInterval.millis() > 0 && threadPool != null) {
            this.syncOnEachOperation = false;
            syncScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, new Sync());
        } else if (syncInterval.millis() == 0) {
            this.syncOnEachOperation = true;
        }

        if (indexSettingsService != null) {
            indexSettingsService.addListener(applySettings);
        }
        this.recoveredTranslogs = new ArrayList<>();
        try {
            switch (mode) {
                case RECOVER:
                    long recoveredNextTranslogId = recoverFromFiles(recoveredTranslogs);
                    current = createWriter(Math.max(1, recoveredNextTranslogId+1), recoveredNextTranslogId == -1);
                    break;
                case CREATE:
                    IOUtils.rm(location);
                    Files.createDirectories(location);
                    current = createWriter(1, true);
                    break;
                case OPEN:
                    final Checkpoint checkpoint = TranslogReader.openCheckpoint(location);
                    recoveredTranslogs.add(openReader(location.resolve(getFilename(checkpoint.translogId)), false));
                    lastCommittedTranslogId = -1; // playing safe
                    current = createWriter(checkpoint.translogId+1, false);
            }
            // now that we know which files are there, create a new current one.
        } catch (Throwable t) {
            // close the opened translog files if we fail to create a new translog...
            IOUtils.closeWhileHandlingException(currentCommittingTranslog, current);
            throw t;
        }
    }

    /** recover all translog files found on disk */
    private long recoverFromFiles(ArrayList<ImmutableTranslogReader> recoveredTranslogs) throws IOException {
        long recoveredNextTranslogId = -1;
        boolean success = false;
        ArrayList<ImmutableTranslogReader> foundTranslogs = new ArrayList<>();
        try (ReleasableLock lock = writeLock.acquire()) {
            String checkpointTranslogFile = "";
            try {
                Checkpoint checkpoint = TranslogReader.openCheckpoint(location);
                checkpointTranslogFile = getFilename(checkpoint.translogId);
                foundTranslogs.add(openReader(location.resolve(checkpointTranslogFile), false));
            } catch (NoSuchFileException | FileNotFoundException ex) {
                logger.warn("Recovering translog but no checkpoint found");
            }
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(location, TRANSLOG_FILE_PREFIX + "[0-9]*")) {
                for (Path file : stream) {
                    if (checkpointTranslogFile.equals(file.getFileName().toString()) == false) {
                        final ImmutableTranslogReader reader = openReader(file, true);
                        foundTranslogs.add(reader);
                        logger.debug("found local translog with id [{}]", reader.id);
                    }
                }
            }
            CollectionUtil.timSort(foundTranslogs);
            recoveredTranslogs.addAll(foundTranslogs);
            success = true;
            return recoveredTranslogs.isEmpty() ? -1 : recoveredTranslogs.get(recoveredTranslogs.size()-1).translogId();
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(foundTranslogs);
            }
        }
    }

    ImmutableTranslogReader openReader(Path path, boolean committed) throws IOException {
        final long id = parseIdFromFileName(path);
        if (id < 0) {
            throw new TranslogException(shardId, "failed to parse id from file name matching pattern " + path);
        }
        FileChannel channel = FileChannel.open(path, StandardOpenOption.READ);
        try {
            final ChannelReference raf = new ChannelReference(path, id, channel, new OnCloseRunnable());
            ImmutableTranslogReader reader = ImmutableTranslogReader.open(raf, committed);
            channel = null;
            return reader;
        } finally {
            IOUtils.close(channel);
        }
    }

    /* extracts the translog id from a file name. returns -1 upon failure */
    public static long parseIdFromFileName(Path translogFile) {
        final String fileName = translogFile.getFileName().toString();
        final Matcher matcher = PARSE_ID_PATTERN.matcher(fileName);
        if (matcher.matches()) {
            try {
                return Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new ElasticsearchException("number formatting issue in a file that passed PARSE_ID_PATTERN: " + fileName + "]", e);
            }
        }
        return -1;
    }

    public void updateBuffer(ByteSizeValue bufferSize) {
        this.bufferSize = bufferSize.bytesAsInt();
        try (ReleasableLock lock = writeLock.acquire()) {
            current.updateBufferSize(this.bufferSize);
        }
    }

    boolean isOpen() {
        return closed.get() == false;
    }

    @Override
    public void close() throws IOException {
        if (closed.compareAndSet(false, true)) {
            if (indexSettingsService != null) {
                indexSettingsService.removeListener(applySettings);
            }

            try (ReleasableLock lock = writeLock.acquire()) {
                try {
                    IOUtils.close(current, currentCommittingTranslog);
                } finally {
                    IOUtils.close(recoveredTranslogs);
                    recoveredTranslogs.clear();
                }
            } finally {
                FutureUtils.cancel(syncScheduler);
                logger.debug("translog closed");
            }
        }
    }

    /**
     * Returns all translog locations as absolute paths.
     * These paths don't contain actual translog files they are
     * directories holding the transaction logs.
     */
    public Path location() {
        return location;
    }

    /**
     * Returns the id of the current transaction log.
     */
    public long currentId() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.translogId();
        }
    }

    /**
     * Returns the number of operations in the transaction files that aren't committed to lucene..
     * Note: may return -1 if unknown
     */
    public int totalOperations() {
        int ops = 0;
        try (ReleasableLock lock = readLock.acquire()) {
            ops += current.totalOperations();
            if (currentCommittingTranslog != null) {
                int tops = currentCommittingTranslog.totalOperations();
                assert tops != TranslogReader.UNKNOWN_OP_COUNT;
                ops += tops;
            }
        }
        return ops;
    }

    /**
     * Returns the size in bytes of the translog files that aren't committed to lucene.
     */
    public long sizeInBytes() {
        long size = 0;
        try (ReleasableLock lock = readLock.acquire()) {
            size += current.sizeInBytes();
            if (currentCommittingTranslog != null) {
                size += currentCommittingTranslog.sizeInBytes();
            }
        }
        return size;
    }



    TranslogWriter createWriter(long id, boolean writeCheckpoints) throws IOException {
        TranslogWriter newFile;
        try {
            newFile = TranslogWriter.create(type, shardId, id, location.resolve(getFilename(id)), new OnCloseRunnable(), bufferSize, writeCheckpoints);
        } catch (IOException e) {
            throw new TranslogException(shardId, "failed to create new translog file", e);
        }
        return newFile;
    }


    /**
     * Read the Operation object from the given location.
     */
    public Translog.Operation read(Location location) {


        try (ReleasableLock lock = readLock.acquire()) {
            final TranslogReader reader;
            if (current.translogId() == location.translogId) {
                reader = current;
            } else if (currentCommittingTranslog != null && currentCommittingTranslog.translogId() == location.translogId) {
                reader = currentCommittingTranslog;
            } else {
                throw new IllegalStateException("Can't read from translog location" + location);
            }
            return reader.read(location);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to read source from translog location " + location, e);
        }
    }

    /**
     * Adds a create operation to the transaction log.
     */
    public Location add(Operation operation) throws TranslogException {
        ReleasableBytesStreamOutput out = new ReleasableBytesStreamOutput(bigArrays);
        try {
            writeOperation(out, operation);
            ReleasablePagedBytesReference bytes = out.bytes();
            try (ReleasableLock lock = readLock.acquire()) {
                Location location = current.add(bytes);
                if (syncOnEachOperation) {
                    current.sync();
                }
                assert current.assertBytesAtLocation(location, bytes);
                return location;
            }
        } catch (Throwable e) {
            throw new TranslogException(shardId, "Failed to write operation [" + operation + "]", e);
        } finally {
            Releasables.close(out.bytes());
        }
    }

    /**
     * Snapshots the current transaction log allowing to safely iterate over the snapshot.
     * Snapshots are fixed in time and will not be updated with future operations.
     */
    public Snapshot newSnapshot() {
        try (ReleasableLock lock = readLock.acquire()) {
            ArrayList<TranslogReader> toOpen = new ArrayList<>();
            toOpen.addAll(recoveredTranslogs);
            if (currentCommittingTranslog != null) {
                toOpen.add(currentCommittingTranslog);
            }
            toOpen.add(current);
            return createdSnapshot(toOpen.toArray(new TranslogReader[toOpen.size()]));
        }
    }

    private Snapshot createdSnapshot(TranslogReader... translogs) {
        ArrayList<ChannelSnapshot> channelSnapshots = new ArrayList<>();
        boolean success = false;
        try {
            for (TranslogReader translog : translogs) {
                channelSnapshots.add(translog.newChannelSnapshot());
            }
            Snapshot snapshot = new TranslogSnapshot(channelSnapshots);
            success = true;
            return snapshot;
        } finally {
            if (success == false) {
                IOUtils.closeWhileHandlingException(channelSnapshots);
            }
        }
    }

    /**
     * Returns a view into the current translog that is guaranteed to retain all current operations
     * while receiving future ones as well
     */
    public Translog.View newView() {
        // we need to acquire the read lock to make sure new translog is created
        // and will be missed by the view we're making
        try (ReleasableLock lock = readLock.acquire()) {
            ArrayList<TranslogReader> translogs = new ArrayList<>();
            try {
                if (currentCommittingTranslog != null) {
                    translogs.add(currentCommittingTranslog.clone());
                }
                translogs.add(current.reader());
                FsView view = new FsView(translogs);
                // this is safe as we know that no new translog is being made at the moment
                // (we hold a read lock) and the view will be notified of any future one
                outstandingViews.add(view);
                translogs.clear();
                return view;
            } finally {
                // close if anything happend and we didn't reach the clear
                IOUtils.closeWhileHandlingException(translogs);
            }
        }
    }

    /**
     * Sync's the translog.
     */
    public void sync() throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (closed.get() == false) {
                current.sync();
            }
        }
    }

    public boolean syncNeeded() {
        try (ReleasableLock lock = readLock.acquire()) {
            return current.syncNeeded();
        }
    }

    /** package private for testing */
    public String getFilename(long translogId) {
        return TRANSLOG_FILE_PREFIX + translogId + TRANSLOG_FILE_SUFFIX;
    }


    /**
     * Ensures that the given location has be synced / written to the underlying storage.
     * @return Returns <code>true</code> iff this call caused an actual sync operation otherwise <code>false</code>
     */
    public boolean ensureSynced(Location location) throws IOException {
        try (ReleasableLock lock = readLock.acquire()) {
            if (location.translogId == current.id) { // if we have a new one it's already synced
                return current.syncUpTo(location.translogLocation + location.size);
            }
        }
        return false;
    }

    /**
     * return stats
     */
    public TranslogStats stats() {
        // acquire lock to make the two numbers roughly consistent (no file change half way)
        try (ReleasableLock lock = readLock.acquire()) {
            return new TranslogStats(totalOperations(), sizeInBytes());
        }
    }

    private boolean isReferencedTranslogId(long translogId) {
        return translogId >= lastCommittedTranslogId;
    }


    private final class OnCloseRunnable implements Callback<ChannelReference> {
        @Override
        public void handle(ChannelReference channelReference) {
            try (ReleasableLock lock = writeLock.acquire()) {
                if (isReferencedTranslogId(channelReference.getTranslogId()) == false) {
                    // if the given path is not the current we can safely delete the file since all references are released
                    logger.trace("delete translog file - not referenced and not current anymore {}", channelReference.getPath());
                    IOUtils.deleteFilesIgnoringExceptions(channelReference.getPath());
                }
            }
        }
    }

    /**
     * a view into the translog, capturing all translog file at the moment of creation
     * and updated with any future translog.
     */
    class FsView implements View {

        boolean closed;
        // last in this list is always FsTranslog.current
        final List<TranslogReader> orderedTranslogs;

        FsView(List<TranslogReader> orderedTranslogs) {
            assert orderedTranslogs.isEmpty() == false;
            // clone so we can safely mutate..
            this.orderedTranslogs = new ArrayList<>(orderedTranslogs);
        }

        /**
         * Called by the parent class when ever the current translog changes
         *
         * @param oldCurrent a new read only reader for the old current (should replace the previous reference)
         * @param newCurrent a reader into the new current.
         */
        synchronized void onNewTranslog(TranslogReader oldCurrent, TranslogReader newCurrent) throws IOException {
            // even though the close method removes this view from outstandingViews, there is no synchronisation in place
            // between that operation and an ongoing addition of a new translog, already having an iterator.
            // As such, this method can be called despite of the fact that we are closed. We need to check and ignore.
            if (closed) {
                // we have to close the new references created for as as we will not hold them
                IOUtils.close(oldCurrent, newCurrent);
                return;
            }
            orderedTranslogs.remove(orderedTranslogs.size() - 1).close();
            orderedTranslogs.add(oldCurrent);
            orderedTranslogs.add(newCurrent);
        }

        @Override
        public synchronized long minTranslogId() {
            ensureOpen();
            return orderedTranslogs.get(0).translogId();
        }

        @Override
        public synchronized int totalOperations() {
            int ops = 0;
            for (TranslogReader translog : orderedTranslogs) {
                int tops = translog.totalOperations();
                if (tops == TranslogReader.UNKNOWN_OP_COUNT) {
                    return -1;
                }
                ops += tops;
            }
            return ops;
        }

        @Override
        public synchronized long sizeInBytes() {
            long size = 0;
            for (TranslogReader translog : orderedTranslogs) {
                size += translog.sizeInBytes();
            }
            return size;
        }

        public synchronized Snapshot snapshot() {
            ensureOpen();
            return createdSnapshot(orderedTranslogs.toArray(new TranslogReader[orderedTranslogs.size()]));
        }


        void ensureOpen() {
            if (closed) {
                throw new ElasticsearchException("View is already closed");
            }
        }

        @Override
        public void close() {
            List<TranslogReader> toClose = new ArrayList<>();
            try {
                synchronized (this) {
                    if (closed == false) {
                        logger.trace("closing view starting at translog [{}]", minTranslogId());
                        closed = true;
                        outstandingViews.remove(this);
                        toClose.addAll(orderedTranslogs);
                        orderedTranslogs.clear();
                    }
                }
            } finally {
                try {
                    // Close out of lock to prevent deadlocks between channel close which checks for
                    // references in InternalChannelReference.closeInternal (waiting on a read lock)
                    // and other FsTranslog#newTranslog calling FsView.onNewTranslog (while having a write lock)
                    IOUtils.close(toClose);
                } catch (Exception e) {
                    throw new ElasticsearchException("failed to close view", e);
                }
            }
        }
    }

    class Sync implements Runnable {
        @Override
        public void run() {
            // don't re-schedule  if its closed..., we are done
            if (closed.get()) {
                return;
            }
            if (syncNeeded()) {
                threadPool.executor(ThreadPool.Names.FLUSH).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            sync();
                        } catch (Exception e) {
                            logger.warn("failed to sync translog", e);
                        }
                        if (closed.get() == false) {
                            syncScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, Sync.this);
                        }
                    }
                });
            } else {
                syncScheduler = threadPool.schedule(syncInterval, ThreadPool.Names.SAME, Sync.this);
            }
        }
    }

    public static class Location implements Accountable, Comparable<Location> {

        public final long translogId;
        public final long translogLocation;
        public final int size;

        public Location(long translogId, long translogLocation, int size) {
            this.translogId = translogId;
            this.translogLocation = translogLocation;
            this.size = size;
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.NUM_BYTES_OBJECT_HEADER + 2 * RamUsageEstimator.NUM_BYTES_LONG + RamUsageEstimator.NUM_BYTES_INT;
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }

        @Override
        public String toString() {
            return "[id: " + translogId + ", location: " + translogLocation + ", size: " + size + "]";
        }

        @Override
        public int compareTo(Location o) {
            if (translogId == o.translogId) {
                return Long.compare(translogLocation, o.translogLocation);
            }
            return Long.compare(translogId, o.translogId);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Location location = (Location) o;

            if (translogId != location.translogId) return false;
            if (translogLocation != location.translogLocation) return false;
            return size == location.size;

        }

        @Override
        public int hashCode() {
            int result = (int) (translogId ^ (translogId >>> 32));
            result = 31 * result + (int) (translogLocation ^ (translogLocation >>> 32));
            result = 31 * result + size;
            return result;
        }
    }

    /**
     * A snapshot of the transaction log, allows to iterate over all the transaction log operations.
     */
    public interface Snapshot extends Releasable {

        /**
         * The total number of operations in the translog.
         */
        int estimatedTotalOperations();

        /**
         * Returns the next operation in the snapshot or <code>null</code> if we reached the end.
         */
        public Translog.Operation next() throws IOException;

    }

    /** a view into the current translog that receives all operations from the moment created */
    public interface View extends Releasable {

        /**
         * The total number of operations in the view.
         */
        int totalOperations();

        /**
         * Returns the size in bytes of the files behind the view.
         */
        long sizeInBytes();

        /** create a snapshot from this view */
        Snapshot snapshot();

        /** this smallest translog id in this view */
        long minTranslogId();

    }

    /**
     * A generic interface representing an operation performed on the transaction log.
     * Each is associated with a type.
     */
    public interface Operation extends Streamable {
        enum Type {
            CREATE((byte) 1),
            SAVE((byte) 2),
            DELETE((byte) 3),
            DELETE_BY_QUERY((byte) 4);

            private final byte id;

            private Type(byte id) {
                this.id = id;
            }

            public byte id() {
                return this.id;
            }

            public static Type fromId(byte id) {
                switch (id) {
                    case 1:
                        return CREATE;
                    case 2:
                        return SAVE;
                    case 3:
                        return DELETE;
                    case 4:
                        return DELETE_BY_QUERY;
                    default:
                        throw new IllegalArgumentException("No type mapped for [" + id + "]");
                }
            }
        }

        Type opType();

        long estimateSize();

        Source getSource();

    }

    public static class Source {
        public final BytesReference source;
        public final String routing;
        public final String parent;
        public final long timestamp;
        public final long ttl;

        public Source(BytesReference source, String routing, String parent, long timestamp, long ttl) {
            this.source = source;
            this.routing = routing;
            this.parent = parent;
            this.timestamp = timestamp;
            this.ttl = ttl;
        }
    }

    public static class Create implements Operation {
        public static final int SERIALIZATION_FORMAT = 6;

        private String id;
        private String type;
        private BytesReference source;
        private String routing;
        private String parent;
        private long timestamp;
        private long ttl;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Create() {
        }

        public Create(Engine.Create create) {
            this.id = create.id();
            this.type = create.type();
            this.source = create.source();
            this.routing = create.routing();
            this.parent = create.parent();
            this.timestamp = create.timestamp();
            this.ttl = create.ttl();
            this.version = create.version();
            this.versionType = create.versionType();
        }

        public Create(String type, String id, byte[] source) {
            this.id = id;
            this.type = type;
            this.source = new BytesArray(source);
        }

        @Override
        public Type opType() {
            return Type.CREATE;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String id() {
            return this.id;
        }

        public BytesReference source() {
            return this.source;
        }

        public String type() {
            return this.type;
        }

        public String routing() {
            return this.routing;
        }

        public String parent() {
            return this.parent;
        }

        public long timestamp() {
            return this.timestamp;
        }

        public long ttl() {
            return this.ttl;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            if (version >= 1) {
                if (in.readBoolean()) {
                    routing = in.readString();
                }
            }
            if (version >= 2) {
                if (in.readBoolean()) {
                    parent = in.readString();
                }
            }
            if (version >= 3) {
                this.version = in.readLong();
            }
            if (version >= 4) {
                this.timestamp = in.readLong();
            }
            if (version >= 5) {
                this.ttl = in.readLong();
            }
            if (version >= 6) {
                this.versionType = VersionType.fromValue(in.readByte());
            }

            assert versionType.validateVersionForWrites(version);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            if (routing == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(routing);
            }
            if (parent == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(parent);
            }
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
            out.writeByte(versionType.getValue());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Create create = (Create) o;

            if (timestamp != create.timestamp ||
                    ttl != create.ttl ||
                    version != create.version ||
                    id.equals(create.id) == false ||
                    type.equals(create.type) == false ||
                    source.equals(create.source) == false) {
                return false;
            }
            if (routing != null ? !routing.equals(create.routing) : create.routing != null) {
                return false;
            }
            if (parent != null ? !parent.equals(create.parent) : create.parent != null) {
                return false;
            }
            return versionType == create.versionType;

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (int) (ttl ^ (ttl >>> 32));
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + versionType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Create{" +
                    "id='" + id + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    public static class Index implements Operation {
        public static final int SERIALIZATION_FORMAT = 6;

        private String id;
        private String type;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;
        private BytesReference source;
        private String routing;
        private String parent;
        private long timestamp;
        private long ttl;

        public Index() {
        }

        public Index(Engine.Index index) {
            this.id = index.id();
            this.type = index.type();
            this.source = index.source();
            this.routing = index.routing();
            this.parent = index.parent();
            this.version = index.version();
            this.timestamp = index.timestamp();
            this.ttl = index.ttl();
            this.versionType = index.versionType();
        }

        public Index(String type, String id, byte[] source) {
            this.type = type;
            this.id = id;
            this.source = new BytesArray(source);
        }

        @Override
        public Type opType() {
            return Type.SAVE;
        }

        @Override
        public long estimateSize() {
            return ((id.length() + type.length()) * 2) + source.length() + 12;
        }

        public String type() {
            return this.type;
        }

        public String id() {
            return this.id;
        }

        public String routing() {
            return this.routing;
        }

        public String parent() {
            return this.parent;
        }

        public long timestamp() {
            return this.timestamp;
        }

        public long ttl() {
            return this.ttl;
        }

        public BytesReference source() {
            return this.source;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return versionType;
        }

        @Override
        public Source getSource() {
            return new Source(source, routing, parent, timestamp, ttl);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            id = in.readString();
            type = in.readString();
            source = in.readBytesReference();
            try {
                if (version >= 1) {
                    if (in.readBoolean()) {
                        routing = in.readString();
                    }
                }
                if (version >= 2) {
                    if (in.readBoolean()) {
                        parent = in.readString();
                    }
                }
                if (version >= 3) {
                    this.version = in.readLong();
                }
                if (version >= 4) {
                    this.timestamp = in.readLong();
                }
                if (version >= 5) {
                    this.ttl = in.readLong();
                }
                if (version >= 6) {
                    this.versionType = VersionType.fromValue(in.readByte());
                }
            } catch (Exception e) {
                throw new ElasticsearchException("failed to read [" + type + "][" + id + "]", e);
            }

            assert versionType.validateVersionForWrites(version);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(id);
            out.writeString(type);
            out.writeBytesReference(source);
            if (routing == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(routing);
            }
            if (parent == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                out.writeString(parent);
            }
            out.writeLong(version);
            out.writeLong(timestamp);
            out.writeLong(ttl);
            out.writeByte(versionType.getValue());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Index index = (Index) o;

            if (version != index.version ||
                    timestamp != index.timestamp ||
                    ttl != index.ttl ||
                    id.equals(index.id) == false ||
                    type.equals(index.type) == false ||
                    versionType != index.versionType ||
                    source.equals(index.source) == false) {
                return false;
            }
            if (routing != null ? !routing.equals(index.routing) : index.routing != null) {
                return false;
            }
            return !(parent != null ? !parent.equals(index.parent) : index.parent != null);

        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + versionType.hashCode();
            result = 31 * result + source.hashCode();
            result = 31 * result + (routing != null ? routing.hashCode() : 0);
            result = 31 * result + (parent != null ? parent.hashCode() : 0);
            result = 31 * result + (int) (timestamp ^ (timestamp >>> 32));
            result = 31 * result + (int) (ttl ^ (ttl >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Index{" +
                    "id='" + id + '\'' +
                    ", type='" + type + '\'' +
                    '}';
        }
    }

    public static class Delete implements Operation {
        public static final int SERIALIZATION_FORMAT = 2;

        private Term uid;
        private long version = Versions.MATCH_ANY;
        private VersionType versionType = VersionType.INTERNAL;

        public Delete() {
        }

        public Delete(Engine.Delete delete) {
            this(delete.uid());
            this.version = delete.version();
            this.versionType = delete.versionType();
        }

        public Delete(Term uid) {
            this.uid = uid;
        }

        public Delete(Term uid, long version, VersionType versionType) {
            this.uid = uid;
            this.version = version;
            this.versionType = versionType;
        }

        @Override
        public Type opType() {
            return Type.DELETE;
        }

        @Override
        public long estimateSize() {
            return ((uid.field().length() + uid.text().length()) * 2) + 20;
        }

        public Term uid() {
            return this.uid;
        }

        public long version() {
            return this.version;
        }

        public VersionType versionType() {
            return this.versionType;
        }

        @Override
        public Source getSource(){
            throw new IllegalStateException("trying to read doc source from delete operation");
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            uid = new Term(in.readString(), in.readString());
            if (version >= 1) {
                this.version = in.readLong();
            }
            if (version >= 2) {
                this.versionType = VersionType.fromValue(in.readByte());
            }
            assert versionType.validateVersionForWrites(version);

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeString(uid.field());
            out.writeString(uid.text());
            out.writeLong(version);
            out.writeByte(versionType.getValue());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Delete delete = (Delete) o;

            return version == delete.version &&
                    uid.equals(delete.uid) &&
                    versionType == delete.versionType;
        }

        @Override
        public int hashCode() {
            int result = uid.hashCode();
            result = 31 * result + (int) (version ^ (version >>> 32));
            result = 31 * result + versionType.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "Delete{" +
                    "uid=" + uid +
                    '}';
        }
    }

    /** @deprecated Delete-by-query is removed in 2.0, but we keep this so translog can replay on upgrade. */
    @Deprecated
    public static class DeleteByQuery implements Operation {

        public static final int SERIALIZATION_FORMAT = 2;
        private BytesReference source;
        @Nullable
        private String[] filteringAliases;
        private String[] types = Strings.EMPTY_ARRAY;

        public DeleteByQuery() {
        }

        public DeleteByQuery(Engine.DeleteByQuery deleteByQuery) {
            this(deleteByQuery.source(), deleteByQuery.filteringAliases(), deleteByQuery.types());
        }

        public DeleteByQuery(BytesReference source, String[] filteringAliases, String... types) {
            this.source = source;
            this.types = types == null ? Strings.EMPTY_ARRAY : types;
            this.filteringAliases = filteringAliases;
        }

        @Override
        public Type opType() {
            return Type.DELETE_BY_QUERY;
        }

        @Override
        public long estimateSize() {
            return source.length() + 8;
        }

        public BytesReference source() {
            return this.source;
        }

        public String[] filteringAliases() {
            return filteringAliases;
        }

        public String[] types() {
            return this.types;
        }

        @Override
        public Source getSource() {
            throw new IllegalStateException("trying to read doc source from delete_by_query operation");
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            int version = in.readVInt(); // version
            source = in.readBytesReference();
            if (version < 2) {
                // for query_parser_name, which was removed
                if (in.readBoolean()) {
                    in.readString();
                }
            }
            int typesSize = in.readVInt();
            if (typesSize > 0) {
                types = new String[typesSize];
                for (int i = 0; i < typesSize; i++) {
                    types[i] = in.readString();
                }
            }
            if (version >= 1) {
                int aliasesSize = in.readVInt();
                if (aliasesSize > 0) {
                    filteringAliases = new String[aliasesSize];
                    for (int i = 0; i < aliasesSize; i++) {
                        filteringAliases[i] = in.readString();
                    }
                }
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(SERIALIZATION_FORMAT);
            out.writeBytesReference(source);
            out.writeVInt(types.length);
            for (String type : types) {
                out.writeString(type);
            }
            if (filteringAliases != null) {
                out.writeVInt(filteringAliases.length);
                for (String alias : filteringAliases) {
                    out.writeString(alias);
                }
            } else {
                out.writeVInt(0);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            DeleteByQuery that = (DeleteByQuery) o;

            if (!Arrays.equals(filteringAliases, that.filteringAliases)) {
                return false;
            }
            if (!Arrays.equals(types, that.types)) {
                return false;
            }
            return source.equals(that.source);
        }

        @Override
        public int hashCode() {
            int result = source.hashCode();
            result = 31 * result + (filteringAliases != null ? Arrays.hashCode(filteringAliases) : 0);
            result = 31 * result + Arrays.hashCode(types);
            return result;
        }

        @Override
        public String toString() {
            return "DeleteByQuery{" +
                    "types=" + Arrays.toString(types) +
                    '}';
        }
    }

    /**
     * Returns the current durability mode of this translog.
     */
    public Durabilty getDurabilty() {
        return durabilty;
    }

    public enum Durabilty {
        /**
         * Async durability - translogs are synced based on a time interval.
         */
        ASYNC,
        /**
         * Request durability - translogs are synced for each high levle request (bulk, index, delete)
         */
        REQUEST;

        public static Durabilty getFromSettings(ESLogger logger, Settings settings, Durabilty defaultValue) {
            final String value = settings.get(INDEX_TRANSLOG_DURABILITY, defaultValue.name());
            try {
                return valueOf(value.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                logger.warn("Can't apply {} illegal value: {} using {} instead, use one of: {}", INDEX_TRANSLOG_DURABILITY, value, defaultValue, Arrays.toString(values()));
                return defaultValue;
            }
        }
    }

    private static void verifyChecksum(BufferedChecksumStreamInput in) throws IOException {
        // This absolutely must come first, or else reading the checksum becomes part of the checksum
        long expectedChecksum = in.getChecksum();
        long readChecksum = in.readInt() & 0xFFFF_FFFFL;
        if (readChecksum != expectedChecksum) {
            throw new TranslogCorruptedException("translog stream is corrupted, expected: 0x" +
                    Long.toHexString(expectedChecksum) + ", got: 0x" + Long.toHexString(readChecksum));
        }
    }

    public static Translog.Operation readOperation(StreamInput input) throws IOException {
        // TODO: validate size to prevent OOME
        int opSize = input.readInt();
        // This BufferedChecksumStreamInput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(input);
        Translog.Operation operation;
        try {
            Translog.Operation.Type type = Translog.Operation.Type.fromId(in.readByte());
            operation = newOperationFromType(type);
            operation.readFrom(in);
            verifyChecksum(in);
        } catch (EOFException e) {
            throw new TruncatedTranslogException("reached premature end of file, translog is truncated", e);
        } catch (AssertionError|Exception e) {
            throw new TranslogCorruptedException("translog corruption while reading from stream", e);
        }
        return operation;
    }

    public static void writeOperation(StreamOutput outStream, Translog.Operation op) throws IOException {
        // We first write to a NoopStreamOutput to get the size of the
        // operation. We could write to a byte array and then send that as an
        // alternative, but here we choose to use CPU over allocating new
        // byte arrays.
        NoopStreamOutput noopOut = new NoopStreamOutput();
        noopOut.writeByte(op.opType().id());
        op.writeTo(noopOut);
        noopOut.writeInt(0); // checksum holder
        int size = noopOut.getCount();

        // This BufferedChecksumStreamOutput remains unclosed on purpose,
        // because closing it closes the underlying stream, which we don't
        // want to do here.
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(outStream);
        outStream.writeInt(size); // opSize is not checksummed
        out.writeByte(op.opType().id());
        op.writeTo(out);
        long checksum = out.getChecksum();
        out.writeInt((int)checksum);
    }

    /**
     * Returns a new empty translog operation for the given {@link Translog.Operation.Type}
     */
    static Translog.Operation newOperationFromType(Translog.Operation.Type type) throws IOException {
        switch (type) {
            case CREATE:
                return new Translog.Create();
            case DELETE:
                return new Translog.Delete();
            case DELETE_BY_QUERY:
                return new Translog.DeleteByQuery();
            case SAVE:
                return new Translog.Index();
            default:
                throw new IOException("No type for [" + type + "]");
        }
    }


    @Override
    public void prepareCommit() throws IOException {
        TranslogWriter writer = null;
        try (ReleasableLock lock = writeLock.acquire()) {
            if (currentCommittingTranslog != null) {
                throw new IllegalStateException("already committing a translog with id: " + currentCommittingTranslog.translogId());
            }
            writer = current;
            currentCommittingTranslog = current.immutableReader();
            current.sync();
            // create a new translog file - this will sync it
            final TranslogWriter newFile = createWriter(current.translogId() + 1, true);
            current = newFile;
            // notify all outstanding views of the new translog (no views are created now as
            // we hold a write lock).
            for (FsView view : outstandingViews) {
                view.onNewTranslog(currentCommittingTranslog.clone(), newFile.reader());
            }
            logger.trace("current translog set to [{}]", current.translogId());
            assert writer.syncNeeded() == false : "old translog writer must not need a sync";

        } catch (Throwable t) {
            close();// tragic event
            throw t;
        } finally {
            IOUtils.close(writer);
        }
    }

    @Override
    public void commit() throws IOException {
        ImmutableTranslogReader toClose = null;
        try (ReleasableLock lock = writeLock.acquire()) {
            if (currentCommittingTranslog == null) {
                prepareCommit();
            }
            current.sync();
            lastCommittedTranslogId = current.translogId(); // this is important - otherwise old files will not be cleaned up
            if (recoveredTranslogs.isEmpty() == false) {
                IOUtils.close(recoveredTranslogs);
                recoveredTranslogs.clear();
            }
            toClose = this.currentCommittingTranslog;
            this.currentCommittingTranslog = null;
        } finally {
            IOUtils.close(toClose);
        }
    }

    @Override
    public void rollback() throws IOException {
        close();
    }

    public enum OpenMode {
        CREATE,
        RECOVER,
        OPEN;
    }

}
