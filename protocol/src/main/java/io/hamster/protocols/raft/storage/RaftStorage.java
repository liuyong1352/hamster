package io.hamster.protocols.raft.storage;

import io.hamster.protocols.raft.storage.log.RaftLog;
import io.hamster.protocols.raft.storage.system.MetaStore;
import io.hamster.storage.StorageException;
import io.hamster.storage.StorageLevel;
import io.hamster.storage.journal.JournalSegmentDescriptor;
import io.hamster.storage.journal.JournalSegmentFile;
import io.hamster.storage.statistics.StorageStatistics;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.function.Predicate;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Immutable log configuration and {@link RaftLog} factory.
 * <p>
 * This class provides a factory for {@link RaftLog} objects. {@code Storage} objects are immutable and
 * can be created only via the {@link RaftStorage.Builder}. To create a new
 * {@code Storage.Builder}, use the static {@link #builder()} factory method:
 * <pre>
 *   {@code
 *     Storage storage = Storage.builder()
 *       .withDirectory(new File("logs"))
 *       .withStorageLevel(StorageLevel.DISK)
 *       .build();
 *   }
 * </pre>
 *
 * @see RaftLog
 */
public class RaftStorage {

    /**
     * Returns a new storage builder.
     *
     * @return A new storage builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    private final String prefix;
    private final StorageLevel storageLevel;
    private final File directory;
    private final int maxSegmentSize;
    private final int maxEntrySize;
    private final boolean dynamicCompaction;
    private final double freeDiskBuffer;
    private final double freeMemoryBuffer;
    private final boolean flushOnCommit;
    private final boolean retainStaleSnapshots;
    private final StorageStatistics statistics;


    private RaftStorage(
            String prefix,
            StorageLevel storageLevel,
            File directory,
            int maxSegmentSize,
            int maxEntrySize,
            boolean dynamicCompaction,
            double freeDiskBuffer,
            double freeMemoryBuffer,
            boolean flushOnCommit,
            boolean retainStaleSnapshots) {
        this.prefix = prefix;
        this.storageLevel = storageLevel;
        this.directory = directory;
        this.maxSegmentSize = maxSegmentSize;
        this.maxEntrySize = maxEntrySize;
        this.dynamicCompaction = dynamicCompaction;
        this.freeDiskBuffer = freeDiskBuffer;
        this.freeMemoryBuffer = freeMemoryBuffer;
        this.flushOnCommit = flushOnCommit;
        this.retainStaleSnapshots = retainStaleSnapshots;
        this.statistics = new StorageStatistics(directory);
        directory.mkdirs();
    }

    /**
     * Returns the storage filename prefix.
     *
     * @return The storage filename prefix.
     */
    public String prefix() {
        return prefix;
    }

    /**
     * Returns the storage directory.
     * <p>
     * The storage directory is the directory to which all {@link RaftLog}s write files. Segment files
     * for multiple logs may be stored in the storage directory, and files for each log instance will be identified
     * by the {@code name} provided when the log is {@link #openLog() opened}.
     *
     * @return The storage directory.
     */
    public File directory() {
        return directory;
    }

    /**
     * Returns the storage level.
     * <p>
     * The storage level dictates how entries within individual log {@link RaftLog}s should be stored.
     *
     * @return The storage level.
     */
    public StorageLevel storageLevel() {
        return storageLevel;
    }

    /**
     * Returns the maximum log segment size.
     * <p>
     * The maximum segment size dictates the maximum size any segment in a {@link RaftLog} may consume
     * in bytes.
     *
     * @return The maximum segment size in bytes.
     */
    public int maxLogSegmentSize() {
        return maxSegmentSize;
    }

    /**
     * Returns whether dynamic log compaction is enabled.
     *
     * @return whether dynamic log compaction is enabled
     */
    public boolean dynamicCompaction() {
        return dynamicCompaction;
    }

    /**
     * Returns the percentage of disk space that must be available before log compaction is forced.
     *
     * @return the percentage of disk space that must be available before log compaction is forced
     */
    public double freeDiskBuffer() {
        return freeDiskBuffer;
    }

    /**
     * Returns the percentage of memory space that must be available before log compaction is forced.
     *
     * @return the percentage of memory space that must be available before log compaction is forced
     */
    public double freeMemoryBuffer() {
        return freeMemoryBuffer;
    }

    /**
     * Returns whether to flush buffers to disk when entries are committed.
     *
     * @return Whether to flush buffers to disk when entries are committed.
     */
    public boolean isFlushOnCommit() {
        return flushOnCommit;
    }

    /**
     * Returns a boolean value indicating whether to retain stale snapshots on disk.
     * <p>
     * If this option is enabled, snapshots will be retained on disk even after they no longer contribute
     * to the state of the system (there's a more recent snapshot). Users may want to disable this option
     * for backup purposes.
     *
     * @return Indicates whether to retain stale snapshots on disk.
     */
    public boolean isRetainStaleSnapshots() {
        return retainStaleSnapshots;
    }

    /**
     * Returns the Raft storage statistics.
     *
     * @return the Raft storage statistics
     */
    public StorageStatistics statistics() {
        return statistics;
    }


    /**
     * Attempts to acquire a lock on the storage directory.
     *
     * @param id the ID with which to lock the directory
     * @return indicates whether the lock was successfully acquired
     */
    public boolean lock(String id) {
        File file = new File(directory, String.format(".%s.lock", prefix));
        try {
            if (file.createNewFile()) {
                try (OutputStream out = new FileOutputStream(file)) {
                    out.write(id.getBytes(StandardCharsets.UTF_8));
                }
                return true;
            } else {
                try (InputStream input = new FileInputStream(file)) {
                    byte[] bytes = new byte[128];
                    int len = input.read(bytes);
                    if (len != -1) {
                        String lock = new String(bytes, 0, len, StandardCharsets.UTF_8);
                        return lock.equals(id);
                    }
                }
                return false;
            }
        } catch (IOException e) {
            throw new StorageException("Failed to acquire storage lock");
        }
    }

    /**
     * Opens a new {@link MetaStore}, recovering metadata from disk if it exists.
     * <p>
     * The meta store will be loaded using based on the configured {@link StorageLevel}. If the storage level is persistent
     * then the meta store will be loaded from disk, otherwise a new meta store will be created.
     *
     * @return The metastore.
     */
    public MetaStore openMetaStore() {
        return new MetaStore(this);
    }

    /**
     * Deletes a {@link MetaStore} from disk.
     * <p>
     * The meta store will be deleted by simply reading {@code meta} file names from disk and deleting metadata
     * files directly. Deleting the meta store does not involve reading any metadata files into memory.
     */
    public void deleteMetaStore() {
        deleteFiles(f -> f.getName().equals(String.format("%s.meta", prefix))
                || f.getName().equals(String.format("%s.conf", prefix)));
    }

    /**
     * Unlocks the storage directory.
     */
    public void unlock() {
        deleteFiles(f -> f.getName().equals(String.format(".%s.lock", prefix)));
    }

    /**
     * Opens a new {@link RaftLog}, recovering the log from disk if it exists.
     * <p>
     * When a log is opened, the log will attempt to load segments from the storage {@link #directory()}
     * according to the provided log {@code name}. If segments for the given log name are present on disk, segments
     * will be loaded and indexes will be rebuilt from disk. If no segments are found, an empty log will be created.
     * <p>
     * When log files are loaded from disk, the file names are expected to be based on the provided log {@code name}.
     *
     * @return The opened log.
     */
    public RaftLog openLog() {
        return RaftLog.builder()
                .withName(prefix)
                .withDirectory(directory)
                .withStorageLevel(storageLevel)
                .withMaxSegmentSize(maxSegmentSize)
                .withMaxEntrySize(maxEntrySize)
                .withFlushOnCommit(flushOnCommit)
                .build();
    }

    /**
     * Deletes a {@link RaftLog} from disk.
     * <p>
     * The log will be deleted by simply reading {@code log} file names from disk and deleting log files directly.
     * Deleting log files does not involve rebuilding indexes or reading any logs into memory.
     */
    public void deleteLog() {
        deleteFiles(f -> JournalSegmentFile.isSegmentFile(prefix, f));
    }

    /**
     * Deletes file in the storage directory that match the given predicate.
     */
    private void deleteFiles(Predicate<File> predicate) {
        directory.mkdirs();

        // Iterate through all files in the storage directory.
        for (File file : directory.listFiles(f -> f.isFile() && predicate.test(f))) {
            try {
                Files.delete(file.toPath());
            } catch (IOException e) {
                // Ignore the exception.
            }
        }
    }

    @Override
    public String toString() {
        return toStringHelper(this)
                .add("directory", directory())
                .toString();
    }

    /**
     * Builds a {@link RaftStorage} configuration.
     * <p>
     * The storage builder provides simplifies building more complex {@link RaftStorage} configurations. To
     * create a storage builder, use the {@link #builder()} factory method. Set properties of the configured
     * {@code Storage} object with the various {@code with*} methods. Once the storage has been configured,
     * call {@link #build()} to build the object.
     * <pre>
     *   {@code
     *   RaftStorage storage = RaftStorage.builder()
     *     .withDirectory(new File("logs"))
     *     .withPersistenceLevel(PersistenceLevel.DISK)
     *     .build();
     *   }
     * </pre>
     */
    public static class Builder implements io.hamster.utils.Builder<RaftStorage> {

        private static final String DEFAULT_PREFIX = "hamster";
        private static final String DEFAULT_DIRECTORY = System.getProperty(DEFAULT_PREFIX + ".data", System.getProperty("user.dir"));
        private static final int DEFAULT_MAX_SEGMENT_SIZE = 1024 * 1024 * 32;
        private static final int DEFAULT_MAX_ENTRY_SIZE = 1024 * 1024; // 1MB
        private static final boolean DEFAULT_DYNAMIC_COMPACTION = true;
        private static final double DEFAULT_FREE_DISK_BUFFER = .2;
        private static final double DEFAULT_FREE_MEMORY_BUFFER = .2;
        private static final boolean DEFAULT_FLUSH_ON_COMMIT = true;
        private static final boolean DEFAULT_RETAIN_STALE_SNAPSHOTS = false;

        private String prefix = DEFAULT_PREFIX;
        private StorageLevel storageLevel = StorageLevel.DISK;
        private File directory = new File(DEFAULT_DIRECTORY);
        private int maxSegmentSize = DEFAULT_MAX_SEGMENT_SIZE;
        private int maxEntrySize = DEFAULT_MAX_ENTRY_SIZE;
        private boolean dynamicCompaction = DEFAULT_DYNAMIC_COMPACTION;
        private double freeDiskBuffer = DEFAULT_FREE_DISK_BUFFER;
        private double freeMemoryBuffer = DEFAULT_FREE_MEMORY_BUFFER;
        private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;
        private boolean retainStaleSnapshots = DEFAULT_RETAIN_STALE_SNAPSHOTS;

        private Builder() {
        }

        /**
         * Sets the storage prefix.
         *
         * @param prefix The storage prefix.
         * @return The storage builder.
         */
        public Builder withPrefix(String prefix) {
            this.prefix = checkNotNull(prefix, "prefix cannot be null");
            return this;
        }

        /**
         * Sets the log storage level, returning the builder for method chaining.
         * <p>
         * The storage level indicates how individual {@link io.hamster.protocols.raft.storage.log.RaftLogEntry entries}
         * should be persisted in the log.
         *
         * @param storageLevel The log storage level.
         * @return The storage builder.
         */
        public Builder withStorageLevel(StorageLevel storageLevel) {
            this.storageLevel = checkNotNull(storageLevel, "storageLevel");
            return this;
        }

        /**
         * Sets the log directory, returning the builder for method chaining.
         * <p>
         * The log will write segment files into the provided directory. If multiple {@link RaftStorage} objects are located
         * on the same machine, they write logs to different directories.
         *
         * @param directory The log directory.
         * @return The storage builder.
         * @throws NullPointerException If the {@code directory} is {@code null}
         */
        public Builder withDirectory(String directory) {
            return withDirectory(new File(checkNotNull(directory, "directory")));
        }

        /**
         * Sets the log directory, returning the builder for method chaining.
         * <p>
         * The log will write segment files into the provided directory. If multiple {@link RaftStorage} objects are located
         * on the same machine, they write logs to different directories.
         *
         * @param directory The log directory.
         * @return The storage builder.
         * @throws NullPointerException If the {@code directory} is {@code null}
         */
        public Builder withDirectory(File directory) {
            this.directory = checkNotNull(directory, "directory");
            return this;
        }

        /**
         * Sets the maximum segment size in bytes, returning the builder for method chaining.
         * <p>
         * The maximum segment size dictates when logs should roll over to new segments. As entries are written to
         * a segment of the log, once the size of the segment surpasses the configured maximum segment size, the
         * log will create a new segment and append new entries to that segment.
         * <p>
         * By default, the maximum segment size is {@code 1024 * 1024 * 32}.
         *
         * @param maxSegmentSize The maximum segment size in bytes.
         * @return The storage builder.
         * @throws IllegalArgumentException If the {@code maxSegmentSize} is not positive
         */
        public Builder withMaxSegmentSize(int maxSegmentSize) {
            checkArgument(maxSegmentSize > JournalSegmentDescriptor.BYTES, "maxSegmentSize must be greater than " + JournalSegmentDescriptor.BYTES);
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }

        /**
         * Sets the maximum entry size in bytes, returning the builder for method chaining.
         *
         * @param maxEntrySize the maximum entry size in bytes
         * @return the storage builder
         * @throws IllegalArgumentException if the {@code maxEntrySize} is not positive
         */
        public Builder withMaxEntrySize(int maxEntrySize) {
            checkArgument(maxEntrySize > 0, "maxEntrySize must be positive");
            this.maxEntrySize = maxEntrySize;
            return this;
        }

        /**
         * Enables dynamic log compaction.
         * <p>
         * When dynamic compaction is enabled, logs will be compacted only during periods of low load on the cluster
         * or when the cluster is running out of disk space.
         *
         * @return the Raft storage builder
         */
        public Builder withDynamicCompaction() {
            return withDynamicCompaction(true);
        }

        /**
         * Enables dynamic log compaction.
         * <p>
         * When dynamic compaction is enabled, logs will be compacted only during periods of low load on the cluster
         * or when the cluster is running out of disk space.
         *
         * @param dynamicCompaction whether to enable dynamic compaction
         * @return the Raft storage builder
         */
        public Builder withDynamicCompaction(boolean dynamicCompaction) {
            this.dynamicCompaction = dynamicCompaction;
            return this;
        }

        /**
         * Sets the percentage of free disk space that must be preserved before log compaction is forced.
         *
         * @param freeDiskBuffer the free disk percentage
         * @return the Raft log builder
         */
        public Builder withFreeDiskBuffer(double freeDiskBuffer) {
            checkArgument(freeDiskBuffer > 0, "freeDiskBuffer must be positive");
            checkArgument(freeDiskBuffer < 1, "freeDiskBuffer must be less than 1");
            this.freeDiskBuffer = freeDiskBuffer;
            return this;
        }

        /**
         * Sets the percentage of free memory space that must be preserved before log compaction is forced.
         *
         * @param freeMemoryBuffer the free disk percentage
         * @return the Raft log builder
         */
        public Builder withFreeMemoryBuffer(double freeMemoryBuffer) {
            checkArgument(freeMemoryBuffer > 0, "freeMemoryBuffer must be positive");
            checkArgument(freeMemoryBuffer < 1, "freeMemoryBuffer must be less than 1");
            this.freeMemoryBuffer = freeMemoryBuffer;
            return this;
        }

        /**
         * Enables flushing buffers to disk when entries are committed to a segment, returning the builder
         * for method chaining.
         * <p>
         * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time
         * an entry is committed in a given segment.
         *
         * @return The storage builder.
         */
        public Builder withFlushOnCommit() {
            return withFlushOnCommit(true);
        }

        /**
         * Sets whether to flush buffers to disk when entries are committed to a segment, returning the builder
         * for method chaining.
         * <p>
         * When flush-on-commit is enabled, log entry buffers will be automatically flushed to disk each time
         * an entry is committed in a given segment.
         *
         * @param flushOnCommit Whether to flush buffers to disk when entries are committed to a segment.
         * @return The storage builder.
         */
        public Builder withFlushOnCommit(boolean flushOnCommit) {
            this.flushOnCommit = flushOnCommit;
            return this;
        }

        /**
         * Enables retaining stale snapshots on disk, returning the builder for method chaining.
         * <p>
         * As the system state progresses, periodic snapshots of the state machine's state are taken.
         * Once a new snapshot of the state machine is taken, all preceding snapshots no longer contribute
         * to the state of the system and can therefore be removed from disk. By default, snapshots will not
         * be retained once a new snapshot is stored on disk. Enabling snapshot retention will ensure that
         * all snapshots will be saved, e.g. for backup purposes.
         *
         * @return The storage builder.
         */
        public Builder withRetainStaleSnapshots() {
            return withRetainStaleSnapshots(true);
        }

        /**
         * Sets whether to retain stale snapshots on disk, returning the builder for method chaining.
         * <p>
         * As the system state progresses, periodic snapshots of the state machine's state are taken.
         * Once a new snapshot of the state machine is taken, all preceding snapshots no longer contribute
         * to the state of the system and can therefore be removed from disk. By default, snapshots will not
         * be retained once a new snapshot is stored on disk. Enabling snapshot retention will ensure that
         * all snapshots will be saved, e.g. for backup purposes.
         *
         * @param retainStaleSnapshots Whether to retain stale snapshots on disk.
         * @return The storage builder.
         */
        public Builder withRetainStaleSnapshots(boolean retainStaleSnapshots) {
            this.retainStaleSnapshots = retainStaleSnapshots;
            return this;
        }

        @Override
        public RaftStorage build() {
            return new RaftStorage(
                    prefix,
                    storageLevel,
                    directory,
                    maxSegmentSize,
                    maxEntrySize,
                    dynamicCompaction,
                    freeDiskBuffer,
                    freeMemoryBuffer,
                    flushOnCommit,
                    retainStaleSnapshots);
        }
    }
}
