package io.hamster.protocols.raft.storage.log;

import io.hamster.storage.StorageLevel;
import io.hamster.storage.journal.DelegatingJournal;
import io.hamster.storage.journal.SegmentedJournal;

import java.io.File;

public class RaftLog extends DelegatingJournal<RaftLogEntry> {

    /**
     * Returns a new Raft log builder.
     *
     * @return A new Raft log builder.
     */
    public static Builder builder() {
        return new Builder();
    }

    private final SegmentedJournal<RaftLogEntry> journal;
    private final boolean flushOnCommit;
    private final RaftLogWriter writer;
    private volatile long commitIndex;

    protected RaftLog(SegmentedJournal<RaftLogEntry> journal, boolean flushOnCommit) {
        super(journal);
        this.journal = journal;
        this.flushOnCommit = flushOnCommit;
        this.writer = new RaftLogWriter(journal.writer());
    }

    @Override
    public RaftLogWriter writer() {
        return writer;
    }

    @Override
    public RaftLogReader openReader(long index) {
        return openReader(index, RaftLogReader.Mode.ALL);
    }

    @Override
    public RaftLogReader openReader(long index, RaftLogReader.Mode mode) {
        return new RaftLogReader(journal.openReader(index, mode));
    }

    /**
     * Returns whether {@code flushOnCommit} is enabled for the log.
     *
     * @return Indicates whether {@code flushOnCommit} is enabled for the log.
     */
    boolean isFlushOnCommit() {
        return flushOnCommit;
    }

    /**
     * Commits entries up to the given index.
     *
     * @param index The index up to which to commit entries.
     */
    void setCommitIndex(long index) {
        this.commitIndex = index;
    }

    /**
     * Returns the Raft log commit index.
     *
     * @return The Raft log commit index.
     */
    long getCommitIndex() {
        return commitIndex;
    }

    /**
     * Compacts the journal up to the given index.
     * <p>
     * The semantics of compaction are not specified by this interface.
     *
     * @param index The index up to which to compact the journal.
     */
    public void compact(long index) {
        journal.compact(index);
    }

    /**
     * Raft log builder.
     */
    public static class Builder implements io.hamster.utils.Builder<RaftLog> {
        private static final boolean DEFAULT_FLUSH_ON_COMMIT = false;

        private final SegmentedJournal.Builder<RaftLogEntry> journalBuilder = SegmentedJournal.builder();
        private boolean flushOnCommit = DEFAULT_FLUSH_ON_COMMIT;

        protected Builder() {
        }

        /**
         * Sets the storage name.
         *
         * @param name The storage name.
         * @return The storage builder.
         */
        public Builder withName(String name) {
            journalBuilder.withName(name);
            return this;
        }

        /**
         * Sets the log storage level, returning the builder for method chaining.
         * <p>
         * The storage level indicates how individual entries should be persisted in the journal.
         *
         * @param storageLevel The log storage level.
         * @return The storage builder.
         */
        public Builder withStorageLevel(StorageLevel storageLevel) {
            journalBuilder.withStorageLevel(storageLevel);
            return this;
        }

        /**
         * Sets the log directory, returning the builder for method chaining.
         * <p>
         * The log will write segment files into the provided directory.
         *
         * @param directory The log directory.
         * @return The storage builder.
         * @throws NullPointerException If the {@code directory} is {@code null}
         */
        public Builder withDirectory(String directory) {
            journalBuilder.withDirectory(directory);
            return this;
        }

        /**
         * Sets the log directory, returning the builder for method chaining.
         * <p>
         * The log will write segment files into the provided directory.
         *
         * @param directory The log directory.
         * @return The storage builder.
         * @throws NullPointerException If the {@code directory} is {@code null}
         */
        public Builder withDirectory(File directory) {
            journalBuilder.withDirectory(directory);
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
            journalBuilder.withMaxSegmentSize(maxSegmentSize);
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
            journalBuilder.withMaxEntrySize(maxEntrySize);
            return this;
        }

        /**
         * Sets the log index density.
         * <p>
         * The index density is the frequency at which the position of entries written to the journal will be recorded in
         * an in-memory index for faster seeking.
         *
         * @param indexDensity the index density
         * @return the log builder
         * @throws IllegalArgumentException if the density is not between 0 and 1
         */
        public Builder withIndexDensity(double indexDensity) {
            journalBuilder.withIndexDensity(indexDensity);
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

        @Override
        public RaftLog build() {
            return new RaftLog(journalBuilder.withCodec(new RaftLogCodec()).build(), flushOnCommit);
        }
    }
}
