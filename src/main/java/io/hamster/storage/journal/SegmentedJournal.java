package io.hamster.storage.journal;

import com.google.common.collect.Sets;
import io.hamster.storage.StorageException;
import io.hamster.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class SegmentedJournal<E> implements Journal<E> {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final String name;
    private final StorageLevel storageLevel;
    private final File directory;
    private final JournalCodec<E> codec;

    private final NavigableMap<Long, JournalSegment<E>> segments = new ConcurrentSkipListMap<>();

    private final int maxEntrySize;
    private final int maxSegmentSize;
    private JournalSegment<E> currentSegment;
    private final SegmentedJournalWriter<E> writer;
    private final Collection<SegmentedJournalReader> readers = Sets.newConcurrentHashSet();

    private volatile long commitIndex;
    private volatile boolean open = true;

    public SegmentedJournal(
            String name,
            StorageLevel storageLevel,
            File directory,
            JournalCodec<E> codec,
            int maxSegmentSize,
            int maxEntrySize) {
        this.name = name;
        this.storageLevel = checkNotNull(storageLevel, "storageLevel cannot be null");
        this.directory = directory;

        this.codec = codec;
        this.maxSegmentSize = maxSegmentSize;
        this.maxEntrySize = maxEntrySize;
        open();
        this.writer = new SegmentedJournalWriter<>(this);
    }

    /**
     * Opens the segments.
     */
    private void open() {
        // Load existing log segments from disk.
        for (JournalSegment<E> segment : loadSegments()) {
            segments.put(segment.descriptor().index(), segment);
        }
        // If a segment doesn't already exist, create an initial segment starting at index 1.
        if (!segments.isEmpty()) {
            currentSegment = segments.lastEntry().getValue();
        } else {
            JournalSegmentDescriptor descriptor = JournalSegmentDescriptor.builder()
                    .withId(1)
                    .withIndex(1)
                    .withMaxSegmentSize(maxSegmentSize)
                    .build();

            currentSegment = createSegment(descriptor);

            segments.put(descriptor.index(), currentSegment);
        }
    }

    private JournalSegment<E> createSegment(JournalSegmentDescriptor descriptor) {
        File segmentFile = JournalSegmentFile.createSegmentFile(name, directory, descriptor.id());

        RandomAccessFile randomFile = null;
        FileChannel channel = null;
        try {
            randomFile = new RandomAccessFile(segmentFile, "rw");
            randomFile.setLength(descriptor.maxSegmentSize());
            channel = randomFile.getChannel();

            ByteBuffer buffer = ByteBuffer.allocate(JournalSegmentDescriptor.BYTES);
            descriptor.copyTo(buffer);
            buffer.flip();
            channel.write(buffer);

        } catch (IOException e) {
            throw new StorageException(e);
        } finally {
            try {
                if (channel != null) {
                    channel.close();
                }

                if (randomFile != null) {
                    randomFile.close();
                }
            } catch (IOException e) {
                //ignore
            }
        }
        JournalSegment<E> segment = newSegment(new JournalSegmentFile(segmentFile), descriptor);
        log.debug("Created segment: {}", segment);
        return segment;
    }

    private JournalSegment<E> newSegment(JournalSegmentFile journalSegmentFile, JournalSegmentDescriptor descriptor) {
        return new JournalSegment<>(journalSegmentFile, descriptor, codec, maxEntrySize);
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

    @Override
    public SegmentedJournalWriter<E> writer() {
        return writer;
    }

    @Override
    public SegmentedJournalReader<E> openReader(long index) {
        return openReader(index, SegmentedJournalReader.Mode.ALL);
    }

    /**
     * Opens a new Raft log reader with the given reader mode.
     *
     * @param index The index from which to begin reading entries.
     * @param mode  The mode in which to read entries.
     * @return The Raft log reader.
     */
    public SegmentedJournalReader<E> openReader(long index, SegmentedJournalReader.Mode mode) {
        SegmentedJournalReader<E> reader = new SegmentedJournalReader<>(this, index, mode);
        readers.add(reader);
        return reader;
    }

    /**
     * Returns the segment for the given index.
     *
     * @param index The index for which to return the segment.
     * @throws IllegalStateException if the segment manager is not open
     */
    synchronized JournalSegment<E> getSegment(long index) {
        assertOpen();
        if (currentSegment != null && index > currentSegment.index()) {
            return currentSegment;
        }
        // If the index is in another segment, get the entry with the next lowest first index.
        Map.Entry<Long, JournalSegment<E>> segment = segments.floorEntry(index);
        if(segment != null){
            return segment.getValue();
        }
        return getFirstSegment();
    }

    /**
     * Returns the segment following the segment with the given ID.
     *
     * @param index The segment index with which to look up the next segment.
     * @return The next segment for the given index.
     */
    JournalSegment<E> getNextSegment(long index) {
        Map.Entry<Long, JournalSegment<E>> nextSegment = segments.higherEntry(index);
        return nextSegment != null ? nextSegment.getValue() : null;
    }


    /**
     * Returns the first segment in the log.
     *
     * @throws IllegalStateException if the segment manager is not open
     */
    JournalSegment<E> getFirstSegment() {
        assertOpen();
        Map.Entry<Long, JournalSegment<E>> segment = segments.firstEntry();
        return segment != null ? segment.getValue() : null;
    }

    void closeReader(SegmentedJournalReader reader) {
        readers.remove(reader);
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    /**
     * Returns the last segment in the log.
     *
     * @throws IllegalStateException if the segment manager is not open
     */
    JournalSegment<E> getLastSegment() {
        assertOpen();
        return segments.lastEntry().getValue();
    }

    /**
     * Asserts that the manager is open.
     *
     * @throws IllegalStateException if the segment manager is not open
     */
    private void assertOpen() {
        checkState(currentSegment != null, "journal not open");
    }


    /**
     * Loads all segments from disk.
     *
     * @return A collection of segments for the log.
     */
    protected Collection<JournalSegment<E>> loadSegments() {
        // Ensure log directories are created.
        directory.mkdirs();

        TreeMap<Long, JournalSegment<E>> segments = new TreeMap<>();
        for (File file : directory.listFiles(File::isFile)) {

            // If the file looks like a segment file, attempt to load the segment.
            if (JournalSegmentFile.isSegmentFile(name, file)) {
                JournalSegmentFile segmentFile = new JournalSegmentFile(file);
                ByteBuffer buffer = ByteBuffer.allocate(JournalSegmentDescriptor.BYTES);
                try (FileChannel channel = openChannel(file)) {
                    channel.read(buffer);
                    buffer.flip();
                } catch (IOException e) {
                    throw new StorageException(e);
                }
                JournalSegmentDescriptor descriptor = new JournalSegmentDescriptor(buffer);
                JournalSegment<E> segment = newSegment(new JournalSegmentFile(file), descriptor);
                // Add the segment to the segments list.
                log.debug("Found segment: {} ({})", segment.descriptor().id(), segmentFile.file().getName());
                segments.put(descriptor.index(), segment);
            }
        }
        return segments.values();
    }

    private FileChannel openChannel(File file) {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    @Override
    public void close() {
        segments.values().forEach(journalSegment -> {
            journalSegment.close();
        });
        writer.close();
        this.open = false;
    }
}
