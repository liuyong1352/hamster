package io.hamster.storage.journal;

import io.hamster.storage.StorageException;
import io.hamster.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Collection;
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
        }
    }

    private JournalSegment<E> createSegment(JournalSegmentDescriptor descriptor){
        File file = JournalSegmentFile.createSegmentFile(name,directory,descriptor.id());
        JournalSegmentFile journalSegmentFile = new JournalSegmentFile(file);
        JournalSegment<E> journalSegment = new JournalSegment<E>(journalSegmentFile,descriptor);
        segments.put(descriptor.index(),journalSegment);
        return journalSegment;
    }

    @Override
    public SegmentedJournalWriter<E> writer() {
        return writer;
    }

    @Override
    public JournalReader<E> openReader(long index) {
        return null;
    }

    @Override
    public JournalReader<E> openReader(long index, JournalReader.Mode mode) {
        return null;
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
                JournalSegment<E> segment = new JournalSegment<>(new JournalSegmentFile(file), descriptor);
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
    public void close(){
        segments.values().forEach(journalSegment -> {
            journalSegment.close();
        });
        writer.close();
        this.open = false;
    }
}
