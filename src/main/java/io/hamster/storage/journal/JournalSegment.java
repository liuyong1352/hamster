package io.hamster.storage.journal;

import io.hamster.storage.StorageException;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.base.Preconditions.checkState;

public class JournalSegment<E> implements AutoCloseable {

    private final JournalSegmentFile file;
    private final JournalSegmentDescriptor descriptor;
    private final JournalCodec<E> codec;


    private final MappableJournalSegmentWriter<E> writer;
    private final AtomicInteger references = new AtomicInteger();
    private boolean open = true;

    public JournalSegment(
            JournalSegmentFile file,
            JournalSegmentDescriptor descriptor,
            JournalCodec<E> codec,
            int maxEntrySize
            ) {
        this.file = file;
        this.descriptor = descriptor;
        this.codec = codec;
        this.writer = new MappableJournalSegmentWriter<>(openChannel(file.file()), this,codec,maxEntrySize);
    }

    @Override
    public void close(){
        this.writer.close();
        this.open = false;
    }

    /**
     * Returns the segment writer.
     *
     * @return The segment writer.
     */
    public MappableJournalSegmentWriter<E> writer() {
        checkOpen();
        return writer;
    }


    public void acquire(){
        references.incrementAndGet();
    }

    private FileChannel openChannel(File file) {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Returns the segment descriptor.
     *
     * @return The segment descriptor.
     */
    public JournalSegmentDescriptor descriptor() {
        return descriptor;
    }

    /**
     * Returns the segment's starting index.
     *
     * @return The segment's starting index.
     */
    public long index() {
        return descriptor.index();
    }

    /**
     * Checks whether the segment is open.
     */
    private void checkOpen() {
        checkState(open, "Segment not open");
    }


}
