package io.hamster.storage.journal;

import com.google.protobuf.Message;
import io.hamster.storage.StorageException;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static com.google.common.base.Preconditions.checkState;

public class JournalSegment<E extends Message> implements AutoCloseable {

    private boolean open = true;
    private final MappableJournalSegmentWriter<E> writer;

    public JournalSegment(JournalSegmentFile file) {
        this.writer = new MappableJournalSegmentWriter<>(openChannel(file.file()), this);
    }

    @Override
    public void close() throws Exception {

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

    private FileChannel openChannel(File file) {
        try {
            return FileChannel.open(file.toPath(), StandardOpenOption.CREATE,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException e) {
            throw new StorageException(e);
        }
    }

    /**
     * Checks whether the segment is open.
     */
    private void checkOpen() {
        checkState(open, "Segment not open");
    }

}
