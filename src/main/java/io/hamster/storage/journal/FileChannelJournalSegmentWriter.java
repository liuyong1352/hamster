package io.hamster.storage.journal;

import io.hamster.storage.StorageException;
import io.hamster.storage.journal.index.JournalIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;


/**
 * Segment writer.
 * <p>
 * The format of an entry in the log is as follows:
 * <ul>
 * <li>64-bit index</li>
 * <li>8-bit boolean indicating whether a term change is contained in the entry</li>
 * <li>64-bit optional term</li>
 * <li>32-bit signed entry length, including the entry type ID</li>
 * <li>8-bit signed entry type ID</li>
 * <li>n-bit entry bytes</li>
 * </ul>
 */
class FileChannelJournalSegmentWriter<E> implements JournalWriter<E> {

    private final FileChannel channel;
    private final JournalSegment segment;
    private final int maxEntrySize;
    private final JournalIndex index;
    private final ByteBuffer memory;
    private final JournalCodec<E> codec;
    private final long firstIndex;
    private Indexed<E> lastEntry;

    FileChannelJournalSegmentWriter(
            FileChannel channel,
            JournalSegment segment,
            JournalCodec<E> codec,
            int maxEntrySize,
            JournalIndex index
    ) {
        this.channel = channel;
        this.segment = segment;
        this.maxEntrySize = maxEntrySize;
        this.codec = codec;
        this.index = index;
        this.firstIndex = segment.index();
        this.memory = ByteBuffer.allocate((maxEntrySize + Integer.BYTES + Integer.BYTES) * 2);
        reset(0);
    }

    @Override
    public long getLastIndex() {
        return 0;
    }

    @Override
    public Indexed<E> getLastEntry() {
        return lastEntry;
    }

    @Override
    public long getNextIndex() {
        if(lastEntry != null){
            return lastEntry.index() + 1;
        } else {
            return firstIndex;
        }
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {

        long index = getNextIndex();

        memory.clear();
        memory.position(Integer.BYTES + Integer.BYTES);

        try {
            codec.encode(entry, memory);
        } catch (Exception e) {
            throw new StorageException.TooLarge("Entry size exceeds maximum allowed bytes (" + maxEntrySize + ")");
        }

        memory.flip();
        int len = memory.limit() - 8;
        try {
            channel.write(memory);
        } catch (IOException e) {
            throw new StorageException(e);
        }
        return new Indexed<>(index,entry,len);
    }

    @Override
    public void append(Indexed<E> entry) {
        final long nextIndex = getNextIndex();

        // If the entry's index is greater than the next index in the segment, skip some entries.
        if (entry.index() > nextIndex) {
            throw new IndexOutOfBoundsException("Entry index is not sequential");
        }
        // If the entry's index is less than the next index in the segment ,truncate the segment
        if (entry.index() < nextIndex) {
            truncate(entry.index() - 1);
        }


        append(entry.entry());
    }

    @Override
    public void commit(long index) {

    }

    @Override
    public void reset(long index) {
        try {
            channel.position(JournalSegmentDescriptor.BYTES);
        } catch (IOException e) {
            throw new StorageException(e);
        }

    }

    @Override
    public void truncate(long index) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
