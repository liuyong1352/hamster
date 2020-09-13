package io.hamster.storage.journal;

import io.hamster.storage.StorageException;
import io.hamster.storage.journal.index.JournalIndex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;


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
        if (lastEntry != null) {
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

        final int length = memory.limit() - (Integer.BYTES + Integer.BYTES);
        final CRC32 crc32 = new CRC32();
        crc32.update(memory.array(), Integer.BYTES + Integer.BYTES, length);
        final long checkSum = crc32.getValue();
        try {
            memory.putInt(0, length);
            memory.putInt(Integer.BYTES, (int) checkSum);
            channel.write(memory);

            // Update the last entry with the correct index/term/length.
            Indexed<E> indexedEntry = new Indexed<>(index, entry, length);
            this.lastEntry = indexedEntry;
            return (Indexed<T>) indexedEntry;
        } catch (IOException e) {
            throw new StorageException(e);
        }
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
        long nextIndex = firstIndex;
        try {
            channel.position(JournalSegmentDescriptor.BYTES);

            memory.clear();
            memory.flip();
            // Record the current buffer position.
            long position = channel.position();

            if (memory.remaining() < maxEntrySize) {
                memory.clear();
                channel.read(memory);
                channel.position(position);
                memory.flip();

            }
            memory.mark();
            int length = memory.getInt();

            while (length > 0 && length <= maxEntrySize) {
                long checkSum = ((long)memory.getInt()) & 0xFFFFFFFFL;
                final CRC32 crc32 = new CRC32();
                crc32.update(memory.array(),memory.position(),length);
                if(checkSum == crc32.getValue()){
                    int limit = memory.limit();
                    memory.limit(memory.position() + length);
                    E entry = codec.decode(memory);
                    Indexed<E> indexedEntry = new Indexed<>(firstIndex,entry,length);
                    memory.limit(limit);
                    this.lastEntry = indexedEntry;
                    nextIndex++;
                } else {
                    break;
                }
            }

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
