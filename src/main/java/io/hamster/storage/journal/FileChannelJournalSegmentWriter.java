package io.hamster.storage.journal;

import com.google.protobuf.Message;
import io.hamster.storage.journal.index.JournalIndex;

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
class FileChannelJournalSegmentWriter<E extends Message> implements JournalWriter<E>  {

    private final FileChannel channel;
    private final JournalSegment segment;
    private final int maxEntrySize;
    private final JournalIndex index;
    //private final ByteBuffer memory;
    private final long firstIndex;
    private Indexed<E> lastEntry;

    FileChannelJournalSegmentWriter(
            FileChannel channel,
            JournalSegment segment,
            int maxEntrySize,
            JournalIndex index
    ){
        this.channel = channel;
        this.segment = segment;
        this.maxEntrySize = maxEntrySize;
        this.index = index;
        this.firstIndex = segment.index();
        //this.memory = ByteBuffer.allocate();
    }

    @Override
    public long getLastIndex() {
        return 0;
    }

    @Override
    public Indexed<E> getLastEntry() {
        return null;
    }

    @Override
    public long getNextIndex() {
        return 0;
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {
        return null;
    }

    @Override
    public void append(Indexed<E> entry) {

    }

    @Override
    public void commit(long index) {

    }

    @Override
    public void reset(long index) {

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
