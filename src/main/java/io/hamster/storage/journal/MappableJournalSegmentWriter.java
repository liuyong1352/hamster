package io.hamster.storage.journal;

import com.google.protobuf.Message;

import java.nio.channels.FileChannel;

public class MappableJournalSegmentWriter<E extends Message> implements JournalWriter<E> {

    private final FileChannel fileChannel;
    private final JournalSegment<E> journalSegment;
    private JournalWriter<E> writer;

    public MappableJournalSegmentWriter(FileChannel fileChannel, JournalSegment<E> journalSegment) {
        this.journalSegment = journalSegment;
        this.fileChannel = fileChannel;
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
    public void append(Indexed<E> entry) {

    }

    @Override
    public void close() throws Exception {

    }
}
