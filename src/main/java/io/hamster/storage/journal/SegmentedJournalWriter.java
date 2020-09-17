package io.hamster.storage.journal;

import java.nio.BufferOverflowException;

public class SegmentedJournalWriter<E> implements JournalWriter<E> {

    private final SegmentedJournal<E> journal;
    private JournalSegment<E> currentSegment;
    private MappableJournalSegmentWriter<E> currentWriter;

    public SegmentedJournalWriter(SegmentedJournal<E> journal) {
        this.journal = journal;
        this.currentSegment = journal.getLastSegment();
        currentSegment.acquire();
        this.currentWriter = currentSegment.writer();
    }

    @Override
    public long getLastIndex() {
        return currentWriter.getLastIndex();
    }

    @Override
    public Indexed<E> getLastEntry() {
        return currentWriter.getLastEntry();
    }

    @Override
    public long getNextIndex() {
        return currentWriter.getNextIndex();
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {
        try {
            return currentWriter.append(entry);
        } catch (BufferOverflowException e) {
            //First entry can not write , the entry size is too large
            if (currentSegment.index() == currentWriter.getNextIndex()) {
                throw e;
            }
            currentWriter.flush();
            currentSegment.release();
            currentSegment = journal.getNextSegment();
            currentSegment.acquire();
            currentWriter = currentSegment.writer();
            return currentWriter.append(entry);
        }
    }

    @Override
    public void append(Indexed<E> entry) {
        try {
            currentWriter.append(entry);
        } catch (BufferOverflowException e) {
            //First entry can not write , the entry size is too large
            if (currentSegment.index() == currentWriter.getNextIndex()) {
                throw e;
            }
            currentWriter.flush();
            currentSegment.release();
            currentSegment = journal.getNextSegment();
            currentSegment.acquire();
            currentWriter = currentSegment.writer();
            currentWriter.append(entry);
        }
    }

    @Override
    public void commit(long index) {
        currentWriter.commit(index);
    }

    @Override
    public void reset(long index) {
        currentWriter.reset(index);
    }

    @Override
    public void truncate(long index) {
        currentWriter.truncate(index);
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {
        currentWriter.close();
    }
}
