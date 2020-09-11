package io.hamster.storage.journal;

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
        return currentWriter.append(entry);
    }

    @Override
    public void append(Indexed<E> entry) {
        currentWriter.append(entry);
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
    public void close(){

    }
}
