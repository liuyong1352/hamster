package io.hamster.storage.journal;

import java.util.NoSuchElementException;

public class SegmentedJournalReader<E> implements JournalReader<E> {

    private final SegmentedJournal<E> journal;
    private JournalSegment<E> currentSegment;
    private MappableJournalSegmentReader<E> currentReader;
    private Indexed<E> previousEntry;
    private final Mode mode;

    public SegmentedJournalReader(SegmentedJournal<E> journal, long index, Mode mode) {
        this.journal = journal;
        this.mode = mode;
        initialize(index);
    }

    /**
     * Initializes the reader to the given index.
     */
    private void initialize(long index) {
        currentSegment = journal.getSegment(index);
        currentSegment.acquire();
        currentReader = currentSegment.createReader();
        long nextIndex = getNextIndex();
        while (index > nextIndex && hasNext()) {
            next();
            nextIndex = getNextIndex();
        }
    }


    @Override
    public long getFirstIndex() {
        return currentReader.getFirstIndex();
    }

    @Override
    public long getCurrentIndex() {
        return currentReader.getCurrentIndex();
    }

    @Override
    public Indexed<E> getCurrentEntry() {
        return currentReader.getCurrentEntry();
    }

    @Override
    public long getNextIndex() {
        return currentReader.getNextIndex();
    }

    @Override
    public boolean hasNext() {
        if (Mode.ALL == mode) {
            return hasNextEntry();
        }
        long nextIndex = getNextIndex();
        long commitIndex = journal.getCommitIndex();
        return (nextIndex <= commitIndex) && hasNextEntry();
    }

    private boolean hasNextEntry() {
        if (currentReader.hasNext()) {
            return true;
        }

        JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
        if (nextSegment != null && nextSegment.index() == getNextIndex()) {
            previousEntry = currentReader.getCurrentEntry();
            currentSegment.release();
            currentSegment = nextSegment;
            currentSegment.acquire();
            currentReader = currentSegment.createReader();

            return currentReader.hasNext();
        }
        return false;
    }


    @Override
    public Indexed<E> next() {
        if (!currentReader.hasNext()) {
            JournalSegment<E> nextSegment = journal.getNextSegment(currentSegment.index());
            if (nextSegment != null && nextSegment.index() == getNextIndex()) {
                previousEntry = currentReader.getCurrentEntry();
                currentSegment.release();
                currentSegment = nextSegment;
                currentSegment.acquire();
                currentReader = currentSegment.createReader();
                return currentReader.next();
            } else {
                throw new NoSuchElementException();
            }
        } else {
            previousEntry = currentReader.getCurrentEntry();
            return currentReader.next();
        }

    }

    @Override
    public void reset() {
        currentReader.reset();
    }

    @Override
    public void reset(long index) {
        currentReader.reset(index);
    }

    @Override
    public void close() {
        currentReader.close();
        journal.closeReader(this);
    }
}
