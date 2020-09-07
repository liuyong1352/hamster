package io.hamster.storage.journal;

import java.io.File;

public class SegmentedJournal<E> {

    private final File root;
    private final String name;
    private final int maxEntrySize;
    private JournalSegment<E> current;

    public SegmentedJournal(
            String name,
            File root,
            int maxEntrySize) {
        this.name = name;
        this.root = root;
        this.maxEntrySize = maxEntrySize;
        open();
    }

    private void open(){
        for(File file : root.listFiles()){
            if(JournalSegmentFile.isSegmentFile(name,file)){
                JournalSegmentFile journalSegmentFile = new JournalSegmentFile(file);
                JournalSegment<E> journalSegment = new JournalSegment<E>(journalSegmentFile,null);
            }

        }
    }
}
