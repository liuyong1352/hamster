package io.hamster.storage.journal;

import io.hamster.protocols.raft.storage.log.QueryEntry;
import io.hamster.protocols.raft.storage.log.RaftLogCodec;
import io.hamster.protocols.raft.storage.log.RaftLogEntry;
import io.hamster.storage.StorageLevel;

import java.io.File;

public class Test {


    @org.junit.Test
    public void t() {
        /*ByteBuffer buffer = ByteBuffer.allocate(6);
        buffer.put("sss".getBytes());
        int len = buffer.limit();
        // Compute the checksum for the entry.
        final CRC32 crc32 = new CRC32();
        ByteBuffer slice = buffer.slice();
        slice.limit(len);
        crc32.update(slice);
        final long checksum = crc32.getValue();*/


        RaftLogEntry logEntry = RaftLogEntry.newBuilder()
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setQuery(QueryEntry.newBuilder().build())
                .build();

        File file = JournalSegmentFile.createSegmentFile("foo", new File(System.getProperty("user.dir")), 1);
        JournalSegmentFile journalSegmentFile = new JournalSegmentFile(file);

        JournalSegmentDescriptor journalSegmentDescriptor = JournalSegmentDescriptor.builder()
                .withId(1)
                .withIndex(0)
                .build();

        Indexed<RaftLogEntry> indexed = new Indexed(0, logEntry, logEntry.getSerializedSize());

      /*  JournalSegment journalSegment = new JournalSegment(journalSegmentFile,journalSegmentDescriptor);
        MappableJournalSegmentWriter writer = journalSegment.writer();
        Indexed<RaftLogEntry> indexed = new Indexed(0,logEntry,logEntry.getSerializedSize());
        writer.append(indexed);*/


        SegmentedJournal<RaftLogEntry> segmentedJournal = new SegmentedJournal<>("test",
                StorageLevel.MAPPED,
                new File(System.getProperty("user.dir")),
                new RaftLogCodec(), 1024 * 1024,
                1024);

        SegmentedJournalWriter<RaftLogEntry> writer1 = segmentedJournal.writer();
        writer1.append(indexed);
    }

}
