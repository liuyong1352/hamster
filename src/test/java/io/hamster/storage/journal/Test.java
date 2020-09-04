package io.hamster.storage.journal;

import com.google.protobuf.Message;
import io.hamster.protocols.raft.storage.log.QueryEntry;
import io.hamster.protocols.raft.storage.log.RaftLogEntry;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

public class Test {


    @org.junit.Test
    public void t(){
        ByteBuffer buffer = ByteBuffer.allocate(6);
        buffer.put("sss".getBytes());
        int len = buffer.limit();
        // Compute the checksum for the entry.
        final CRC32 crc32 = new CRC32();
        ByteBuffer slice = buffer.slice();
        slice.limit(len);
        crc32.update(slice);
        final long checksum = crc32.getValue();


        RaftLogEntry logEntry = RaftLogEntry.newBuilder()
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setQuery(QueryEntry.newBuilder().build())
                .build();

        File file = JournalSegmentFile.createSegmentFile("foo", new File(System.getProperty("user.dir")), 1);
        JournalSegmentFile journalSegmentFile = new JournalSegmentFile(file);
        JournalSegment journalSegment = new JournalSegment(journalSegmentFile);
        MappableJournalSegmentWriter writer = journalSegment.writer();
        Indexed<Message> indexed = new Indexed(0,logEntry,logEntry.getSerializedSize());
        writer.append(indexed);
    }

}
