package io.hamster.storage.journal;

import com.google.protobuf.ByteString;
import io.hamster.protocols.raft.storage.log.QueryEntry;
import io.hamster.protocols.raft.storage.log.RaftLogCodec;
import io.hamster.protocols.raft.storage.log.RaftLogEntry;
import io.hamster.storage.StorageLevel;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class Test {


    @org.junit.Test
    public void t() {


        SegmentedJournal<RaftLogEntry> segmentedJournal = new SegmentedJournal<>("test",
                StorageLevel.MAPPED,
                new File(System.getProperty("user.dir")),
                new RaftLogCodec(), 1024 * 1024,
                1024);


        SegmentedJournalWriter<RaftLogEntry> writer1 = segmentedJournal.writer();

        long next = writer1.getNextIndex();

        RaftLogEntry logEntry = RaftLogEntry.newBuilder()
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setQuery(QueryEntry.newBuilder()
                        .setValue(ByteString.copyFrom("中国", Charset.defaultCharset()))
                        .build())
                .build();

        Indexed<RaftLogEntry> indexed = new Indexed(next, logEntry, logEntry.getSerializedSize());

        writer1.append(indexed);

        SegmentedJournalReader<RaftLogEntry> reader = segmentedJournal.openReader(1);

        Indexed<RaftLogEntry> indexed1 = reader.next();
        System.out.println(indexed1.entry().getQuery().getValue().toString(Charset.defaultCharset()));
        System.out.println(indexed1);
    }

}
