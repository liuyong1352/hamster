package io.hamster.storage.journal;

import java.nio.ByteBuffer;

public class Test {


    @org.junit.Test
    public void t() throws Exception {

        TestEntry testEntry = new TestEntry(1024 - 64 - 8 - 4);
        TestEntryCodec codec = new TestEntryCodec();
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        codec.encode(testEntry, buffer);

        int len = buffer.position() + 8;

        Journal<TestEntry> journal = SegmentedJournal.<TestEntry>builder()
                .withIndexDensity(0.2)
                .withMaxEntrySize(1024)
                .withMaxSegmentSize(1024)
                .withCodec(new TestEntryCodec())
                .build();


        JournalWriter<TestEntry> writer1 = journal.writer();
        JournalReader<TestEntry> reader = journal.openReader(1);

        writer1.append(testEntry);

        System.out.println(writer1.getNextIndex());


        /*int n = 5 ;
        for(int i=0;i<n ;i++){
            long next = writer1.getNextIndex();

            RaftLogEntry logEntry = RaftLogEntry.newBuilder()
                    .setTerm(1)
                    .setTimestamp(System.currentTimeMillis())
                    .setQuery(QueryEntry.newBuilder()
                            .setValue(ByteString.copyFrom("中国" + next, Charset.defaultCharset()))
                            .build())
                    .build();

            Indexed<RaftLogEntry> indexed = new Indexed(next, logEntry, logEntry.getSerializedSize());
            writer1.append(indexed);
            System.out.println(next);
        }

        while (reader.hasNext()){
            Indexed<RaftLogEntry> indexed1 = reader.next();
            System.out.println(indexed1.entry().getQuery().getValue().toString(Charset.defaultCharset()));
            System.out.println(indexed1);
        }*/
    }

}
