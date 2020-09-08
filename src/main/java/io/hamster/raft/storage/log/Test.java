package io.hamster.raft.storage.log;

import io.hamster.protocols.raft.storage.log.QueryEntry;
import io.hamster.protocols.raft.storage.log.RaftLogCodec;
import io.hamster.protocols.raft.storage.log.RaftLogEntry;

import java.nio.ByteBuffer;

public class Test {
    public static void main(String[] args) throws Exception{
        RaftLogEntry logEntry = RaftLogEntry.newBuilder()
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setQuery(QueryEntry.newBuilder().build())
                .build();

        RaftLogCodec codec = new RaftLogCodec();
        ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
        codec.encode(logEntry,byteBuffer);

        byteBuffer.flip();
        RaftLogEntry newEntry = codec.decode(byteBuffer);

        System.out.println(logEntry.equals(newEntry));
    }
}
