package io.hamster.raft.storage.log;

import io.hamster.protocols.raft.storage.log.QueryEntry;
import io.hamster.protocols.raft.storage.log.RaftLogEntry;

public class Test {
    public static void main(String[] args) {
        RaftLogEntry logEntry = RaftLogEntry.newBuilder()
                .setTerm(1)
                .setTimestamp(System.currentTimeMillis())
                .setQuery(QueryEntry.newBuilder().build())
                .build();
        System.out.println(logEntry);
    }
}
