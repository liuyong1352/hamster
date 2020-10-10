package io.hamster.protocols.raft.storage.log;

import io.hamster.storage.journal.DelegatingJournalReader;
import io.hamster.storage.journal.JournalReader;

public class RaftLogReader extends DelegatingJournalReader<RaftLogEntry> {

    public RaftLogReader(JournalReader<RaftLogEntry> delegate) {
        super(delegate);
    }
}
