package io.hamster.storage.journal;

import com.google.protobuf.CodedOutputStream;
import io.hamster.protocols.raft.storage.log.RaftLogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;

public class RaftLogCodec implements JournalCodec<RaftLogEntry> {
    @Override
    public void encode(RaftLogEntry entry, ByteBuffer buffer) throws IOException {
        entry.writeTo(CodedOutputStream.newInstance(buffer));
    }

    @Override
    public RaftLogEntry decode(ByteBuffer buffer) throws IOException {
        return RaftLogEntry.parseFrom(buffer);
    }
}
