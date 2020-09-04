package io.hamster.storage.journal;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.Message;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.zip.CRC32;

public class MappedJournalSegmentWriter<E extends Message> implements JournalWriter<E> {

    private final MappedByteBuffer mappedBuffer;
    private final ByteBuffer buffer;

    MappedJournalSegmentWriter(MappedByteBuffer buffer) {
        this.mappedBuffer = buffer;
        this.buffer = buffer.slice();
    }

    @Override
    public long getLastIndex() {
        return 0;
    }

    @Override
    public Indexed<E> getLastEntry() {
        return null;
    }

    @Override
    public long getNextIndex() {
        return 0;
    }

    @Override
    public <T extends E> Indexed<T> append(T entry) {

        // Serialize the entry.
        int position = buffer.position();
        if (position + Integer.BYTES + Integer.BYTES > buffer.limit()) {
            throw new BufferOverflowException();
        }

        int length = entry.getSerializedSize();

        // Compute the checksum for the entry.
        final CRC32 crc32 = new CRC32();
        ByteBuffer slice = buffer.slice();
        slice.limit(length);
        crc32.update(slice);
        final long checksum = crc32.getValue();

        CodedOutputStream codedOutputStream = CodedOutputStream.newInstance(buffer);
        try {
            entry.writeTo(codedOutputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void append(Indexed<E> entry) {
        final long nextIndex = getNextIndex();



    }

    @Override
    public void commit(long index) {

    }

    @Override
    public void reset(long index) {

    }

    @Override
    public void truncate(long index) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
