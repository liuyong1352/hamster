package io.hamster.storage.journal;

import io.hamster.storage.StorageLevel;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public abstract class AbstractJournalTest {

    private static final JournalCodec<TestEntry> CODEC = new JournalCodec<TestEntry>() {
        @Override
        public void encode(TestEntry entry, ByteBuffer buffer) throws IOException {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(entry);
            byte[] bytes = bos.toByteArray();
            buffer.putInt(bytes.length);
            buffer.put(bytes);
        }

        @Override
        public TestEntry decode(ByteBuffer buffer) throws IOException {
            try {
                byte[] bytes = new byte[buffer.getInt()];
                buffer.get(bytes);
                ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
                ObjectInputStream ois = new ObjectInputStream(bis);
                return (TestEntry) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    };

    protected static final TestEntry ENTRY = new TestEntry(32);
    private static final Path PATH = Paths.get("target/test-logs/");

    private final int maxSegmentSize;
    private final int maxEntryPerSegment;

    protected AbstractJournalTest(int maxSegmentSize) throws IOException {
        this.maxSegmentSize = maxSegmentSize;
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        CODEC.encode(ENTRY, buffer);
        buffer.flip();
        int entryLength = (buffer.remaining() + 8);
        this.maxEntryPerSegment = (maxSegmentSize - JournalSegmentDescriptor.BYTES) /entryLength;
    }

    protected abstract StorageLevel storageLevel();

    @Parameterized.Parameters
    public static Collection primeNumbers() throws IOException {
        ByteBuffer buffer = ByteBuffer.allocate(1024);
        CODEC.encode(ENTRY, buffer);
        buffer.flip();
        int entryLength = (buffer.remaining() + 8);

        List<Object[]> runs = new ArrayList<>();
        for (int i = 2; i <= 100; i++) {
            runs.add(new Object[]{64 + (i * (entryLength + 8))});
        }
        return runs;
    }

    protected SegmentedJournal<TestEntry> createJournal() {
        return SegmentedJournal.<TestEntry>builder()
                .withName("test")
                .withDirectory(PATH.toFile())
                .withCodec(CODEC)
                .withStorageLevel(storageLevel())
                .withMaxSegmentSize(maxSegmentSize)
                .withIndexDensity(.2)
                .build();
    }

    @Test
    public void testCloseMultipleTimes() {
        // given
        final Journal<TestEntry> journal = createJournal();

        // when
        journal.close();

        // then
        journal.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testWriteRead() throws Exception {
        try (Journal<TestEntry> journal = createJournal()) {
            JournalWriter<TestEntry> writer = journal.writer();
            JournalReader<TestEntry> reader = journal.openReader(1);

            // Append a couple entries.
            Indexed<TestEntry> indexed;
            assertEquals(1, writer.getNextIndex());
            indexed = writer.append(ENTRY);
            assertEquals(1, indexed.index());

            assertEquals(2, writer.getNextIndex());
            writer.append(new Indexed<>(2, ENTRY, 0));
            reader.reset(2);
            indexed = reader.next();
            assertEquals(2, indexed.index());
            assertFalse(reader.hasNext());

            // Test reading an entry
            Indexed<TestEntry> entry1;
            reader.reset();
            entry1 = (Indexed) reader.next();
            assertEquals(1, entry1.index());
            assertEquals(entry1, reader.getCurrentEntry());
            assertEquals(1, reader.getCurrentIndex());

            // Test reading a second entry
            Indexed<TestEntry> entry2;
            assertTrue(reader.hasNext());
            assertEquals(2, reader.getNextIndex());
            entry2 = (Indexed) reader.next();
            assertEquals(2, entry2.index());
            assertEquals(entry2, reader.getCurrentEntry());
            assertEquals(2, reader.getCurrentIndex());
            assertFalse(reader.hasNext());

            // Test opening a new reader and reading from the journal.
            reader = journal.openReader(1);
            assertTrue(reader.hasNext());
            entry1 = (Indexed) reader.next();
            assertEquals(1, entry1.index());
            assertEquals(entry1, reader.getCurrentEntry());
            assertEquals(1, reader.getCurrentIndex());
            assertTrue(reader.hasNext());

            assertTrue(reader.hasNext());
            assertEquals(2, reader.getNextIndex());
            entry2 = (Indexed) reader.next();
            assertEquals(2, entry2.index());
            assertEquals(entry2, reader.getCurrentEntry());
            assertEquals(2, reader.getCurrentIndex());
            assertFalse(reader.hasNext());

            // Reset the reader.
            reader.reset();

            // Test opening a new reader and reading from the journal.
            reader = journal.openReader(1);
            assertTrue(reader.hasNext());
            entry1 = (Indexed) reader.next();
            assertEquals(1, entry1.index());
            assertEquals(entry1, reader.getCurrentEntry());
            assertEquals(1, reader.getCurrentIndex());
            assertTrue(reader.hasNext());

            assertTrue(reader.hasNext());
            assertEquals(2, reader.getNextIndex());
            entry2 = (Indexed) reader.next();
            assertEquals(2, entry2.index());
            assertEquals(entry2, reader.getCurrentEntry());
            assertEquals(2, reader.getCurrentIndex());
            assertFalse(reader.hasNext());

            // Truncate the journal and write a different entry.
            writer.truncate(1);
            assertEquals(2, writer.getNextIndex());
            writer.append(new Indexed<>(2, ENTRY, 0));
            reader.reset(2);
            indexed = reader.next();
            assertEquals(2, indexed.index());

            // Reset the reader to a specific index and read the last entry again.
            reader.reset(2);

            assertNotNull(reader.getCurrentEntry());
            assertEquals(1, reader.getCurrentIndex());
            assertEquals(1, reader.getCurrentEntry().index());
            assertTrue(reader.hasNext());
            assertEquals(2, reader.getNextIndex());
            entry2 = (Indexed) reader.next();
            assertEquals(2, entry2.index());
            assertEquals(entry2, reader.getCurrentEntry());
            assertEquals(2, reader.getCurrentIndex());
            assertFalse(reader.hasNext());
        }
    }

    @Test
    public void testResetTruncateZero() throws Exception {
        try (SegmentedJournal<TestEntry> journal = createJournal()) {
            JournalWriter<TestEntry> writer = journal.writer();
            JournalReader<TestEntry> reader = journal.openReader(1);

            assertEquals(0, writer.getLastIndex());
            writer.append(ENTRY);
            writer.append(ENTRY);
            writer.reset(1);
            assertEquals(0, writer.getLastIndex());
            writer.append(ENTRY);
            assertEquals(1, reader.next().index());
            writer.reset(1);
            assertEquals(0, writer.getLastIndex());
            writer.append(ENTRY);
            assertEquals(1, writer.getLastIndex());
            assertEquals(1, writer.getLastEntry().index());

            assertTrue(reader.hasNext());
            assertEquals(1, reader.next().index());

            writer.truncate(0);
            assertEquals(0, writer.getLastIndex());
            assertNull(writer.getLastEntry());
            writer.append(ENTRY);
            assertEquals(1, writer.getLastIndex());
            assertEquals(1, writer.getLastEntry().index());

            assertTrue(reader.hasNext());
            assertEquals(1, reader.next().index());
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testWriteReadEntries() throws Exception {
        try (Journal<TestEntry> journal = createJournal()) {
            JournalWriter<TestEntry> writer = journal.writer();
            JournalReader<TestEntry> reader = journal.openReader(1);

            for (int i = 1; i <= maxEntryPerSegment; i++) {
                writer.append(ENTRY);
                assertTrue(reader.hasNext());
                Indexed<TestEntry> entry;
                entry = reader.next();
                assertEquals(i, entry.index());
                assertEquals(32, entry.entry().bytes().length);
                reader.reset(i);
                entry = reader.next();
                assertEquals(i, entry.index());
                assertEquals(32, entry.entry().bytes().length);

                if (i > 6) {
                    reader.reset(i - 5);
                    assertNotNull(reader.getCurrentEntry());
                    assertEquals(i - 6, reader.getCurrentIndex());
                    assertEquals(i - 6, reader.getCurrentEntry().index());
                    assertEquals(i - 5, reader.getNextIndex());
                    reader.reset(i + 1);
                }

                writer.truncate(i - 1);
                writer.append(ENTRY);

                reader.reset(i);
                assertTrue(reader.hasNext());
                entry = reader.next();
                assertEquals(i, entry.index());
                assertEquals(32, entry.entry().bytes().length);
            }
        }
    }

    @Before
    @After
    public void cleanupStorage() throws IOException {
        if (Files.exists(PATH)) {
            Files.walkFileTree(PATH, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

}
