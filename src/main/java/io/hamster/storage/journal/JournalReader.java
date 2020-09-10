package io.hamster.storage.journal;

import java.util.Iterator;

public interface JournalReader<E> extends Iterator<Indexed<E>>, AutoCloseable {

    /**
     * Raft log reader mode.
     */
    enum Mode {

        /**
         * Reads all entries from the log.
         */
        ALL,

        /**
         * Reads committed entries from the log.
         */
        COMMITS,
    }
}
