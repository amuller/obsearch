package org.ajmm.obsearch.storage.bdb;

import hep.aida.bin.StaticBin1D;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import net.obsearch.utils.bytes.ByteConversion;

import org.ajmm.obsearch.Status;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.index.utils.ByteArrayComparator;
import org.ajmm.obsearch.storage.CloseIterator;
import org.ajmm.obsearch.storage.OBStore;
import org.ajmm.obsearch.storage.Tuple;
import org.ajmm.obsearch.storage.TupleBytes;

import com.sleepycat.bind.tuple.SortedDoubleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Sequence;
import com.sleepycat.je.SequenceConfig;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * BDBOBStore is a storage abstraction for Berkeley DB. It is designed to work
 * on byte array keys storing byte array values.
 * @author Arnoldo Jose Muller Molina
 */

public abstract class AbstractBDBOBStore < T extends Tuple > implements
        OBStore < T > {

    private static ByteArrayComparator comp = new ByteArrayComparator();

    protected StaticBin1D stats = new StaticBin1D();

    /**
     * Berkeley DB database.
     */
    protected Database db;

    /**
     * Database for sequences.
     */
    protected Database sequence;

    /**
     * Sequence counter
     */
    protected Sequence counter;

    /**
     * Name of the database.
     */
    private String name;

    /**
     * If this storage system accepts duplicates or not.
     */
    private boolean duplicates;

    /**
     * Builds a new Storage system by receiving a Berkeley DB database.
     * @param db
     *                The database to be stored.
     * @param name
     *                Name of the database.
     * @param sequences
     *                Database used to store sequences.
     * @throws DatabaseException
     *                 if something goes wrong with the database.
     */
    public AbstractBDBOBStore(String name, Database db, Database sequences)
            throws DatabaseException {
        this.db = db;
        this.name = name;
        this.duplicates = db.getConfig().getSortedDuplicates();
        this.sequence = sequences;
        // initialize sequences
        SequenceConfig config = new SequenceConfig();
        config.setAllowCreate(true);
        DatabaseEntry key = new DatabaseEntry((name + "_seq").getBytes());
        if (!duplicates) {
            this.counter = sequence.openSequence(null, key, config);
        }
    }

    public void close() throws OBStorageException {
        try {
            db.close();
            sequence.close();
        } catch (DatabaseException d) {
            throw new OBStorageException(d);
        }
    }

    public org.ajmm.obsearch.OperationStatus delete(byte[] key)
            throws OBStorageException {
        org.ajmm.obsearch.OperationStatus r = new org.ajmm.obsearch.OperationStatus();
        try {
            OperationStatus res = db.delete(null, new DatabaseEntry(key));
            if (res.NOTFOUND == res) {
                r.setStatus(Status.NOT_EXISTS);
            } else if (res.SUCCESS == res) {
                r.setStatus(Status.OK);
            } else {
                assert false;
            }
        } catch (Exception e) {
            throw new OBStorageException(e);
        }
        return r;
    }

    public void deleteAll() throws OBStorageException {
        try {
            db.getEnvironment().truncateDatabase(null, db.getDatabaseName(),
                    false);
        } catch (DatabaseException d) {
            throw new OBStorageException(d);
        }
    }

    public String getName() {
        return this.name;
    }

    public ByteBuffer getValue(byte[] key) throws IllegalArgumentException,
            OBStorageException {
        if (duplicates) {
            throw new IllegalArgumentException();
        }
        DatabaseEntry search = new DatabaseEntry(key);
        DatabaseEntry value = new DatabaseEntry();
        try {
            OperationStatus res = db.get(null, search, value, null);
            if (res == OperationStatus.SUCCESS) {
                if (this.stats != null) {
                    stats.add(value.getData().length);
                }
                return ByteConversion.createByteBuffer(value.getData());
            } else {
                return null;
            }
        } catch (DatabaseException e) {
            throw new OBStorageException(e);
        }
    }

    public org.ajmm.obsearch.OperationStatus put(byte[] key, ByteBuffer value)
            throws OBStorageException {

        DatabaseEntry k = new DatabaseEntry(key);
        DatabaseEntry v = new DatabaseEntry(value.array());
        org.ajmm.obsearch.OperationStatus res = new org.ajmm.obsearch.OperationStatus();
        try {
            OperationStatus r = db.put(null, k, v);
            if (r == OperationStatus.SUCCESS) {
                res.setStatus(Status.OK);
            } // Result() is always initialized with error.
        } catch (DatabaseException e) {
            throw new OBStorageException(e);
        }
        return res;
    }

    public boolean allowsDuplicatedData() {
        return duplicates;
    }

    /**
     * Base class used to iterate over cursors.
     * @param <O>
     *                The type of tuple that will be returned by the iterator.
     */
    protected abstract class CursorIterator < T > implements CloseIterator < T > {

        protected Cursor cursor;

        private boolean cursorClosed = false;

        protected DatabaseEntry keyEntry = new DatabaseEntry();

        protected DatabaseEntry dataEntry = new DatabaseEntry();

        /**
         * Previous key entry
         */
        protected DatabaseEntry prevKeyEntry = null;

        /**
         * Previous data entry
         */
        protected DatabaseEntry prevDataEntry = null;

        protected OperationStatus retVal;

        private T next = null;

        private byte[] max;

        private byte[] current;

        private boolean full;

        /**
         * Creates a cursor iterator in full mode.
         * @throws OBStorageException
         */
        protected CursorIterator() throws OBStorageException {
            this(null, null, true);
        }

        public CursorIterator(byte[] min, byte[] max) throws OBStorageException {
            this(min, max, false);
        }

        protected CursorIterator(byte[] min, byte[] max, boolean full)
                throws OBStorageException {
            this.max = max;
            this.current = min;
            this.full = full;
            try {
                CursorConfig config = new CursorConfig();
                config.setReadUncommitted(true);
                this.cursor = db.openCursor(null, config);
                keyEntry.setData(current);
                if (!full) {
                    retVal = cursor
                            .getSearchKeyRange(keyEntry, dataEntry, null);
                } else {
                    retVal = cursor.getFirst(keyEntry, dataEntry, null);
                }

            } catch (DatabaseException e) {
                throw new OBStorageException(e);
            }
            loadNext();
        }

        public boolean hasNext() {
            return next != null;
        }

        /**
         * Loads data from keyEntry and dataEntry and puts it into next. If we
         * go beyond max, we set next to null so that everybody will work
         * properly.
         */
        private void loadNext() throws NoSuchElementException {
            if (retVal == OperationStatus.SUCCESS) {
                current = keyEntry.getData();

                int c = -1; // full mode
                if (!full) {
                    c = comp.compare(current, max);
                }
                if (c <= 0) {
                    next = createTuple(current, ByteConversion
                            .createByteBuffer(dataEntry.getData()));
                    stats.add(dataEntry.getData().length);
                } else { // end of the loop
                    next = null;
                    // close the cursor
                    // closeCursor();
                }
            } else { // we are done
                next = null;
                // close the cursor
                // closeCursor();
            }
        }

        /**
         * Creates a tuple from the given key and value.
         * @param key
         *                raw key.
         * @param value
         *                raw value.
         * @return A new tuple of type T created from the raw data key and
         *         value.
         */
        protected abstract T createTuple(byte[] key, ByteBuffer value);

        public T next() {
            synchronized (keyEntry) {
                if (next == null) {
                    throw new NoSuchElementException(
                            "You tried to access an iterator with no next elements");
                }
                T res = next;
                try {
                    prevKeyEntry = keyEntry;
                    prevDataEntry = dataEntry;
                    retVal = cursor.getNext(keyEntry, dataEntry, null);
                } catch (DatabaseException e) {
                    throw new NoSuchElementException("Berkeley DB's error: "
                            + e.getMessage());
                }
                // get the next elements.
                loadNext();
                return res;
            }
        }

        public void closeCursor() throws OBException {
            try {
                synchronized (cursor) {
                    if (!cursorClosed) {
                        cursor.close();
                        cursorClosed = true;
                    }
                }
            } catch (DatabaseException e) {
                throw new NoSuchElementException(
                        "Could not close the internal cursor");
            }
        }

        /**
         * Currently not supported. To be supported in the future.
         */
        public void remove() {
            try {
                // double x1 = SortedDoubleBinding.entryToDouble(keyEntry);
                OperationStatus ret = null; // cursor.getPrev(keyEntry,
                // dataEntry, null);
                // double x = SortedDoubleBinding.entryToDouble(keyEntry);
                if (this.retVal !=  OperationStatus.SUCCESS) {
                    Cursor c = db.openCursor(null, null);
                    ret = c.getLast(keyEntry, dataEntry, null);
                    if (ret != OperationStatus.SUCCESS) {
                        throw new NoSuchElementException();
                    }
                    ret = c.delete();                    
                    c.close();
                } else {

                    ret = cursor.getPrev(keyEntry, dataEntry, null);
                    if (ret != OperationStatus.SUCCESS) {
                        throw new NoSuchElementException();
                    }
                    ret = cursor.delete();
                }

                if (ret != OperationStatus.SUCCESS) {
                    throw new NoSuchElementException();
                }

            } catch (DatabaseException e) {
                throw new IllegalArgumentException(e);
            }
        }

        /*
         * public void finalize() throws Throwable { try { closeCursor(); }
         * finally { super.finalize(); } }
         */

    }

    public long size() throws OBStorageException {
        long res;

        try {
            res = db.count();
        } catch (DatabaseException e) {
            throw new OBStorageException(e);
        }
        return res;
    }

    /**
     * Returns the next id from the database (incrementing sequences).
     * @return The next id that can be inserted.
     */
    public long nextId() throws OBStorageException {
        long res;
        try {
            res = this.counter.get(null, 1);
        } catch (DatabaseException e) {
            throw new OBStorageException(e);
        }
        return res;
    }

    @Override
    public StaticBin1D getReadStats() {
        return this.stats;
    }

    @Override
    public void setReadStats(StaticBin1D stats) {
        this.stats = stats;
    }

    /**
     * @see org.ajmm.obsearch.storage.OBStore#processRangeRaw(byte[], byte[])
     */
    @Override
    public CloseIterator < TupleBytes > processRangeRaw(byte[] low, byte[] high)
            throws OBStorageException {
        return new ByteArrayIterator(low, high);
    }

    /**
     * Iterator used to process range results.
     */
    /*
     * TODO: I am leaving the closing of the cursor to the last iteration or the
     * finalize method (whichever happens first). We should test if this is ok,
     * or if there is an issue with this because Berkeley's iterator explicitly
     * have a "close" method.
     */
    protected class ByteArrayIterator
            extends CursorIterator < TupleBytes > {

        protected ByteArrayIterator() throws OBStorageException {
            super();
        }

        protected ByteArrayIterator(byte[] min, byte[] max)
                throws OBStorageException {
            super(min, max);
        }

        protected ByteArrayIterator(byte[] min, byte[] max, boolean full)
                throws OBStorageException {
            super(min, max, full);
        }

        @Override
        protected TupleBytes createTuple(byte[] key, ByteBuffer value) {
            return new TupleBytes(key, value);
        }
    }

}
