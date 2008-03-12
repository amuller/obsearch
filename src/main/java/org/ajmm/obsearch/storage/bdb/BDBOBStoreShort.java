package org.ajmm.obsearch.storage.bdb;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.storage.OBStoreShort;
import org.ajmm.obsearch.storage.TupleShort;

import com.sleepycat.bind.tuple.ShortBinding;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.OperationStatus;

public final class BDBOBStoreShort
        extends BDBOBStore implements OBStoreShort {

    /**
     * Builds a new Storage system by receiving a Berkeley DB database that uses
     * shorts as a primary indexing method.
     * @param db
     *                The database to be stored.
     * @param name
     *                Name of the database.
     * @throws DatabaseException
     *                 if something goes wrong with the database.
     */
    public BDBOBStoreShort(String name, Database db) throws DatabaseException {
        super(name, db);
    }

    public Result delete(short key) throws OBStorageException {
        return super.delete(getBytes(key));
    }

    /**
     * Converts the given value to an array of bytes.
     * @param value
     *                the value to be converted.
     * @return An array of bytes with the given value encoded.
     */
    private byte[] getBytes(short value) {
        TupleOutput out = new TupleOutput();
        out.writeShort(value);
        return out.getBufferBytes();
    }

    /**
     * Loads the given value to a DatabaseEntry entry
     * @param value
     *                The value to load
     * @param entry
     *                The place where we will put the entry.
     */
    private void loadIntoEntry(short value, DatabaseEntry entry) {
        ShortBinding.shortToEntry(value, entry);
    }

    /**
     * Converts the value of the given entry into its primitive type.
     * @param entry
     *                The place where we will put the entry.
     */
    private short entryToValue(DatabaseEntry entry) {
        return ShortBinding.entryToShort(entry);
    }

    public byte[] getValue(short key) throws IllegalArgumentException,
            OBStorageException {
        return super.getValue(getBytes(key));
    }

    public Result put(short key, byte[] value) throws IllegalArgumentException,
            OBStorageException {
        return super.put(getBytes(key), value);
    }

    public Iterator < TupleShort > processRange(short low, short high)
            throws OBStorageException {
        return new ShortIterator(low, high);
    }

    /**
     * Iterator used to process range results.
     */
    /*
     * TODO: I am leaving the closing of the cursor to the last iteration or the
     * finalize method (whichever happens first). We should have
     */
    final class ShortIterator extends CursorIterator < TupleShort > {
        
        private TupleShort next = null;

        private short max;

        private short current;

        private ShortIterator(short min, short max) throws OBStorageException {
            this.max = max;
            this.current = min;
            try {
                this.cursor = db.openCursor(null, null);
                loadIntoEntry(current, keyEntry);
                retVal = cursor.getSearchKeyRange(keyEntry, dataEntry, null);
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
                current = entryToValue(keyEntry);
                if (current <= max) {
                    next = new TupleShort(current, dataEntry.getData());
                } else { // end of the loop
                    next = null;
                    // close the cursor
                    closeCursor();
                }
            } else { // we are done
                next = null;
                // close the cursor
                closeCursor();
            }
        }
        
        public TupleShort next() {
            synchronized (keyEntry) {
                if (next == null) {
                    throw new NoSuchElementException(
                            "You tried to access an iterator with no next elements");
                }
                TupleShort res = next;
                try {
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
    }
}
