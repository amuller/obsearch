package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.TimeStampResult;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * This class wraps an standard index, and allows the user to obtain information
 * regarding the most recent insertions and deletions. The idea is to used this
 * index in a distributed environment.  the index stores OB objects whose distance
 * functions generate shorts.
 * @author Arnoldo Jose Muller Molina
 * @param <O>
 *            The type of object to be stored in the Index.
 * @since 0.7
 */
public abstract class AbstractSynchronizableIndex < O extends OB > implements
        SynchronizableIndex < O > {
    /**
     * Size of the cache for the underlying index.
     */
    private static final int CACHE_SIZE = 20 * 1024 * 1024;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractSynchronizableIndex.class);

    /**
     * Separate db environment for this index.
     */
    private transient Environment databaseEnvironment;

    /**
     * Convenience object for configuring databases.
     */
    private transient DatabaseConfig dbConfig;

    /**
     * Holds the box|time --> object mapping.
     */
    private transient Database timeDB;

    /**
     * Stores the latest modification to the data.
     */
    protected transient AtomicLongArray timeByBox;

    /**
     * Database directory where the sync information will be stored.
     */
    private File dbDir;

    /**
     * Holds a count of the number of boxes per each box.
     */
    protected transient AtomicIntegerArray objectsByBox;

    /**
     * Initializes the index.
     * @param index
     *            Index that will be wrapped into this index.
     * @param dbDir
     *            The directory of the index.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     */
    public AbstractSynchronizableIndex(final Index < O > index, final File dbDir)
            throws DatabaseException {
        this.dbDir = dbDir;
        initDB();
    }

    /**
     * Initialize the databases.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     */
    private void initDB() throws DatabaseException {
        initBerkeleyDB();
        initInsertTime();
    }

    /**
     * Return the underlying index.
     * @return The index that is being synchronized
     */
    public abstract Index < O > getIndex();

    /**
     * This method makes sure that all the databases are created with the same
     * settings.
     * @throws DatabaseException
     *             Exception thrown by sleepycat
     */
    private void initBerkeleyDB() throws DatabaseException {
        /* Open a transactional Oracle Berkeley DB Environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        //envConfig.setCacheSize(CACHE_SIZE); // 20 MB
        // envConfig.setTxnNoSync(true);
        // envConfig.setTxnWriteNoSync(true);
        // envConfig.setLocking(false);
        this.databaseEnvironment = new Environment(dbDir, envConfig);

        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(true);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        // dbConfig.setBtreeComparator(IntLongComparator.class);
        // dbConfig.setDuplicateComparator(IntLongComparator.class);
        // dbConfig.setExclusiveCreate(true);
    }

    /**
     * Initializes time database.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     */
    private void initInsertTime() throws DatabaseException {
        timeDB = databaseEnvironment.openDatabase(null, "insertTime", dbConfig);
    }

    public final int totalBoxes() {
        return getIndex().totalBoxes();
    }

    public void close() throws DatabaseException {
        getIndex().close();
    }

    public Result delete(final O object) throws IllegalIdException,
            DatabaseException, OBException, IllegalAccessException,
            InstantiationException {
        return delete(object, System.currentTimeMillis());
    }

    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException,
            UndefinedPivotsException {
        getIndex().freeze();
        
        timeByBox = new AtomicLongArray(getIndex().totalBoxes());
        objectsByBox = new AtomicIntegerArray(getIndex().totalBoxes());
        int i = 0;
        while (i < getIndex().totalBoxes()) {
            objectsByBox.set(i, -1);
            i++;
        }
        // after we freeze, we have to insert our data
         i = 0;
        int max = getIndex().databaseSize();
        int[] boxes = new int[this.totalBoxes()];
        while (i < max) {
            O object = getIndex().getObject(i);
            assert object != null;
            int box = getIndex().getBox(object);
            boxes[box] += 1;
            insertInsertEntry(box, System.currentTimeMillis(), i);
            i++;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Boxes distribution:" + Arrays.toString(boxes));
        }
        assert i == this.getIndex().databaseSize();
        assert this.timeDB.count() == this.getIndex().databaseSize() : "time: "
                + timeDB.count() + " the rest: " + getIndex().databaseSize();
    }

    public O getObject(final int i) throws DatabaseException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException {
        return getIndex().getObject(i);
    }

    public Result insert(final O object) throws IllegalIdException,
            DatabaseException, OBException, IllegalAccessException,
            InstantiationException {
        return insert(object, System.currentTimeMillis());
    }

   
    public Result insert(final O object, final long time)
            throws IllegalIdException, DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        Result r = getIndex().insert(object);
        if (r.getStatus() == Result.Status.OK) { // if we could insert the object
            if (isFrozen()) {
                int box = getIndex().getBox(object);
                insertInsertEntry(box, time, r.getId());
                incElementsPerBox(box);
            }
        }
        return r;
    }

    public Result delete(final O object, final long time)
            throws IllegalIdException, DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        Result r = getIndex().delete(object);
        if (r.getStatus() == Result.Status.OK) { // if we could delete the object
            if (isFrozen()) {
                int box = getIndex().getBox(object);
                insertDeleteEntry(box, time, object);
                decElementsPerBox(box);
            }
        }
        return r;
    }

    public boolean isFrozen() {
        return getIndex().isFrozen();
    }

    /**
     * Adds a time entry to the database Key: box + time Value: id.
     * @param box
     *            the box to add
     * @param time
     *            the time of the insertion
     * @param id
     *            the internal id
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    protected final void insertInsertEntry(final int box, final long time,
            final int id) throws DatabaseException, OBException {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        keyEntry.setData(boxTimeToByteArray(box, time));
        TupleOutput out = new TupleOutput();
        out.writeBoolean(true);
        out.writeInt(id);
        dataEntry.setData(out.getBufferBytes());
        OperationStatus ret = timeDB.put(null, keyEntry, dataEntry);
        if (ret != OperationStatus.SUCCESS) {
            throw new DatabaseException();
        }
        // update the cache, make sure that the latest
        // time is updated automatically
        synchronized (timeByBox) {
            if (timeByBox.get(box) < time) {
                timeByBox.set(box, time);
            }
        }

    }

    /**
     * Stores the given object into deleteTimeDB using the appropiate key (box +
     * time). In the case of insertTimeDB we insert only the internal object id,
     * but in the case of deletes, we store all the object
     * @param box
     *            the box to add
     * @param time
     *            the time of the insertion
     * @param object
     *            the object that was deleted
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    protected final void insertDeleteEntry(final int box, final long time,
            final O object) throws DatabaseException, OBException {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();

        keyEntry.setData(boxTimeToByteArray(box, time));
        TupleOutput out = new TupleOutput();
        out.writeBoolean(false);
        object.store(out);
        dataEntry.setData(out.getBufferBytes());
        OperationStatus ret = timeDB.put(null, keyEntry, dataEntry);
        if (ret != OperationStatus.SUCCESS) {
            throw new DatabaseException();
        }
        // update the cache, make sure that the latest
        // time is updated automatically
        synchronized (timeByBox) {
            if (timeByBox.get(box) < time) {
                timeByBox.set(box, time);
            }
        }

    }

    /**
     * Increase the number of elements per box.
     * @param box
     *            box that will be incremented
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private void incElementsPerBox(final int box) throws DatabaseException,
            OBException {
        if (objectsByBox.get(box) == -1) {
            elementsPerBox(box);
        }
        objectsByBox.incrementAndGet(box);
    }

    /**
     * Decrease the number of elements per box.
     * @param box
     *            box that will be decremented.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private void decElementsPerBox(final int box) throws DatabaseException,
            OBException {
        if (objectsByBox.get(box) == -1) {
            elementsPerBox(box);
        }
        objectsByBox.decrementAndGet(box);
    }

    /**
     * Obtain the most recent modification per box.
     * @param box
     *            The box that will be processed
     * @return Latest modification
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    public final long latestModification(final int box) throws DatabaseException,
            OBException {
        if (timeByBox.get(box) == 0) {
            // 0 is the unitialized value.
            timeByBox.set(box, latestInsertedItemAux(box));
        }
        return timeByBox.get(box);
    }

    /**
     * Obtain the number of objects per box.
     * @param box
     *            The box that will be processed
     * @return number of objects per box
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    public int elementsPerBox(final int box) throws DatabaseException,
            OBException {
        if (objectsByBox.get(box) == -1) {
            // this updates the box count.
            objectsByBox.set(box, objectsPerBoxAux(box));
        }
        return this.objectsByBox.get(box);
    }

    /**
     * Counts the number of objects for the given box.
     * @param box
     *            the box to be processed
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @return number of objects for the given box.
     */
    private int objectsPerBoxAux(final int box) throws DatabaseException {
        Cursor cursor = null;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        long resTime = -1;
        int count = 0;
        try {
            cursor = timeDB.openCursor(null, null);
            key.setData(boxTimeToByteArray(box, resTime));
            OperationStatus retVal = cursor.getSearchKeyRange(key, foundData,
                    LockMode.DEFAULT);
            MyTupleInput in = new MyTupleInput();
            int cbox = box;
            while (retVal == OperationStatus.SUCCESS && cbox == box) {
                in.setBuffer(key.getData());
                cbox = in.readInt();
                retVal = cursor.getNext(key, foundData, null);
                if (cbox == box) {
                    count++;
                }
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return count;
    }

    /**
     * Obtains the latest inserted item in the given box or -1 if there are no
     * items.
     * @param box
     *            Box to process
     * @return Latest inserted or deleted item timestamp from the database
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private long latestInsertedItemAux(final int box) throws DatabaseException,
            OBException {
        return latestInsertedItemAuxAux(box, this.timeDB);
    }

    /**
     * Obtains the latest inserted item in the given box or -1 if there are no
     * items. Reads the data from the database.
     * @param box
     *            Box to process
     * @param db
     *            Database that will be queried.
     * @return Latest inserted or deleted item timestamp from the database
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    private long latestInsertedItemAuxAux(final int box, final Database db)
            throws DatabaseException, OBException {
        Cursor cursor = null;
        DatabaseEntry key = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        long resTime = -1;
        try {
            cursor = db.openCursor(null, null);
            key.setData(boxTimeToByteArray(box, 0));
            OperationStatus retVal = cursor.getSearchKeyRange(key, foundData,
                    LockMode.DEFAULT);
            MyTupleInput in = new MyTupleInput();
            int cbox = box;
            while (retVal == OperationStatus.SUCCESS && cbox == box) {
                in.setBuffer(key.getData());
                cbox = in.readInt();
                long time = in.readLong();
                assert validate(resTime, time, cbox, box) : "resTime: "
                        + resTime + " time: " + time;
                if (resTime < time) {
                    resTime = time;
                }
                retVal = cursor.getNext(key, foundData, null);
            }
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        return resTime;
    }

    /**
     * Validate that prev is <= next if cbox == box.
     * @param prev
     *            previous time
     * @param next
     *            next time
     * @param cbox
     *            current box
     * @param box
     *            previous box
     * @return true if prev is <= next when cbox == box
     */
    private boolean validate(final long prev, final long next, final int cbox,
            final int box) {
        if (cbox != box) {
            return true;
        } else {
            return prev <= next;
        }
    }

    /**
     * Create a byte array out of appending box and time.
     * @param box
     *            Box to add
     * @param time
     *            Time to add
     * @return a byte array with box + time encoded
     */
    private byte[] boxTimeToByteArray(final int box, final long time) {
        TupleOutput data = new TupleOutput();
        data.writeInt(box);
        data.writeLong(time);
        return data.getBufferBytes();
    }

    public Iterator < TimeStampResult < O >> elementsNewerThan(final int box,
            final long time) throws DatabaseException {
        return new TimeStampIterator(box, time);
    }

    public int getBox(final O object) throws OBException {
        return this.getIndex().getBox(object);
    }

    /**
     * Returns the boxes currently available in the index.
     * @return boxes currently available in the index
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     */
    public int[] currentBoxes() throws DatabaseException, OBException {
        int i = 0;
        int max = this.totalBoxes();
        List < Integer > result = new LinkedList < Integer >();
        while (i < max) {
            if (latestModification(i) != -1) {
                result.add(i);
            }
            i++;
        }
        int[] resultArray = new int[result.size()];
        i = 0;
        Iterator < Integer > it = result.iterator();
        while (it.hasNext()) {
            resultArray[i] = it.next();
            i++;
        }
        return resultArray;
    }

    /**
     * This class is responsible of iterating the index when a
     * elementsNewerThan() call is made.
     */
    public class TimeStampIterator implements Iterator {

        /**
         * The internal cursor reference.
         */
        private Cursor cursor = null;

        /**
         * The key currently extracted.
         */
        private final DatabaseEntry keyEntry = new DatabaseEntry();
        
        /**
         * The value currently extracted.
         */
        private final DatabaseEntry dataEntry = new DatabaseEntry();

        /**
         * Operation results.
         */
        private OperationStatus retVal;

        /**
         * Byte stream input used to parse data.
         */
        private MyTupleInput in;

        /**
         * Box that was originally queried.
         */
        private int box;

        /**
         * Current box.
         */
        private int cbox = -1;

        /**
         * Previous time value.
         */
        private long previous = Long.MIN_VALUE;

        /**
         * elements currently processed.
         */
        private int count = 0;

        /**
         * Transaction employed (used because this iterator
         * can be called from several threads).
         */
        private Transaction txn;

        /**
         * Creates a new TimeStampIterator from the given database if the
         * parameter idFormat = true then the iterator will extract the objects
         * from the internal index. If the parameter is false, then the objects
         * are actually embeded in the record and will be read from there
         * @param box Box to extract
         * @param from From the given time
         * @throws DatabaseException If somehing goes wrong with the DB
         */
        public TimeStampIterator(final int box, final long from)
                throws DatabaseException {
            this.box = box;

            CursorConfig config = new CursorConfig();
            config.setReadUncommitted(true);
            txn = databaseEnvironment.beginTransaction(null, null);
            cursor = timeDB.openCursor(txn, config);

            previous = from;
            keyEntry.setData(boxTimeToByteArray(box, from));
            retVal = cursor.getSearchKeyRange(keyEntry, dataEntry, null);
            in = new MyTupleInput();
            goNextAux();
        }

        /**
         * Update the data from keyEntry and dataEntry.
         */
        private void goNextAux() {
            if (OperationStatus.SUCCESS == retVal) {
                in.setBuffer(keyEntry.getData());
                cbox = in.readInt();
                long recent = in.readLong();
                count++;
                assert validate(recent) : "Previous: " + previous + " recent"
                        + recent + "returned: " + count;
                previous = recent;
            } else {
                cbox = -1;
            }
        }

        /**
         * Validates some general conditions in the data.
         * @param recent  the latest time.
         * @return true if previous <= recent when box == cbox
         */
        private boolean validate(final long recent) {
            if (box == cbox) {
                return previous <= recent;
            } else {
                return true;
            }
        }

        /**
         * Go to the next record.
         * @throws DatabaseException If somehing goes wrong with the DB
         */
        private void goNext() throws DatabaseException {
            retVal = cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT);
            goNextAux();
        }

        /**
         * Close this iterator.
         * This method makes this iterator a bit different from standard
         * iterators.
         */
        public void close() {
            try {
                cursor.close();
            } catch (DatabaseException e) {
                // logger.fatal(e);
                // if the cursor has been closed already ignore the error
                // assert false;
            }
        }

        /**
         * If there are more elements in the iterator.
         * @return true if there are more elements in the iterator.
         */
        public boolean hasNext() {
            boolean res = (retVal == OperationStatus.SUCCESS) && cbox == box;
            if (!res) {
                close();
            }
            return res;
        }

        /**
         * @return The next elements.
         */
        public TimeStampResult next() {
            try {
                if (hasNext()) {
                    TupleInput inl = new TupleInput(dataEntry.getData());
                    O obj = null;
                    Boolean insert = inl.readBoolean();
                    if (insert) {
                        int id = inl.readInt();
                        obj = getObject(id);
                    } else {
                        obj = getIndex().readObject(inl);
                    }
                    goNext();
                    return new TimeStampResult(obj, previous, insert);
                } else {
                    assert false : "You should be calling hasNext before calling next.";
                    return null;
                }
            } catch (Exception e) {
                logger.fatal("Error while retrieving record", e);
                assert false;
                return null;
            }
        }

        /**
         * The remove method is not implemented. Please do not use it.
         */
        public void remove() {
            assert false;
        }

    }

    public void relocateInitialize(final File dbPath) throws DatabaseException,
            NotFrozenException, DatabaseException, IllegalAccessException,
            InstantiationException, OBException, IOException {
        getIndex().relocateInitialize(dbPath);
    }

    public O readObject(final TupleInput in) throws InstantiationException,
            IllegalAccessException, OBException {
        return getIndex().readObject(in);
    }

    public Result exists(final O object) throws DatabaseException,
            OBException, IllegalAccessException, InstantiationException {
        return getIndex().exists(object);
    }
}
