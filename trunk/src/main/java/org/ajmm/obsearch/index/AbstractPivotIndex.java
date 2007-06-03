package org.ajmm.obsearch.index;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.ajmm.obsearch.Dim;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.OBPriorityQueue;
import org.ajmm.obsearch.OBResult;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.pivotselection.PivotSelector;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
/**
 * A Pivot index uses n pivots from the database to speed up search. The
 * following outlines the insertion workflow: 1) All insertions are copied into
 * a temporary B-Tree A. 2) User freezes the database. 3) Pivot tuples are
 * calculated for each object and they are copied into a B-tree B. 4) Subclasses
 * can calculate additional values to be used by the index. 5) All the objects
 * are finally re-inserted into the final B-Tree C 6) We will keep using B-Tree
 * A to ease object catching and to reduce the size of the pivot Tree C. Note
 * that B is deleted. Generics are used to make sure that all the inserted
 * objects will be of the same type You should not mix types as the distance
 * function and the objects must be consistent. Also, Inserts are first added to
 * A, and then to C. This guarantees that there will be always objects to match.
 * 
 * @param <O>
 *            The object type to be used
 * @param <D>
 *            The dimension type to be used
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */
@XStreamAlias("AbstractPivotIndex")
public abstract class AbstractPivotIndex<O extends OB>
        implements Index<O> {

    private static transient final Logger logger = Logger
            .getLogger(AbstractPivotIndex.class);

    private File dbDir;

    protected byte pivotsCount;

    private boolean frozen;

    // we should not have to control this property after freezing. This is just
    // used as a safeguard.
    // It allows RandomPivotSelector to be implemented easily
    private transient int maxId;

    private transient PivotSelector pivotSelector;

    protected transient Environment databaseEnvironment;

    // database with the objects
    protected transient Database aDB;

    // database with the temporary pivots
    protected transient Database bDB;

    protected transient Database pivotsDB;

    protected transient DatabaseConfig dbConfig;

    protected transient O[] pivots;

    protected transient OBCache<O> cache;

    // we keep this in order to be able to create objects of type O
    protected Class<O> type;

    /**
     * Creates a new pivot index. The maximum number of pivots has been
     * arbitrarily hardcoded to 256.
     * 
     * @param databaseDirectory
     *            Where all the databases will be stored.
     * @param pivots
     *            The number of pivots to be used.
     * @param pivotSelector
     *            The object that will choose a hopefully good set of pivots
     *            from the DB.
     * @param type
     *            The type of the object the user will use. Has to match with O.
     *            Do something like YourObj.class
     * @throws DatabaseException
     */
    public AbstractPivotIndex(final File databaseDirectory, final byte pivots)
            throws DatabaseException, IOException {
        this.dbDir = databaseDirectory;
        if(! dbDir.mkdirs()){
            //throw new IOException("Directory already exists");
        }
        this.pivotsCount = pivots;
        frozen = false;
        maxId = 0;
        initDB();
    }

    /**
     * Creates an array with the pivots It has to be created like this because
     * we are using generics
     */
    protected void createPivotsArray() {
        this.pivots = (O[]) Array.newInstance(type, pivotsCount);
    }

    /**
     * Initialization of all the databases involved Subclasses should override
     * this method if they want to create new databases
     * 
     * @throws DatabaseException
     */
    private final void initDB() throws DatabaseException {
        initBerkeleyDB();
        // way of creating a database
        initA();
        initB();
        initPivots();
        initC();
    }
    
    private void initB() throws DatabaseException{
        final boolean duplicates = dbConfig.getSortedDuplicates();
        dbConfig.setSortedDuplicates(false);
        bDB = databaseEnvironment.openDatabase(null, "B", dbConfig);
        dbConfig.setSortedDuplicates(duplicates);
    }

    /**
     * This method will be called by the super class Initializes the C
     * database(s)
     */
    protected abstract void initC() throws DatabaseException;

    protected Object readResolve() throws DatabaseException,
            NotFrozenException, DatabaseException, IllegalAccessException,
            InstantiationException {
        if (logger.isDebugEnabled()) {
            logger
                    .debug("Initializing transient fields after de-serialization");
        }
        initDB();
        loadPivots();
        initCache();
        return this;
    }

  

    protected void initCache() throws DatabaseException {
        int size = databaseSize();
        cache = new OBCache<O>(size);
    }

    protected int databaseSize() throws DatabaseException {
        return (int) aDB.count();
    }

    /**
     * Creates database A.
     * 
     * @throws DatabaseException
     *             if something goes wrong
     */
    private void initA() throws DatabaseException {
        final boolean duplicates = dbConfig.getSortedDuplicates();
        dbConfig.setSortedDuplicates(false);
        aDB = databaseEnvironment.openDatabase(null, "A", dbConfig);
        dbConfig.setSortedDuplicates(duplicates);
    }

    /**
     * Creates database Pivots.
     * 
     * @throws Exception
     */
    private void initPivots() throws DatabaseException {
        final boolean duplicates = dbConfig.getSortedDuplicates();
        dbConfig.setSortedDuplicates(false);
        pivotsDB = databaseEnvironment.openDatabase(null, "Pivots", dbConfig);
        dbConfig.setSortedDuplicates(duplicates);
    }

    /**
     * This method makes sure that all the databases are created with the same
     * settings. TODO: Need to tweak these values
     * 
     * @throws DatabaseException
     *             Exception thrown by sleepycat
     */
    private void initBerkeleyDB() throws DatabaseException {
        /* Open a transactional Oracle Berkeley DB Environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        // envConfig.setCacheSize(8000000);
        // envConfig.setTxnNoSync(true);
        // envConfig.setTxnWriteNoSync(true);
        // envConfig.setLocking(false);
        this.databaseEnvironment = new Environment(dbDir, envConfig);

        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        // dbConfig.setExclusiveCreate(true);
    }

    /**
     * Utility method to insert data before freezing takes place.
     * 
     * @param object
     *            The object to be inserted
     * @param id
     *            The id to be added
     * @throws IllegalIdException
     *             if the given ID already exists or if isFrozen() = false and
     *             the ID's did not come in sequential order.
     * @throws DatabaseException
     * @return 1 if the object was inserted
     */
    protected byte insertUnFrozen(O object, int id) throws IllegalIdException,
            DatabaseException {
        if (id != maxId) {
            throw new IllegalIdException();
        }
        if (type == null) { // a way of storing the class type for O
            type = (Class<O>) object.getClass();
        }
        maxId = id + 1;
        insertA(object, id);
        return 1;
    }

    public byte insert(O object, int id) throws IllegalIdException,
            DatabaseException, OBException ,  IllegalAccessException, InstantiationException{
        if (isFrozen()) {
            insertA(object, id);
            return insertFrozen(object, id);
        } else {            
            return insertUnFrozen(object, id);
        }
    }

    /**
     * Inserts in database A the given Object.
     * 
     * @param object
     * @param id
     * @throws DatabaseException
     */
    protected void insertA(final O object, final int id)
            throws DatabaseException {
        insertObjectInDatabase(object, id, aDB);
    }

    /**
     * Inserts the given object with the given Id in the database x
     * 
     * @param object
     *            object to be inserted
     * @param id
     *            id for the object
     * @param x
     *            Database to be used
     * @throws DatabaseException
     */
    protected void insertObjectInDatabase(final O object, final int id,
            Database x) throws DatabaseException {
        final DatabaseEntry keyEntry = new DatabaseEntry();

        // store the object in bytes
        final TupleOutput out = new TupleOutput();
        object.store(out);

        // store the ID
        IntegerBinding.intToEntry(id, keyEntry);
        insertInDatabase(out, keyEntry, x);
    }
    
    /**
     * Inserts the given OBJECT in B. 
     * @param id
     * @param object
     * @throws DatabaseException
     */
    protected abstract void insertInB(int id, O object);
   /*protected void insertTupleInB(int id, D[] tuple) throws DatabaseException{
       DatabaseEntry keyEntry = new DatabaseEntry();
       DatabaseEntry dataEntry = new DatabaseEntry();
       TupleOutput out = new TupleOutput();
       // write the tuple
       for (D d : tuple) {
           d.store(out);
       }
       // store the ID
       IntegerBinding.intToEntry(id, keyEntry);
       insertInDatabase(out, keyEntry, bDB);
   }*/

    protected void insertInDatabase(final TupleOutput out,
            DatabaseEntry keyEntry, Database x) throws DatabaseException {
        final DatabaseEntry dataEntry = new DatabaseEntry();
        dataEntry.setData(out.getBufferBytes());
        x.put(null, keyEntry, dataEntry);
    }

    /**
     * Utility method to insert data in C after freezing. Must be implemented by
     * the subclasses It should not insert anything into A
     * 
     * @param object
     *            The object to be inserted
     * @param id
     *            The id to be added
     * @throws IllegalIdException
     *             if the id already exists
     * @return 1 if successful 0 otherwise
     */
    protected abstract byte insertFrozen(final O object, final int id)
            throws IllegalIdException, OBException, DatabaseException,
            OBException,  IllegalAccessException, InstantiationException ;

    /**
     * If the database is frozen returns silently if it is not throws
     * NotFrozenException
     */
    protected void assertFrozen() throws NotFrozenException {
        if (!isFrozen()) {
            throw new NotFrozenException();
        }
    }

    /**
     * Freezes the index. From this point data can be inserted, searched and
     * deleted The index might deteriorate at some point so every once in a
     * while it is a good idea to rebuild de index. After the method returns,
     * searching is enabled. 
     * 
     * @param pivotSelector
     *            The pivot selector to be used
     * @throws IOException
     *             if the serialization process fails
     * @throws AlreadyFrozenException
     *             If the index was already frozen and the user attempted to
     *             freeze it again
     */
    public void freeze(PivotSelector pivotSelector) throws IOException,
            AlreadyFrozenException, IllegalIdException, IllegalAccessException,
            InstantiationException, DatabaseException, OutOfRangeException,
            OBException {
        if (isFrozen()) {
            throw new AlreadyFrozenException();
        }
        this.pivotSelector = pivotSelector; 
        createPivotsArray();
        if (logger.isDebugEnabled()) {
            logger.debug("Storing Pivots in B");
        }
        storePivots();
        // we have to create database B        
       insertFromAtoB();

        calculateIndexParameters(); // this must be done by the subclasses
       
        // we have to insert the objects already inserted in A into C
        insertFromBtoC();
        // cache is initialized as from the point we set frozen = true 
        // queries can be achieved
        initCache();
        // we could delete bDB from this point
        
        this.frozen = true;
        // queries can be executed from this point
        
        XStream xstream = new XStream();
        // TODO: make sure this "this" will print the subclass and not the
        // current class
        String xml = xstream.toXML(this);
        FileWriter fout = new FileWriter(this.dbDir.getPath()
                + getSerializedName());
        fout.write(xml);
        fout.close();        
        
    }
    
    /**
     * Must return "this"
     * Used to serialize the current son
     */
    protected abstract Index serializeObject();
    
    /**
     * Inserts all the values already inserted in A into B
     * 
     */
    private void insertFromAtoB() throws IllegalAccessException,
            InstantiationException, DatabaseException, OBException {
        Cursor cursor = null;
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        
        try {
            int i = 0;

            cursor = aDB.openCursor(null, null);
            O obj = this.instantiateObject();
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) 
                    == OperationStatus.SUCCESS) {
                int id = IntegerBinding.entryToInt(foundKey);
                assert i == id;
                TupleInput in = new TupleInput(foundData.getData());
                obj.load(in);
                insertInB(id, obj);
                i++;
            }
            // should be the same
        } finally {
            cursor.close();
        }
    }

    /**
     * Inserts all the values already inserted in A into C
     * 
     */
    protected abstract void insertFromBtoC();
    /*private void insertFromBtoC() throws IllegalAccessException,
            InstantiationException, DatabaseException, OBException {
        Cursor cursor = null;
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();
        try {
            int i = 0;

            cursor = aDB.openCursor(null, null);
            O obj = this.instantiateObject();
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT)
                    == OperationStatus.SUCCESS) {
                int id = IntegerBinding.entryToInt(foundKey);
                assert i == id;
                TupleInput in = new TupleInput(foundData.getData());
                obj.load(in);
                insertFrozen(obj, id);
                i++;
            }
            // should be the same
        } finally {
            cursor.close();
        }
    }*/

    /**
     * Children of this class have to implement this method if they want to
     * calculate extra parameters
     */
    protected abstract void calculateIndexParameters()
            throws DatabaseException, IllegalAccessException,
            InstantiationException, OutOfRangeException;

    

    /**
     * Inserts the given tuple in the database B using the given id B database
     * will hold tuples of floats (they have been normalized)
     * 
     * @param id
     *            the id to use for the insertion
     * @param tuple
     *            The tuple to be inserted
     */
    /*protected void insertPivotTupleInBDB(int id, D[] tuple)
            throws DatabaseException {
        TupleOutput out = new TupleOutput();
        int i = 0;
        assert tuple.length == pivotsCount;
        // first encode all the dimensions in an array
        while (i < tuple.length) {
            tuple[i].store(out); // stores the given pivot
            i++;
        }
        // now we can store it in the database
        DatabaseEntry keyEntry = new DatabaseEntry();
        IntegerBinding.intToEntry(id, keyEntry);
        this.insertInDatabase(out, keyEntry, bDB);
    }*/

    

    /**
     * returns the given object from DB A
     * 
     * @param id
     * @return the object
     */
    // TODO: need to implement a cache here
    protected O getObject(int id) throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException {
        O res = cache.get(id);
        if (res == null) {
            res = getObject(id, aDB);
            cache.put(id, res);
        }
        return res;
    }

    /**
     * Calculates the tuple vector for the given object
     * 
     * @param obj
     *            object to be processed
     * @param tuple
     *            The resulting tuple will be stored here
     */
    /*protected void calculatePivotTuple(final O obj, D[] tuple)
            throws OBException {
        assert tuple.length == this.pivotsCount;
        int i = 0;
        while (i < tuple.length) {
            obj.distance(this.pivots[i], tuple[i]);
            i++;
        }
    }*/

    /**
     * Stores the pivots selected by the pivot selector in database Pivot. As a
     * side effect leaves the pivots cached in this.pivots
     * 
     * @throws IllegalIdException
     *             If the pivot selector generates invalid ids
     */
    protected void storePivots() throws IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException {
        int[] ids = pivotSelector.generatePivots(pivotsCount, maxId);
        if(logger.isDebugEnabled()){
            logger.debug("Pivots selected " + Arrays.toString( ids));
        }
        assert ids.length == pivots.length && pivots.length == this.pivotsCount;

        int i = 0;
        while (i < ids.length) {
            O obj = getObject(ids[i], aDB);
            insertObjectInDatabase(obj, i, pivotsDB); // store in the B-tree
            pivots[i] = obj;
            i++;
        }
    }

    /**
     * Loads the pivots from the database
     * 
     * @throws NotFrozenException
     *             if the freeze method has not been invoqued.
     */
    protected void loadPivots() throws NotFrozenException, DatabaseException,
            IllegalAccessException, InstantiationException {
        Cursor cursor = null;
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        if (!isFrozen()) {
            throw new NotFrozenException();
        }
        try {
            int i = 0;
            cursor = pivotsDB.openCursor(null, null);
            O obj = this.instantiateObject();
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                assert i == IntegerBinding.entryToInt(foundKey);
                TupleInput in = new TupleInput(foundData.getData());
                obj.load(in);
                pivots[i] = obj;
                i++;
            }
            assert i == pivotsCount; // pivot count and read # of pivots
            // should be the same
        } finally {
            cursor.close();
        }
    }

    /**
     * Generates the name of the file to be used to store the serialized version
     * of this Index.
     * 
     * @return
     */
    public abstract String getSerializedName();

    /**
     * Gets the object with the given id from the database.
     * 
     * @param id
     *            The id to be extracted
     * @param DB
     *            the database to be accessed the object will be returned here
     * @return the object the user asked for
     * @throws DatabaseException
     * @throws IllegalIdException
     *             if the given id does not exist in the database
     */
    private O getObject(int id, Database DB) throws DatabaseException,
            IllegalIdException, IllegalAccessException, InstantiationException {
        // TODO: put these two objects in the class so that they don't have to
        // be created over and over again
        // TODO: add an OB cache
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        IntegerBinding.intToEntry(id, keyEntry);

        O object = instantiateObject();

        if (DB.get(null, keyEntry, dataEntry, null) == OperationStatus.SUCCESS) {
            // TODO: Extend TupleInput so that we don't have to create the
            // object over and over again
            TupleInput in = new TupleInput(dataEntry.getData());
            object.load(in); // load the bytes into the object
        } else {
            throw new IllegalIdException();
        }
        return object;
    }

    protected O instantiateObject() throws IllegalAccessException,
            InstantiationException {
        // Find out if java can give us the type information directly from the
        // template parameter. There should be a way...
        return type.newInstance();
    }

    public O[] getPivots() {
        return this.pivots;
    }

    /**
     * Returns the current amount of pivots
     * 
     * @return
     */
    public byte getPivotsCount() {
        return this.pivotsCount;
    }

    /**
     * Returns true if the database has been frozen
     * 
     * @return true if the database has been frozen
     */
    public boolean isFrozen() {
        return this.frozen;
    }

}
