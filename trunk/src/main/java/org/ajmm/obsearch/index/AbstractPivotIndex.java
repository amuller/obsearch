package org.ajmm.obsearch.index;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.Status;
import org.ajmm.obsearch.asserts.OBAsserts;
import org.ajmm.obsearch.cache.OBCache;
import org.ajmm.obsearch.cache.OBCacheLoader;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.sleepycat.je.StatsConfig;
import com.sleepycat.je.Transaction;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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
 * A Pivot index uses n pivots from the database to speed up search. The
 * following outlines the insertion workflow: 1) All insertions are stored into
 * a temporary B-Tree A. 2) User freezes the database. 3) Pivot tuples are
 * calculated for each object and they are copied into a B-tree B. 4) Subclasses
 * can calculate additional values to be used by the index. 5) All the objects
 * are finally re-inserted into the final B-Tree C 6) We will keep using B-Tree
 * A to ease object catching and to reduce the size of the pivot Tree C. Note
 * that B is deleted. Generics are used to make sure that all the inserted
 * objects will be of the same type You should not mix types as the distance
 * function and the objects must be consistent. Also, Inserts are first added to
 * A, and then to C. This guarantees that there will be always objects to match.
 * @param <O>
 *                The object type to be used
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
@XStreamAlias("AbstractPivotIndex")
public abstract class AbstractPivotIndex < O extends OB > implements Index < O > {

    /**
     * Number of bytes used by the ids that OBSearch uses.
     */
    protected static final transient int ID_SIZE_BYTES = 4;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractPivotIndex.class);

    /**
     * Directory that will contain the index.
     */
    private File dbDir;

    /**
     * Number of dimensions used to index objects.
     */
    protected short pivotsCount;

    /**
     * If the index is frozen or not.
     */
    private boolean frozen;

    /**
     * We should not have to control this property after freezing. This is just
     * used as a safeguard to make sure that every inserted item before freezing
     * is a consecutive integer (always guaranteed because the user does not
     * provide this id). It allows some PivotSelectors to be implemented easily.
     */
    private transient int maxId;

    /**
     * Berkeley DB database environment.
     */
    protected transient Environment databaseEnvironment;

    /**
     * Berkeley DB database environment used only at database creation time.
     */
    protected transient Environment databaseEnvironmentCreation;

    /**
     * Database with the objects. The index is a map of ids -> OB. We store the
     * objects independently of the SMAP info. for efficiency reasons.
     */
    protected transient Database aDB;

    /**
     * Database with the temporary pivots. Used only during Freezing, after
     * freezing this database can be erased.
     */
    protected transient Database bDB;

    /**
     * Database used to store objects inserted before Freezing. This is to make
     * sure that one object is inserted only once per
     */
    protected transient Database kDB;

    /**
     * The pivots for this Tree. When we instantiate or de-serialize this object
     * we load them from {@link #pivotsBytes}.
     */
    protected transient O[] pivots;

    /**
     * Bytes of the pivots. From this array we can load the pivots into
     * {@link #pivots}.
     */
    protected byte[][] pivotsBytes;

    /**
     * Cache used for storing recently accessed objects O.
     */
    protected transient OBCache < O > cache;

    /**
     * Keeps track of the ids for insertion. When we want to insert a new
     * record, we increment this id.
     */
    protected transient AtomicInteger id;

    /**
     * We keep this in order to be able to create objects of type O.
     */
    protected Class < O > type;

    /**
     * Size of the cache for the underlying db.
     */
    protected static final transient int CACHE_SIZE = 700 * 1024 * 1024;

    /**
     * The pivot selector used in the index.
     */
    protected PivotSelector < O > pivotSelector;

    /**
     * Creates a new pivot index. The maximum number of pivots has been
     * arbitrarily hard-coded to the size of short.
     * @param databaseDirectory
     *                Where all the databases will be stored.
     * @param pivots
     *                The number of pivots to be used.
     * @param pivotSelector The pivot selector that will be used at freeze. If you are not planning to freeze during the life of this object, just put a null.
     * @param type The class of the object O that will be used.   
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory does not exist.
     */
    public AbstractPivotIndex(final File databaseDirectory, final short pivots,
            PivotSelector < O > pivotSelector, Class<O> type) throws DatabaseException,
            IOException {
        this.dbDir = databaseDirectory;
        if (!dbDir.exists()) {
            throw new IOException("Directory does not exist.");
        }
        assert pivots <= Short.MAX_VALUE;
        assert pivots >= Short.MIN_VALUE;
        this.pivotsCount = pivots;
        frozen = false;
        maxId = 0;
        id = new AtomicInteger(0);
        this.pivotsBytes = new byte[pivotsCount][];
        this.pivotSelector = pivotSelector;
        initDB();
        this.type = type;
    }

    /**
     * Creates an array with the pivots. It has to be created like this because
     * we are using generics.
     */
    protected void createPivotsArray() {
        this.pivots = emptyPivotsArray();
    }

    /**
     * Create an empty pivots array.
     * @return an empty pivots array of size {@link #pivotsCount}.
     */
    public O[] emptyPivotsArray() {
        return (O[]) Array.newInstance(type, pivotsCount);
    }

    /**
     * Initialization of all the databases involved Subclasses should override
     * this method if they want to create new databases.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 If any of the paths to be used is wrong.
     */
    private void initDB() throws DatabaseException, IOException {
        initBerkeleyDB();
        // way of creating a database
        initA();
        initB();
        initC();
        initK();
    }

    /**
     * Initializes only the databases that are to be used after freezing.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 if some path is wrong and the databases cannot be
     *                 created.
     */
    private void initDBAfterInitialization() throws DatabaseException,
            IOException {
        initBerkeleyDB();
        initA();
        // way of creating a database
        initC();
    }

    /**
     * Initializes database B. This database is only used during
     * {@link #freeze()}.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     */
    private void initB() throws DatabaseException {

        DatabaseConfig dbConfig = createDefaultDatabaseConfig();
        dbConfig.setSortedDuplicates(false);
        dbConfig.setDeferredWrite(true); // temp database!
        bDB = databaseEnvironmentCreation.openDatabase(null, "B", dbConfig);
    }

    /**
     * Initializes database B. This database is only used before
     * {@link #freeze()}.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     */
    private void initK() throws DatabaseException {
        DatabaseConfig dbConfig = createDefaultDatabaseConfig();
        dbConfig.setSortedDuplicates(false);
        dbConfig.setDeferredWrite(true);
        kDB = databaseEnvironmentCreation.openDatabase(null, "K", dbConfig);
    }

    /**
     * This method will be called by the super class Initializes the C *
     * database(s).
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     */
    protected abstract void initC() throws DatabaseException;

    /**
     * This method is called by xstream when all the serialized fields have been
     * populated. Used for de-serialization.
     * @return Returns this.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws NotFrozenException
     *                 if the index has not been frozen.
     * @throws IOException
     *                 If any of the paths that will be used are wrong.
     */
    protected Object initializeAfterSerialization() throws DatabaseException,
            NotFrozenException, IllegalAccessException, InstantiationException,
            OBException, IOException {
        if (logger.isDebugEnabled()) {
            logger
                    .debug("Initializing transient fields after de-serialization");
        }
        this.initDBAfterInitialization();
        loadPivots();
        initCache();
        // restore the ids
        id = new AtomicInteger(this.databaseSize());
        return this.returnSelf();
    }

    public void relocateInitialize(final File dbPath) throws DatabaseException,
            NotFrozenException, DatabaseException, IllegalAccessException,
            InstantiationException, OBException, IOException {
        if (dbPath != null) {
            this.dbDir = dbPath;
            if (!dbPath.exists()) {
                throw new IOException(dbPath + " does not exist.");
            }
        }
        initializeAfterSerialization();
    }

    /**
     * Initializes the object cache {@link #cache}.
     * @throws DatabaseException
     *                 If something goes wrong with the DB.
     */
    protected void initCache() throws DatabaseException, OBException {
        if (cache == null) {
            int size = databaseSize();
            cache = new OBCache < O >(new ALoader());
        }
    }

    public int databaseSize() throws DatabaseException {
        return (int) aDB.count();
    }

    /**
     * Creates database A. This is the database where the actual objects O are
     * stored.
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     */
    private void initA() throws DatabaseException {

        DatabaseConfig dbConfig = createDefaultDatabaseConfig();
        dbConfig.setSortedDuplicates(false);
        aDB = databaseEnvironment.openDatabase(null, "A", dbConfig);
        //PreloadConfig pc = new PreloadConfig();
        //pc.setLoadLNs(true);
        //aDB.preload(pc);
    }

    /**
     * Creates the default environment configuration.
     * @return Default environment configuration.
     */
    private EnvironmentConfig createEnvConfig() {
        /* Open a transactional Oracle Berkeley DB Environment. */
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(false);
        // the default value worked pretty well.
        // envConfig.setCacheSize(CACHE_SIZE);
        // leaving this, but it is meaningless when setLocking(false)
        //envConfig.setTxnNoSync(true);
        envConfig.setConfigParam("java.util.logging.DbLogHandler.on", "false");
        // 100 k gave the best performance in one thread and for 30 pivots of
        // shorts
        //envConfig.setConfigParam("je.log.faultReadSize", "30720");
        // envConfig.setConfigParam("je.log.faultReadSize", "10240");
        // alternate access method might be the best. We got to keep all the
        // btree in memory
        // envConfig.setConfigParam("je.evictor.lruOnly", "false");
        // envConfig.setConfigParam("je.evictor.nodesPerScan", "100");
        // envConfig.setTxnNoSync(true);
        // envConfig.setTxnWriteNoSync(true);
        // disable this in production
        // envConfig.setLocking(false);

        return envConfig;
    }

    /**
     * This method makes sure that all the databases are created with the same
     * settings.
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     */
    private void initBerkeleyDB() throws DatabaseException, IOException {
        EnvironmentConfig envConfig = createEnvConfig();
        this.databaseEnvironment = new Environment(dbDir, envConfig);
        EnvironmentConfig envConfig2 = createEnvConfig();
        
        File environmentHome = generateCreationEnvironmentFolder();
        environmentHome.mkdirs();
        this.databaseEnvironmentCreation = new Environment(
                generateCreationEnvironmentFolder(), envConfig2);
        if (!this.isFrozen()) {                       
            OBAsserts.chkFileExists(environmentHome);
        }
    }

    /**
     * Creates a default database configuration.
     * @return default database configuration.
     */
    protected DatabaseConfig createDefaultDatabaseConfig() {
        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig = new DatabaseConfig();
        dbConfig.setTransactional(false);
        dbConfig.setAllowCreate(true);
        dbConfig.setSortedDuplicates(true);
        return dbConfig;
    }

    private File generateCreationEnvironmentFolder() {
        return new File(dbDir + File.separator + "freeze");
    }

    /**
     * Print some B-tree related stats.
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     */
    public void stats() throws DatabaseException {
        StatsConfig config = new StatsConfig();
        config.setClear(true);
        config.setFast(true);
        logger.info(databaseEnvironment.getStats(config));
    }

    /**
     * Utility method to insert data before freezing takes place.
     * @param object
     *                The object to be inserted
     * @param id
     *                The id to be added
     * @throws IllegalIdException
     *                 if the given ID already exists or if isFrozen() = false
     *                 and the ID's did not come in sequential order.
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     * @return 1 if the object was inserted
     */
    protected byte insertUnFrozen(final O object, final int id)
            throws IllegalIdException, DatabaseException {
        if (id != maxId) {
            throw new IllegalIdException();
        }        
        maxId = id + 1;
        insertA(object, id);
        return 1;
    }

    public Result insert(final O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        int resId = -1;
        Result res;
        if (isFrozen()) {
            Result r = exists(object);
            if (r.getStatus() == Status.NOT_EXISTS) {
                // if the object is not in the database, we can insert it
                resId = id.getAndIncrement();
                insertA(object, resId);
                insertFrozen(object, resId);
                res = new Result(Status.OK);
                res.setId(resId);
            } else {
                res = r;
            }
        } else {
            int r = this.existsObjectBeforeFreeze(object);
            if (r == -1) {
                resId = id.getAndIncrement();
                insertUnFrozen(object, resId);
                res = new Result(Status.OK);
                res.setId(resId);
                // put the object in k
                storeObjectInK(object, resId);
            } else {
                res = new Result(Status.EXISTS);
                res.setId(r);
            }
        }
        return res;
    }

    /**
     * Inserts the given object as key and the given id as value in database K
     * @param object
     *                The object that will be used as key
     * @param id
     *                The respective value of the object.
     * @throws DatabaseException
     */
    private void storeObjectInK(O object, int id) throws DatabaseException {
        TupleOutput outValue = new TupleOutput();
        outValue.writeInt(id);
        final DatabaseEntry keyEntry = new DatabaseEntry();
        TupleOutput keyValue = new TupleOutput();
        object.store(keyValue);
        keyEntry.setData(keyValue.getBufferBytes());
        insertInDatabase(outValue, keyEntry, kDB);
    }

    /**
     * Inserts in database A the given Object. The timestamp of the object is
     * stored too.
     * @param object
     *                The object to insert.
     * @param id
     *                The id to employ.
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     */
    protected void insertA(final O object, final int id)
            throws DatabaseException {
        final DatabaseEntry keyEntry = new DatabaseEntry();
        // store the object in bytes
        final TupleOutput out = new TupleOutput();
        object.store(out);
        // store the ID
        IntegerBinding.intToEntry(id, keyEntry);
        insertInDatabase(out, keyEntry, aDB);
    }

    /**
     * Inserts the given object with the given Id in the database x.
     * @param object
     *                object to be inserted
     * @param id
     *                id for the object
     * @param x
     *                Database to be used
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     */
    protected void insertObjectInDatabase(final O object, final int id,
            final Database x) throws DatabaseException {
        final DatabaseEntry keyEntry = new DatabaseEntry();

        // store the object in bytes
        final TupleOutput out = new TupleOutput();
        object.store(out);

        // store the ID
        IntegerBinding.intToEntry(id, keyEntry);
        insertInDatabase(out, keyEntry, x);
    }

    /**
     * Inserts the given object in B.
     * @param id
     *                Id of the object.
     * @param object
     *                The object to insert.
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     */
    protected abstract void insertInB(int id, O object) throws OBException,
            DatabaseException;

    /**
     * A general byte stream insertion procedure.
     * @param out
     *                Byte stream to insert.
     * @param keyEntry
     *                The key to use
     * @param x
     *                The database in which the item should be inserted.
     * @throws DatabaseException
     *                 if something goes wrong with the DB.
     */
    protected void insertInDatabase(final TupleOutput out,
            final DatabaseEntry keyEntry, final Database x)
            throws DatabaseException {
        final DatabaseEntry dataEntry = new DatabaseEntry();
        dataEntry.setData(out.getBufferBytes());
        x.put(null, keyEntry, dataEntry);
    }

    /**
     * Utility method to insert data in C after freezing. Must be implemented by
     * the subclasses It should not insert anything into A.
     * @param object
     *                The object to be inserted
     * @param id
     *                The id to be added
     * @throws IllegalIdException
     *                 if the id already exists
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @return 1 if successful 0 otherwise
     */
    protected abstract byte insertFrozen(final O object, final int id)
            throws IllegalIdException, OBException, DatabaseException,
            IllegalAccessException, InstantiationException;

    /**
     * If the database is frozen returns silently if it is not throws
     * NotFrozenException.
     * @throws NotFrozenException
     *                 if the index has not been frozen.
     */
    protected void assertFrozen() throws NotFrozenException {
        if (!isFrozen()) {
            throw new NotFrozenException();
        }
    }

    /**
     * This method is called before the freeze. It will delete unnecessary data
     * and will initialize the cache. In addition, pivots will be selected here.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     */
    protected void prepareFreeze() throws DatabaseException,
            PivotsUnavailableException, IllegalAccessException,
            InstantiationException, OBException {
        // we do not need this database anymore as it is only before
        // freeze is performed
        deleteDatabaseCreation(kDB, "K");
        kDB = null;
        initCache();
        logger.info("Generating pivots.");
        this.pivotSelector.generatePivots(this);
    }

    /**
     * Freezes the index. From this point data can be inserted, searched and
     * deleted The index might deteriorate at some point so every once in a
     * while it is a good idea to rebuild de index. After the method returns,
     * searching and additional insertions or deletions can be performed on the
     * index.
     * @throws IOException
     *                 if the index serialization process fails
     * @throws AlreadyFrozenException
     *                 If the index was already frozen and the user attempted to
     *                 freeze it again
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws UndefinedPivotsException
     *                 If the pivots of the index have not been selected before
     *                 calling this method.
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws IllegalIdException
     *                 This exception is left as a Debug flag. If you receive
     *                 this exception please report the problem to:
     *                 http://code.google.com/p/obsearch/issues/list
     */
    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException
             {
        if (isFrozen()) {
            throw new AlreadyFrozenException();
        }
        
        if(this.databaseSize() == 0){
            throw new OBException("Cannot freeze a database of size 0");
        }
        try{
            this.prepareFreeze();
        }catch(Exception e){
            logger.debug("Something bad happening before freezing: ", e);
            throw new OBException(e);
        }
        if (pivots == null) {
            throw new OBException(new UndefinedPivotsException());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Storing pivot tuples from A to B");
        }

        

        // we have to create database B
        insertFromAtoB();

        calculateIndexParameters(); // this must be done by the subclasses

        // we have to insert the objects already inserted in A into C
        logger.info("Copying data from B to C");
        insertFromBtoC();

        // we could delete bDB from this point

        this.frozen = true;
        // queries can be executed from this point

        writeSporeFile();

        assert aDB.count() == bDB.count();

        // now we can get rid of the data in B.
        deleteDatabaseCreation(bDB, "B");
        bDB = null;

    }

    /**
     * Deletes all the records of database db.
     * @param db
     *                the database that will be deleted
     */
    private void deleteDatabase(Database db, String name)
            throws DatabaseException {
        db.close();
        // Transaction txn = databaseEnvironment.beginTransaction(null, null);
        this.databaseEnvironment.truncateDatabase(null, name, false);
        // txn = databaseEnvironment.beginTransaction(null, null);
        this.databaseEnvironment.removeDatabase(null, name);

        this.databaseEnvironment.cleanLog();
        this.databaseEnvironment.compress();
        this.databaseEnvironment.checkpoint(null);
    }

    private void deleteDatabaseCreation(Database db, String name)
            throws DatabaseException {
        db.close();
        // Transaction txn = databaseEnvironment.beginTransaction(null, null);
        this.databaseEnvironmentCreation.truncateDatabase(null, name, false);
        // txn = databaseEnvironment.beginTransaction(null, null);
        this.databaseEnvironmentCreation.removeDatabase(null, name);

        this.databaseEnvironmentCreation.cleanLog();
        this.databaseEnvironmentCreation.compress();
        this.databaseEnvironmentCreation.checkpoint(null);
    }

    /**
     * Writes down the spore file of this index.
     * @throws IOException
     *                 If the file cannot be written.
     */
    protected void writeSporeFile() throws IOException {
        String xml = toXML();
        FileWriter fout = new FileWriter(this.dbDir.getPath() + File.separator
                + METADATA_FILENAME);
        fout.write(xml);
        fout.close();
    }

    /**
     * Returns the current maximum id.
     * @return The maximum id of this index.
     */
    public int getMaxId() {
        return this.maxId;
    }

    /**
     * Must return "this" Used to serialize the object.
     * @return This index.
     */
    protected abstract Index returnSelf();

    /**
     * Inserts all the values already inserted in A into B.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    private void insertFromAtoB() throws IllegalAccessException,
            InstantiationException, DatabaseException, OBException {

        int i = 0;
        O obj;
        int count = this.databaseSize();
        while (i < count) {
            obj = this.getObject(i);
            insertInB(i, obj);
            i++;
        }

    }

    public String toXML() {
        XStream xstream = new XStream();
        String xml = xstream.toXML(returnSelf());
        return xml;
    }

    public Result delete(final O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException, NotFrozenException {
        assertFrozen();
        Result res = deleteAux(object);
        // delete the data from database A
        if (res.getStatus() == Status.OK) {
            DatabaseEntry keyEntry = new DatabaseEntry();
            IntegerBinding.intToEntry(res.getId(), keyEntry);
            OperationStatus ret = aDB.delete(null, keyEntry);
            if (ret != OperationStatus.SUCCESS) {
                throw new DatabaseException();
            }
        }
        return res;
    }

    /**
     * Deletes the given object from database C
     * @param object
     * @return {@link org.ajmm.obsearch.Result#OK} and the deleted object's id
     *         if the object was found and sucesfully deleted.
     *         {@link org.ajmm.obsearch.Result#NOT_EXISTS} if the object is not
     *         in the database.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    protected abstract Result deleteAux(final O object)
            throws DatabaseException, OBException, IllegalAccessException,
            InstantiationException;

    /**
     * Copies all the values already inserted in B into C. This is only for
     * efficiency reasons as we could easily re-insert all the objects into C.
     * But this takes some time.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */
    protected abstract void insertFromBtoC() throws DatabaseException,
            OutOfRangeException;

    /**
     * Children of this class have to implement this method if they want to
     * calculate parameters based on the data in B.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */
    protected abstract void calculateIndexParameters()
            throws DatabaseException, IllegalAccessException,
            InstantiationException, OutOfRangeException, OBException;

    /**
     * Returns the given object from DB A.
     * @param id
     *                The internal id of the object.
     * @return the object
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    public final O getObject(final int id) throws DatabaseException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException {
        return cache.get(id);
    }
    
    private class ALoader implements OBCacheLoader<O>{

        public int getDBSize() throws DatabaseException{
            return (int)aDB.count();
        }

        public O loadObject(int i) throws DatabaseException, OutOfRangeException, OBException, InstantiationException , IllegalAccessException {            
            return getObject(i, aDB);
        }
        
    }

    /**
     * Stores the given pivots in a local array. Takes the pivots from the
     * database using the given ids.
     * @param ids
     *                Ids of the pivots that will be stored.
     * @throws IllegalIdException
     *                 If the pivot selector generates invalid ids
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    public void storePivots(final int[] ids) throws IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Pivots selected " + Arrays.toString(ids));

        }
        createPivotsArray();
        assert ids.length == pivots.length && pivots.length == this.pivotsCount;
        int i = 0;
        while (i < ids.length) {
            O obj = getObject(ids[i], aDB);
            TupleOutput out = new TupleOutput();
            obj.store(out);
            this.pivotsBytes[i] = out.getBufferBytes();
            pivots[i] = obj;
            i++;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Detail: " + Arrays.toString(pivots));
        }
    }

    /**
     * Loads the pivots from the pivots array {@link #pivotsBytes} and puts them
     * in {@link #pivots}.
     * @throws NotFrozenException
     *                 if the freeze method has not been invoqued.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    protected void loadPivots() throws NotFrozenException, DatabaseException,
            IllegalAccessException, InstantiationException, OBException {

        createPivotsArray();
        if (!isFrozen()) {
            throw new NotFrozenException();
        }

        int i = 0;
        while (i < pivotsCount) {
            TupleInput in = new TupleInput(this.pivotsBytes[i]);
            O obj = this.instantiateObject();
            obj.load(in);
            pivots[i] = obj;
            i++;
        }
        assert i == pivotsCount; // pivot count and read # of pivots
        // should be the same
        if (logger.isDebugEnabled()) {
            logger.debug("Loaded " + i + " pivots, pivotsCount:" + pivotsCount);
        }

    }

    

    /**
     * Gets the object with the given id from the database.
     * @param id
     *                The id to be extracted
     * @param db
     *                the database to be accessed the object will be returned
     *                here
     * @return the object the user asked for
     * @throws IllegalIdException
     *                 if the given id does not exist in the database
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    private final O getObject(final int id, final Database db)
            throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException, OBException {
        // TODO: Optimization: put these two objects in the class so that they
        // don't have to
        // be created over and over again
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        IntegerBinding.intToEntry(id, keyEntry);

        O object;

        if (db.get(null, keyEntry, dataEntry, null) == OperationStatus.SUCCESS) {
            TupleInput in = new TupleInput(dataEntry.getData());
            object = this.readObject(in);
        } else {
            throw new IllegalIdException();
        }
        return object;
    }

    /**
     * This method is only used before freeze, to keep track of objects that
     * have not been already inserted.
     * @param ob
     *                The object we want to Load
     * @return the object's id if the object is there, otherwise -1
     */
    private int existsObjectBeforeFreeze(O ob) throws DatabaseException {
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry dataEntry = new DatabaseEntry();
        TupleOutput out = new TupleOutput();
        ob.store(out);
        keyEntry.setData(out.getBufferBytes());
        int res = -1;
        if (kDB.get(null, keyEntry, dataEntry, null) == OperationStatus.SUCCESS) {
            TupleInput in = new TupleInput(dataEntry.getData());
            res = in.readInt();
        }
        return res;
    }

    /**
     * Creates a new and empty O object.
     * @return A new empty object of type O
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    protected O instantiateObject() throws IllegalAccessException,
            InstantiationException {
        // Find out if java can give us the type information directly from the
        // template parameter. There should be a way...
        return type.newInstance();
    }

    /**
     * Returns the pivots array {@link #pivots}.
     * @return The pivots of the index.
     */
    public O[] getPivots() {
        return this.pivots;
    }

    /**
     * Returns the current amount of pivots.
     * @return The number of pivots used.
     */
    public short getPivotsCount() {
        return this.pivotsCount;
    }

    /**
     * Returns true if the database has been frozen.
     * @return true if the database has been frozen
     */
    public boolean isFrozen() {
        return this.frozen;
    }

    /**
     * Closes database C.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     */
    protected abstract void closeC() throws DatabaseException;

    /**
     * Closes all the databases and the database environment.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     */
    public void close() throws DatabaseException {
        
       

        databaseEnvironment.cleanLog();
        databaseEnvironment.compress();
        databaseEnvironment.checkpoint(null);
        aDB.close();
        if (bDB != null) {
            bDB.sync();
            bDB.close();
        }
        closeC();
        if (kDB != null) {
            kDB.sync();
            kDB.close();
        }
        
        databaseEnvironment.close();

        if (databaseEnvironmentCreation != null) {
            databaseEnvironmentCreation.cleanLog();
            databaseEnvironmentCreation.compress();
            databaseEnvironmentCreation.checkpoint(null);
            this.databaseEnvironmentCreation.close();
        }
    }

    /**
     * Reads an object from the given tupleinput.
     * @param in
     *                Byte stream where the object will be read from
     * @return An object O generated from in.
     */
    public O readObject(final TupleInput in) throws InstantiationException,
            IllegalAccessException, OBException {
        O result = this.instantiateObject();
        result.load(in);
        return result;
    }

    /**
     * This method returns the corresponding float value of the distance of the
     * given objects
     * @param a
     *                First object to compare
     * @param b
     *                Second object to compare
     * @return A normalized (first pass) value of the distance of a and b
     */
    public abstract float distance(O a, O b) throws OBException;

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Index#getStats()
     */
    @Override
    public String getStats() {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Index#resetStats()
     */
    @Override
    public void resetStats() {
        // TODO Auto-generated method stub
        
    }
    
    
    

}
