package org.ajmm.obsearch.index;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.AbstractOBResult;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.LongBinding;
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
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryCursor;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.sleepycat.je.StatsConfig;
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
 * Subclasses must populate correctly the timestamp B-tree.
 * This B-tree contains this information:
 * key: <box><timestamp>  value: <id>
 * And the subclass has to fill this in.
 * @param <O>
 *            The object type to be used
 * @param <D>
 *            The dimension type to be used
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */
@XStreamAlias("AbstractPivotIndex")
public abstract class AbstractPivotIndex<O extends OB> implements
		Index<O> {

	private static transient final Logger logger = Logger
			.getLogger(AbstractPivotIndex.class);

	private File dbDir;

	protected short pivotsCount;

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

	/**
	 * Keeps track of the inserted ids.
	 */
	protected transient AtomicInteger id;

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
	public AbstractPivotIndex(final File databaseDirectory, final short pivots)
			throws DatabaseException, IOException {
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
		initDB();
	}

	/**
	 * Creates an array with the pivots It has to be created like this because
	 * we are using generics
	 */
	protected void createPivotsArray() {
		this.pivots = emptyPivotsArray();
	}

	public O[] emptyPivotsArray() {
		return (O[]) Array.newInstance(type, pivotsCount);
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
	
	

	private void initB() throws DatabaseException {
		final boolean duplicates = dbConfig.getSortedDuplicates();
		dbConfig.setSortedDuplicates(false);
		bDB = databaseEnvironment.openDatabase(null, "B", dbConfig);
		dbConfig.setSortedDuplicates(duplicates);
	}

	/**
	 * This method will be called by the super class Initializes the C *
	 * database(s)
	 */
	protected abstract void initC() throws DatabaseException;

	/**
	 * This method is called by xstream when all the serialized fields have been
	 * populated
	 * 
	 * @return
	 * @throws DatabaseException
	 * @throws NotFrozenException
	 * @throws DatabaseException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	protected Object initializeAfterSerialization() throws DatabaseException,
			NotFrozenException, DatabaseException, IllegalAccessException,
			InstantiationException {
		if (logger.isDebugEnabled()) {
			logger
					.debug("Initializing transient fields after de-serialization");
		}
		initDB();
		loadPivots();
		initCache();
		initC();
		// restore the ids
		id = new AtomicInteger(this.databaseSize());
		return this.returnSelf();
	}
	
	public void relocateInitialize(File dbPath) throws DatabaseException,
	NotFrozenException, DatabaseException, IllegalAccessException,
	InstantiationException{
		if(dbPath != null){
			this.dbDir = dbPath;
		}
		initializeAfterSerialization();
	}

	// private abstract Object readResolve() throws DatabaseException;

	protected void initCache() throws DatabaseException {
		if (cache == null) {
			int size = databaseSize();
			cache = new OBCache<O>(size);
		}
	}

	public int databaseSize() throws DatabaseException {
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
	 * Class used to create the key that will be used to index the objects by
	 * time
	 */
	private class TimeStampCreator implements SecondaryKeyCreator {
		public boolean createSecondaryKey(SecondaryDatabase secondary,
				DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
			TupleInput in = new TupleInput(data.getData());
			LongBinding.longToEntry(in.readLong(), result);
			return true;
		}
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
		envConfig.setCacheSize(300 * 1024 * 1024); // 80 MB
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

	public void stats() throws DatabaseException {
		StatsConfig config = new StatsConfig();
		config.setClear(true);
		logger.info(databaseEnvironment.getStats(config));
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

	/**
	 * Inserts the given object into the index with the given ID If the given ID
	 * already exists, the exception IllegalIDException is thrown.
	 * 
	 * @param object
	 *            The object to be added
	 * @param id
	 *            Identification number of the given object. This number must be
	 *            responsibly generated by someone
	 * @return 0 if the object already existed or 1 if the object was inserted
	 * @throws IllegalIdException
	 *             if the given ID already exists or if isFrozen() = false and
	 *             the ID's did not come in sequential order
	 * @throws DatabaseException
	 *             If something goes wrong with the DB
	 * @since 0.0
	 */
	public int insert(O object) throws IllegalIdException, DatabaseException,
			OBException, IllegalAccessException, InstantiationException {
		return insert(object, id.getAndIncrement());
	}

	public int  insert(O object, int id) throws IllegalIdException,
			DatabaseException, OBException, IllegalAccessException,
			InstantiationException {
		if (isFrozen()) {
			insertA(object, id);
			insertFrozen(object, id);
		} else {
			insertUnFrozen(object, id);
		}
		return id;
	}

	/**
	 * Inserts in database A the given Object.
	 * The timestamp of the object is stored too.
	 * @param object
	 * @param id
	 * @throws DatabaseException
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
	 * 
	 * @param id
	 * @param object
	 * @throws DatabaseException
	 */
	protected abstract void insertInB(int id, O object) throws OBException,
			DatabaseException;

	/*
	 * protected void insertTupleInB(int id, D[] tuple) throws
	 * DatabaseException{ DatabaseEntry keyEntry = new DatabaseEntry();
	 * DatabaseEntry dataEntry = new DatabaseEntry(); TupleOutput out = new
	 * TupleOutput(); // write the tuple for (D d : tuple) { d.store(out); } //
	 * store the ID IntegerBinding.intToEntry(id, keyEntry);
	 * insertInDatabase(out, keyEntry, bDB); }
	 */

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
			OBException, IllegalAccessException, InstantiationException;

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
	 * Pivot selectors who use the cache (access the objects of the DB) should
	 * call this method before calling freeze. Users do not have to worry about
	 * this method
	 */
	public void prepareFreeze() throws DatabaseException {
		initCache();
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
	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			DatabaseException, OutOfRangeException, OBException,
			UndefinedPivotsException {
		if (isFrozen()) {
			throw new AlreadyFrozenException();
		}

		if (pivots == null) {
			throw new UndefinedPivotsException();
		}

		if (logger.isDebugEnabled()) {
			logger.debug("Storing pivot tuples from A to B");
		}
		//		 cache is initialized as from the point we set frozen = true
		// queries can be achieved
		initCache();
		
		// we have to create database B
		insertFromAtoB();

		calculateIndexParameters(); // this must be done by the subclasses

		// we have to insert the objects already inserted in A into C
		logger.info("Copying data from B to C");
		insertFromBtoC();
		
		// we could delete bDB from this point

		this.frozen = true;
		// queries can be executed from this point

		String xml = toXML();
		FileWriter fout = new FileWriter(this.dbDir.getPath() + File.separator
				+ getSerializedName());
		fout.write(xml);
		fout.close();

		assert aDB.count() == bDB.count();
		
	}

	/**
	 * Returns the current maximum id
	 * 
	 * @return
	 */
	public int getMaxId() {
		return this.maxId;
	}

	/**
	 * Must return "this" Used to serialize the object
	 */
	protected abstract Index returnSelf();

	/**
	 * Inserts all the values already inserted in A into B
	 * 
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
	
	public String toXML(){
		XStream xstream = new XStream();		
		String xml = xstream.toXML(returnSelf());
		return xml;
	}

	/**
	 * Inserts all the values already inserted in A into C
	 * 
	 */
	protected abstract void insertFromBtoC() throws DatabaseException,
			OutOfRangeException;

	/*
	 * private void insertFromBtoC() throws IllegalAccessException,
	 * InstantiationException, DatabaseException, OBException { Cursor cursor =
	 * null; DatabaseEntry foundKey = new DatabaseEntry(); DatabaseEntry
	 * foundData = new DatabaseEntry(); try { int i = 0;
	 * 
	 * cursor = aDB.openCursor(null, null); O obj = this.instantiateObject();
	 * while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) ==
	 * OperationStatus.SUCCESS) { int id = IntegerBinding.entryToInt(foundKey);
	 * assert i == id; TupleInput in = new TupleInput(foundData.getData());
	 * obj.load(in); insertFrozen(obj, id); i++; } // should be the same }
	 * finally { cursor.close(); } }
	 */

	/**
	 * Children of this class have to implement this method if they want to
	 * calculate extra parameters
	 */
	protected abstract void calculateIndexParameters()
			throws DatabaseException, IllegalAccessException,
			InstantiationException, OutOfRangeException, OBException;

	/**
	 * Inserts the given tuple in the database B using the given id B database
	 * will hold tuples of floats (they have been normalized)
	 * 
	 * @param id
	 *            the id to use for the insertion
	 * @param tuple
	 *            The tuple to be inserted
	 */
	/*
	 * protected void insertPivotTupleInBDB(int id, D[] tuple) throws
	 * DatabaseException { TupleOutput out = new TupleOutput(); int i = 0;
	 * assert tuple.length == pivotsCount; // first encode all the dimensions in
	 * an array while (i < tuple.length) { tuple[i].store(out); // stores the
	 * given pivot i++; } // now we can store it in the database DatabaseEntry
	 * keyEntry = new DatabaseEntry(); IntegerBinding.intToEntry(id, keyEntry);
	 * this.insertInDatabase(out, keyEntry, bDB); }
	 */

	/**
	 * returns the given object from DB A
	 * 
	 * @param id
	 * @return the object
	 */
	// TODO: need to implement a cache here
	public O getObject(int id) throws DatabaseException, IllegalIdException,
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
	/*
	 * protected void calculatePivotTuple(final O obj, D[] tuple) throws
	 * OBException { assert tuple.length == this.pivotsCount; int i = 0; while
	 * (i < tuple.length) { obj.distance(this.pivots[i], tuple[i]); i++; } }
	 */

	/**
	 * Stores the given pivots
	 * 
	 * @throws IllegalIdException
	 *             If the pivot selector generates invalid ids
	 */
	public void storePivots(int[] ids) throws IllegalIdException,
			IllegalAccessException, InstantiationException, DatabaseException {
		if (logger.isDebugEnabled()) {
			logger.debug("Pivots selected " + Arrays.toString(ids));
		}
		createPivotsArray();
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
		createPivotsArray();
		if (!isFrozen()) {
			throw new NotFrozenException();
		}
		try {
			int i = 0;
			cursor = pivotsDB.openCursor(null, null);

			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				assert i == IntegerBinding.entryToInt(foundKey);
				TupleInput in = new TupleInput(foundData.getData());
				O obj = this.instantiateObject();
				obj.load(in);
				pivots[i] = obj;
				// if(logger.isDebugEnabled()){
				// logger.debug("Loaded pivot: " + obj);
				// }
				i++;
			}
			assert i == pivotsCount; // pivot count and read # of pivots
			// should be the same
			if (logger.isDebugEnabled()) {
				logger.debug("Loaded " + i + " pivots, pivotsCount:"
						+ pivotsCount);
			}
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

		O object;

		if (DB.get(null, keyEntry, dataEntry, null) == OperationStatus.SUCCESS) {
			// TODO: Extend TupleInput so that we don't have to create the
			// object over and over again
			TupleInput in = new TupleInput(dataEntry.getData());
			object  = this.readObject(in);
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
	public short getPivotsCount() {
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

	/**
	 * Closes database C
	 * 
	 */
	protected abstract void closeC() throws DatabaseException;

	/**
	 * Closes all the databases and the database environment
	 */
	public void close() throws DatabaseException {
		aDB.close();
		bDB.close();
		closeC();
		pivotsDB.close();
		databaseEnvironment.cleanLog();
		databaseEnvironment.close();
	}
	
		
	/**
	 * Reads an object from the given tupleinput
	 * @param in
	 * @return
	 */
	protected O readObject(TupleInput in) throws InstantiationException, IllegalAccessException{
		O result = this.instantiateObject();
		result.load(in);
		return result;
	}
	
	

}
