package org.ajmm.obsearch.index;

import java.io.File;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.index.pivotselection.PivotSelector;
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
 * function and the objects must be consistent.
 * 
 * @param <O>
 *            The object type to be used
 * @param <D>
 *            The dimension type to be used
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */

public abstract class AbstractPivotIndex<O extends OB, D> implements
		Index<O, D> {

	private static final Logger logger = Logger
			.getLogger(AbstractPivotIndex.class);

	private File dbDir;

	private byte pivotsCount;

	private boolean frozen;

	private int maxId;

	private PivotSelector pivotSelector;

	protected Environment databaseEnvironment;

	protected Database A;

	protected DatabaseConfig dbConfig;
	
	protected transient O[] pivots;

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
	 * @throws DatabaseException
	 */
	public AbstractPivotIndex(final File databaseDirectory, final byte pivots)
			throws DatabaseException {
		this.dbDir = databaseDirectory;
		if(dbDir.exists()){
			if(pivots != this.pivotsCount){
				throw new IllegalArgumentException("The pivots value do not match. Database: "  + this.pivotsCount + " given value: " + pivots);
			}
			// load the pivots array
		}else{
			
			dbDir.mkdirs(); // create the directory		
			// TODO check if the index exists, then load the values from the
			// database, or
			// figure out how to do this from
			// outside. Maybe we can just serialize this object once it is frozen.
			this.pivotsCount = pivots;
			frozen = false;
			maxId = 0;

			initBerkeleyDB();
			// way of creating a database
			createA();
			
		}
	}

	/**
	 * Creates database A.
	 * 
	 * @throws Exception
	 */
	private void createA() throws DatabaseException {
		final boolean duplicates = dbConfig.getSortedDuplicates();
		dbConfig.setSortedDuplicates(false);
		A = databaseEnvironment.openDatabase(null, "A", dbConfig);
		dbConfig.setSortedDuplicates(duplicates);
	}

	/**
	 * This method makes sure that all the databases are created with the same
	 * settings TODO: Need to tweak these values
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
		if (id != (maxId + 1)) {
			throw new IllegalIdException();
		}
		maxId = id;
		insertA(object, id);
		return 1;
	}

	public byte insert(O object, int id) throws IllegalIdException,
			DatabaseException {
		if (isFrozen()) {
			return insertUnFrozen(object, id);
		} else {
			return insertFrozen(object, id);
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
		insertInDatabase(object,id,A);
	}
	
	/**
	 * Inserts the given object with the given Id in the database x
	 * @param object object to be inserted
	 * @param id id for the object
	 * @param x Database to be used
	 * @throws DatabaseException
	 */
	protected void insertInDatabase(final O object, final int id, Database x) throws DatabaseException{
		final DatabaseEntry keyEntry = new DatabaseEntry();
		final DatabaseEntry dataEntry = new DatabaseEntry();
		// store the object in bytes
		final TupleOutput out = new TupleOutput();
		object.store(out);
		dataEntry.setData(out.getBufferBytes());
		// store the ID
		IntegerBinding.intToEntry(id, keyEntry);
		x.put(null, keyEntry, dataEntry);
	}

	/**
	 * Utility method to insert data after freezing. Must be implemented by the
	 * subclasses
	 * 
	 * @param object
	 *            The object to be inserted
	 * @param id
	 *            The id to be added
	 * @throws IllegalIdException
	 *             if the id already exists
	 * @return 1 if successful 0 otherwise
	 */
	protected abstract byte insertFrozen(O object, int id)
			throws IllegalIdException;

	/**
	 * Freezes the index. From this point data can be inserted, searched and
	 * deleted The index might deteriorate at some point so every once in a
	 * while it is a good idea to rebuild de index
	 * 
	 * @param pivotSelector
	 *            The pivot selector to be used
	 */
	public void freeze(PivotSelector pivotSelector) {
		int[] ids = pivotSelector.generatePivots(pivotsCount, maxId);
		
		// at the end we save this object into the registry
	}

	/**
	 * Gets the given object from the database.
	 * 
	 * @param id
	 * @param object
	 *            the object will be returned here
	 * @return the object the user asked or null if the id is not in the
	 *         database
	 * @throws DatabaseException
	 * @throws IllegalIdException
	 *             if the given id does not exist in the database
	 */
	public void getObject(int id, O object) throws DatabaseException,
			IllegalIdException {
		// TODO: put these two objects in the class so that they don't have to be created over and over again
		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();
		IntegerBinding.intToEntry(id, keyEntry);
		if (A.get(null, keyEntry, dataEntry, null) == OperationStatus.SUCCESS) {
			// TODO: Extend TupleInput so that we don't have to create the object over and over again
			TupleInput in = new TupleInput(dataEntry.getData());
			object.load(in); // load the bytes into the object
		} else {
			throw new IllegalIdException();
		}
	}

	//public O[] getPivotObjects() {

	//}

}
