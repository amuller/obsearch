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
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.TimeStampResult;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.ajmm.obsearch.index.sync.IntLongComparator;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
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
import com.sleepycat.je.TransactionConfig;

/**
 * This class wraps an standard index, and allows the user to
 * obtain information regarding the most recent
 * insertions and deletions. The idea is to used this index in a
 * distributed environment
 * @author amuller
 *
 * @param <O>
 */
public abstract class AbstractSynchronizableIndex<O extends OB> implements SynchronizableIndex<O> {
	
	private static final transient Logger logger = Logger
	.getLogger(AbstractSynchronizableIndex.class);

	protected transient Environment databaseEnvironment;
	
	protected transient DatabaseConfig dbConfig;
	
	// this database holds a view of aDB based on timestamps
	protected transient Database timeDB;

	
	/**
	 * stores the latest modification to the data
	 */
	protected transient AtomicLongArray timeByBox;
	
	protected File dbDir;
	
	protected transient AtomicIntegerArray objectsByBox;

	public AbstractSynchronizableIndex(Index<O> index, File dbDir) throws DatabaseException{
		this.dbDir = dbDir;
		initDB();
		timeByBox = new AtomicLongArray(index.totalBoxes());
		objectsByBox = new AtomicIntegerArray(index.totalBoxes());
		int i =0;
		while(i < index.totalBoxes()){
			objectsByBox.set(i, -1);
			i++;
		}
	}
	
	private void initDB()throws DatabaseException{
		initBerkeleyDB();
		initInsertTime();
	}
	
	public abstract Index<O> getIndex();
	
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
		envConfig.setTransactional(true);
		envConfig.setCacheSize(20 * 1024 * 1024); // 20 MB
		// envConfig.setTxnNoSync(true);
		// envConfig.setTxnWriteNoSync(true);
		// envConfig.setLocking(false);
		this.databaseEnvironment = new Environment(dbDir, envConfig);

		dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(true);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(true);
		//dbConfig.setBtreeComparator(IntLongComparator.class);
		//dbConfig.setDuplicateComparator(IntLongComparator.class);
		// dbConfig.setExclusiveCreate(true);
	}
	
	/**
	 * Initializes time database
	 * @throws DatabaseException
	 */
	private void initInsertTime() throws DatabaseException{
		timeDB = databaseEnvironment.openDatabase(null, "insertTime", dbConfig);
	}
	
	
	
	public int totalBoxes() {
		return getIndex().totalBoxes();
	}

	public void close() throws DatabaseException {
		getIndex().close();
	}

	public int delete(O object) throws IllegalIdException, DatabaseException,
	OBException, IllegalAccessException, InstantiationException {
		// TODO: update the objects count array.
		return delete(object, System.currentTimeMillis());
	}

	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			DatabaseException, OutOfRangeException, OBException,
			UndefinedPivotsException {
		getIndex().freeze();
		// after we freeze, we have to insert our data
		int i =0;
		int max = getIndex().databaseSize();
		int [] boxes = new int[this.totalBoxes()];
		while(i < max){
			O object = getIndex().getObject(i);
			assert object != null;
			int box = getIndex().getBox(object);
			boxes[box] += 1;
			insertInsertEntry(box, System.currentTimeMillis(),  i );
			i++;
		}
		if(logger.isDebugEnabled()){
			logger.debug("Boxes distribution:" + Arrays.toString(boxes));
		}
		assert i == this.getIndex().databaseSize();
		assert this.timeDB.count() == this.getIndex().databaseSize() : "time: " + timeDB.count() + " the rest: " + getIndex().databaseSize();
	}

	public O getObject(int i) throws DatabaseException, IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {
		return getIndex().getObject(i);
	}
	
	public int insert(O object) throws IllegalIdException, DatabaseException,
			OBException, IllegalAccessException, InstantiationException {
		return insert(object, System.currentTimeMillis());
	}
	
	// FIXME time cannot be 0
	public int insert(O object, long time) throws IllegalIdException, DatabaseException,
			OBException, IllegalAccessException, InstantiationException {
		int id = getIndex().insert(object);
		if(id != -1){ // if we could insert the object
			if(isFrozen()){
				int box = getIndex().getBox(object);
				insertInsertEntry(box, time, id);
				 incElementsPerBox(box);
			}
		}
		return id;
	}
	
	public int delete(O object, long time) throws IllegalIdException, DatabaseException,
	OBException, IllegalAccessException, InstantiationException {
	    int id = getIndex().delete(object);
	    if(id != -1){ // if we could delete the object
		if(isFrozen()){
		    int box = getIndex().getBox(object);
		    insertDeleteEntry(box, time, object);
		    decElementsPerBox(box);
		}
	    }
	    return id;
	}

	public boolean isFrozen() {		
		return getIndex().isFrozen();
	}
	
	/**
	 * Adds a time entry to the database
	 * Key: box + time
	 * Value: id
	 * @param box
	 * @param time
	 * @param id
	 * @return
	 */
	protected void insertInsertEntry(int box, long time, int id) throws DatabaseException ,  OBException{
		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();

		keyEntry.setData(boxTimeToByteArray(box,time));
		TupleOutput out = new TupleOutput();
		out.writeBoolean(true);
		out.writeInt(id);
		dataEntry.setData(out.getBufferBytes());
		OperationStatus ret = timeDB.put(null, keyEntry, dataEntry);
		if(ret != OperationStatus.SUCCESS){
			throw new DatabaseException();
		}	
		// update the cache, make sure that the latest
		// time is updated automatically
		synchronized(timeByBox){
			if(timeByBox.get(box) < time){
				timeByBox.set(box, time);
			}
		}
		
	}
	/**
	 * Stores the given object into deleteTimeDB using the appropiate key
	 * (box + time). In the case of insertTimeDB we insert only the internal
	 * object id, but in the case of deletes, we store all the object
	 * @param box
	 * @param time
	 * @param object
	 * @throws DatabaseException
	 * @throws OBException
	 */
	protected void insertDeleteEntry(int box, long time, O object) throws DatabaseException ,  OBException{
		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();

		keyEntry.setData(boxTimeToByteArray(box,time));
		TupleOutput out = new TupleOutput();
		out.writeBoolean(false);
		object.store(out);
		dataEntry.setData(out.getBufferBytes());
		OperationStatus ret = timeDB.put(null, keyEntry, dataEntry);
		if(ret != OperationStatus.SUCCESS){
			throw new DatabaseException();
		}	
		// update the cache, make sure that the latest
		// time is updated automatically
		synchronized(timeByBox){
			if(timeByBox.get(box) < time){
				timeByBox.set(box, time);
			}
		}
		
	}
	
	private void incElementsPerBox(int box) throws DatabaseException,  OBException{
	    if(objectsByBox.get(box) == -1){
		elementsPerBox(box);
	    }
	    objectsByBox.incrementAndGet(box);
	}
	
	private void decElementsPerBox(int box) throws DatabaseException,  OBException{
	    if(objectsByBox.get(box) == -1){
		elementsPerBox(box);
	    }
	    objectsByBox.decrementAndGet(box);
	}
	
	public long latestModification(int box) throws DatabaseException, OBException{
		if(timeByBox.get(box) == 0){ 
			// 0 is the unitialized value.
			timeByBox.set(box, latestInsertedItemAux(box));
		}
		return timeByBox.get(box);
	}
	
	public int elementsPerBox(int box) throws DatabaseException,  OBException{
		if(objectsByBox.get(box) == -1){
			// this updates the box count.
			objectsByBox.set(box, objectsPerBoxAux(box));
		}
		return this.objectsByBox.get(box);
	}
	/**
	 * Counts the number of objects for the given box
	 * @param box the box to be processed 
	 * @return
	 */
	private int objectsPerBoxAux(int box) throws DatabaseException{
		Cursor cursor = null;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		long resTime = -1;
		int count = 0;
		try {
			cursor = timeDB.openCursor(null, null);
			key.setData(boxTimeToByteArray(box, resTime));			
			OperationStatus retVal = cursor.getSearchKeyRange(key, foundData, LockMode.DEFAULT);
			MyTupleInput in = new MyTupleInput();
			int cbox = box;
			while (retVal == OperationStatus.SUCCESS && cbox == box) {
				in.setBuffer(key.getData());
				cbox = in.readInt();
				retVal = cursor.getNext(key, foundData, null);		
				if(cbox == box){
					count++;
				}
			}
		} finally {
		    if(cursor != null){
			cursor.close();
		    }
		}
		return count;
	}

	/**
	 * Obtains the latest inserted item in the given box or -1 if there are
	 * no items. It also calculates the number of boxes available in the index
	 * 
	 * @param box
	 * @return
	 * @throws DatabaseException
	 * @throws OBException
	 */
	private long latestInsertedItemAux(int box) throws DatabaseException, OBException {
	    return latestInsertedItemAuxAux(box,this.timeDB);
	}
	private long latestInsertedItemAuxAux(int box, Database db) throws DatabaseException, OBException {
		Cursor cursor = null;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		long resTime = -1;
		try {
			cursor = db.openCursor(null, null);
			key.setData(boxTimeToByteArray(box, 0));	
			OperationStatus retVal = cursor.getSearchKeyRange(key, foundData, LockMode.DEFAULT);
			MyTupleInput in = new MyTupleInput();
			int cbox = box;
			while (retVal == OperationStatus.SUCCESS && cbox == box) {
				in.setBuffer(key.getData());
				cbox = in.readInt();
				long time = in.readLong();
				assert  validate(resTime, time, cbox, box) : "resTime: " + resTime + " time: " + time;
				if(resTime < time){
				    resTime = time;	
				}
				retVal = cursor.getNext(key, foundData, null);		
			}
		}finally {
		    if(cursor != null){
			cursor.close();
		    }
		}
		return resTime;
	}
	
	/**
	 * Method used to perform a validation from the assert
	 * only if prev is <= next when cbox == box we return true
	 * @param prev
	 * @param next
	 * @param cbox
	 * @param box
	 * @return
	 */
	private boolean validate(long prev, long next, int cbox , int box){
	    if(cbox != box){
		return true;
	    }else{
	    return prev <= next;
	    }
	}
	
	private byte[] boxTimeToByteArray(int box, long time){
		TupleOutput data = new TupleOutput();
		data.writeInt(box);
		data.writeLong(time);
		return data.getBufferBytes();
	}

	public Iterator<TimeStampResult<O>> elementsNewerThan(int box, long time) throws DatabaseException {
		return new TimeStampIterator(box, time);
	}
	

	
	public int getBox(O object) throws OBException{
		return this.getIndex().getBox(object);
	}
	
	public int [] currentBoxes() throws DatabaseException, OBException{
		int i = 0;
		int max = this.totalBoxes();
		List<Integer> result = new LinkedList<Integer>();
		while(i < max){
			if(latestModification(i) != -1){
				result.add(i);
			}
			i++;
		}
		int [] resultArray  = new int[result.size()];
		i = 0;
		Iterator<Integer> it = result.iterator();
		while(it.hasNext()){
			resultArray[i] = it.next();
			i++;
		}
		return resultArray;
	}

	public class TimeStampIterator  implements Iterator {
		
		private Cursor cursor = null;

		private DatabaseEntry keyEntry = new DatabaseEntry();

		private DatabaseEntry dataEntry = new DatabaseEntry();

		private OperationStatus retVal;

		private MyTupleInput in;
		
		private int box;
		
		private int cbox = -1;

		private long previous = Long.MIN_VALUE;
		
		private int count = 0;
		private Transaction txn;
		
		private boolean closeForced = false;
		
		/**
		 * Creates a new TimeStampIterator from the given database
		 * if the parameter idFormat = true then the iterator will extract
		 * the objects from the internal index. If the parameter is false, then
		 * the objects are actually embeded in the record and will be read from there
		 * @param box
		 * @param from
		 * @param db
		 * @param idFormat
		 * @throws DatabaseException
		 */
		public TimeStampIterator(int box, long from) throws DatabaseException {
			this.box = box;

			CursorConfig config = new CursorConfig();
			config.setReadUncommitted(true);
			txn = databaseEnvironment.beginTransaction(null, null);
			cursor = timeDB.openCursor(txn, config);
			
			previous = from;
			keyEntry.setData(boxTimeToByteArray(box,from));
			retVal = cursor.getSearchKeyRange(keyEntry, dataEntry, null);			
			in = new MyTupleInput();
			goNextAux();
		}
		
		// update the data from keyEntry and dataEntry
		private void goNextAux() {
			if(OperationStatus.SUCCESS == retVal){
				in.setBuffer(keyEntry.getData());
				cbox = in.readInt();
				long recent = in.readLong();
				count++;
				assert validate(recent) : "Previous: " + previous + " recent" + recent + "returned: " + count;
				previous = recent;
			}else{
				cbox = -1;
			}			
		}
		
		// validates some general conditions in the data
		private boolean validate(long recent){
			if(box == cbox){
				return  previous <= recent ;		
			}else{
				return true;
			}				
		}

		private void goNext() throws DatabaseException {		
			retVal = cursor.getNext(keyEntry, dataEntry, LockMode.DEFAULT);
			goNextAux();
		}
		
		public void close(){
		    try {
			cursor.close();
			closeForced = true;
		    } catch (DatabaseException e) {
			//logger.fatal(e);
			// if the cursor has been closed already ignore the error
			//assert false;			
		    }
		}
		
		public boolean hasNext() {
			boolean res = (retVal == OperationStatus.SUCCESS) && cbox == box;
			if (!res) {			    
				close();
			}
			return res;
		}

		public TimeStampResult next() {
			try {
				if (hasNext()) {
					TupleInput inl = new TupleInput(dataEntry.getData());
					O obj = null;
					Boolean insert = inl.readBoolean();
					if(insert){
					    int id = inl.readInt();
					    obj = getObject(id);
					}else{
					    obj = getIndex().readObject(inl);
					}
					goNext();
					return new TimeStampResult(obj, previous, insert);
				} else {
					assert false : "You should be calling hasNext before calling next.";
					return null;
				}
			} catch (Exception e) {
			    	logger.fatal("Error while retrieving record" , e);
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

	public void relocateInitialize(File dbPath) throws DatabaseException,
	NotFrozenException, DatabaseException, IllegalAccessException,
	InstantiationException, OBException, IOException{
		getIndex().relocateInitialize(dbPath);
	}
	
	public O readObject(TupleInput in) throws InstantiationException, IllegalAccessException, OBException{
		return getIndex().readObject(in);
	}
	
	public boolean exists(O object)throws DatabaseException, OBException,
	IllegalAccessException, InstantiationException {
		return getIndex().exists(object);
	}
}
