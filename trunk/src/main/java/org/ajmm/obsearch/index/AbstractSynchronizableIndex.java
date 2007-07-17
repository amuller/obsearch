package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.SynchronizableIndex;
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
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

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
	protected transient Database insertTimeDB;
	
	/**
	 * stores the latest modification to the data
	 */
	protected transient long[] timeByBox;
	
	protected File dbDir;
	
	public AbstractSynchronizableIndex(Index<O> index, File dbDir) throws DatabaseException{
		this.dbDir = dbDir;
		initDB();
		timeByBox = new long[index.totalBoxes()];
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
		envConfig.setTransactional(false);
		envConfig.setCacheSize(20 * 1024 * 1024); // 20 MB
		// envConfig.setTxnNoSync(true);
		// envConfig.setTxnWriteNoSync(true);
		// envConfig.setLocking(false);
		this.databaseEnvironment = new Environment(dbDir, envConfig);

		dbConfig = new DatabaseConfig();
		dbConfig.setTransactional(false);
		dbConfig.setAllowCreate(true);
		dbConfig.setSortedDuplicates(true);
		dbConfig.setBtreeComparator(IntLongComparator.class);
		//dbConfig.setDuplicateComparator(IntLongComparator.class);
		// dbConfig.setExclusiveCreate(true);
	}
	
	/**
	 * Initializes time database
	 * @throws DatabaseException
	 */
	private void initInsertTime() throws DatabaseException{
		insertTimeDB = databaseEnvironment.openDatabase(null, "insertTime", dbConfig);
	}
	
	

	public int totalBoxes() {
		return getIndex().totalBoxes();
	}

	public void close() throws DatabaseException {
		getIndex().close();
	}

	public int delete(O object) throws NotFrozenException, DatabaseException {
		// TODO Auto-generated method stub
		return -1;
	}

	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			DatabaseException, OutOfRangeException, OBException,
			UndefinedPivotsException {
		getIndex().freeze();
		// after we freeze, we have to insert our data
		int i =0;
		int max = getIndex().databaseSize();
		while(i < max){
			O object = getIndex().getObject(i);
			assert object != null;
			int box = getIndex().getBox(object);
			insertTimeEntry(box, System.currentTimeMillis(),  i);
			i++;
		}
		assert i == this.getIndex().databaseSize();
		assert this.insertTimeDB.count() == this.getIndex().databaseSize() : "time: " + insertTimeDB.count() + " the rest: " + getIndex().databaseSize();
	}

	public O getObject(int i) throws DatabaseException, IllegalIdException,
			IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return getIndex().getObject(i);
	}
    // problem with the freezing. we don't know which box they belong too.
	public int insert(O object) throws IllegalIdException, DatabaseException,
			OBException, IllegalAccessException, InstantiationException {
		int id = getIndex().insert(object);
		
		if(isFrozen()){
			int box = getIndex().getBox(object);
			insertTimeEntry(box, System.currentTimeMillis(), id);
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
	protected void insertTimeEntry(int box, long time, int id) throws DatabaseException{
		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();

		keyEntry.setData(boxTimeToByteArray(box,time));
		IntegerBinding.intToEntry(id, dataEntry);
		OperationStatus ret = insertTimeDB.put(null, keyEntry, dataEntry);
		if(ret != OperationStatus.SUCCESS){
			throw new DatabaseException();
		}		
	}
	
	
	
	public long latestModification(int box) throws DatabaseException, OBException{
		if(timeByBox[box] == 0){ // 0 is the unitialized value.
			timeByBox[box] = latestInsertedItemAux(box);
		}
		return this.timeByBox[box];
	}
	
	private long latestInsertedItemAux(int box) throws DatabaseException, OBException {
		//TODO: cache these values, and load them only once at start-up
		// this method is very expensive in terms of I/O
		Cursor cursor = null;
		DatabaseEntry key = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		long resTime = -1;
		try {
			cursor = insertTimeDB.openCursor(null, null);			
			key.setData(boxTimeToByteArray(box, resTime));			
			OperationStatus retVal = cursor.getSearchKeyRange(key, foundData, LockMode.DEFAULT);
			MyTupleInput in = new MyTupleInput();
			int cbox = box;
			while (retVal == OperationStatus.SUCCESS && cbox == box) {
				in.setBuffer(key.getData());
				cbox = in.readInt();
				long time = in.readLong();
				assert resTime <= time;
				resTime = time;
				retVal = cursor.getNext(key, foundData, null);				
			}
		} finally {
			cursor.close();
		}
		return resTime;
	}
	
	private byte[] boxTimeToByteArray(int box, long time){
		TupleOutput data = new TupleOutput();
		data.writeInt(box);
		data.writeLong(time);
		return data.getBufferBytes();
	}

	public Iterator<O> elementsNewerThan(int box, long time) throws DatabaseException {
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

	protected class TimeStampIterator implements Iterator {
		
		private Cursor cursor = null;

		private DatabaseEntry keyEntry = new DatabaseEntry();

		private DatabaseEntry dataEntry = new DatabaseEntry();

		private OperationStatus retVal;

		private MyTupleInput in;
		
		private int box;
		
		private int cbox = -1;

		private long previous = Long.MIN_VALUE;
		
		private int count = 0;

		public TimeStampIterator(int box, long from) throws DatabaseException {
			cursor = insertTimeDB.openCursor(null, null);
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
		
		public boolean hasNext() {
			boolean res = (retVal == OperationStatus.SUCCESS) && cbox == box;
			if (!res) {
				try {
					cursor.close();
				} catch (Exception e) {
					assert false;
					logger.fatal(e);
				}
			}
			return res;
		}

		public O next() {
			try {
				if (hasNext()) {
					TupleInput inl = new TupleInput(dataEntry.getData());
					
					int id = inl.readInt();
					O obj = getObject(id);
					goNext();
					return obj;
				} else {
					assert false : "You should be calling hasNext before calling next.";
					return null;
				}
			} catch (Exception e) {
				assert false : " Error: " + e;
				return null;
			}
		}

		/**
		 * The remove method is not implemented. Please do not use it.
		 */
		public void remove() {
		}

	}


}
