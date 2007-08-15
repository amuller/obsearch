package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.pptree.SpaceTreeLeaf;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.SortedFloatBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

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
 * Class: PPTreeShort Implementation of a P+Tree that stores shorts. We take the
 * burden of maintaining one class per datatype for efficiency reasons.
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */

public class PPTreeShort<O extends OBShort> extends AbstractPPTree<O> implements
	IndexShort<O> {

    private short minInput;

    private short maxInput;

    private static final transient Logger logger = Logger
	    .getLogger(PPTreeShort.class);

    protected transient HashMap<O, OBQueryShort<O>> resultCache;

    protected final static int resultCacheSize = 3000;

    /*
         * Cache used for getting objects from B. Only used before freezing
         */
    private transient OBCache<float[]> bCache;

    public PPTreeShort(File databaseDirectory, short pivots, byte od)
	    throws DatabaseException, IOException {
	this(databaseDirectory, pivots, od, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    public PPTreeShort(File databaseDirectory, short pivots, byte od,
	    short minInput, short maxInput) throws DatabaseException,
	    IOException {
	super(databaseDirectory, pivots, od);
	assert minInput < maxInput;
	this.minInput = minInput;
	this.maxInput = maxInput;
	resultCache = new HashMap<O, OBQueryShort<O>>(3000);
	bCache = new OBCache<float[]>(this.databaseSize());
    }

    @Override
    protected float[] extractTuple(TupleInput in) throws OutOfRangeException {
	int i = 0;
	float[] res = new float[pivotsCount];
	while (i < pivotsCount) {
	    res[i] = normalizeFirstPassAux(in.readShort());
	    i++;
	}
	return res;
    }

    @Override
    public String getSerializedName() {
	// TODO Auto-generated method stub
	return "PPTreeShort";
    }

    protected int distanceValueSizeInBytes() {
	return Short.SIZE / 8;
    }

    /**
         * Method that takes the values already calculated in B and puts them
         * into C This is to save some time when rebuilding the index
         */
    @Override
    protected void insertFromBtoC() throws DatabaseException,
	    OutOfRangeException {

	DatabaseEntry foundKey = new DatabaseEntry();
	DatabaseEntry foundData = new DatabaseEntry();
	Cursor cursor = null;
	long count = super.bDB.count();
	short[] t = new short[pivotsCount];
	try {
	    int i = 0;
	    cursor = bDB.openCursor(null, null);
	    MyTupleInput in = new MyTupleInput();
	    while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
		assert i == IntegerBinding.entryToInt(foundKey);
		// i contains the actual id of the tuple
		in.setBuffer(foundData.getData());
		int cx = 0;
		while (cx < t.length) {
		    t[cx] = in.readShort();
		    cx++;
		}
		this.insertFrozenAux(t, i);
		i++;
	    }
	    // Size reported by the DB and the items
	    // we read should be the same
	    assert i == count;

	} finally {
	    cursor.close();
	}
	assert cDB.count() == bDB.count();

    }

    /**
         * Generates a query rectangle based on the given range and the given
         * tuple. It normalizes the query first level only
         * 
         * @param t
         *                the tuple to be processed
         * @param r
         *                the range
         * @param q
         *                resulting rectangle query
         */

    protected void generateRectangleFirstPass(short[] t, short r, float[][] q)
	    throws OutOfRangeException {
	// create a rectangle query
	int i = 0;
	while (i < q.length) { //
	    q[i][MIN] = normalizeFirstPassAux((short) Math.max(t[i] - r,
		    minInput));
	    q[i][MAX] = normalizeFirstPassAux((short) Math.min(t[i] + r,
		    maxInput));
	    i++;
	}
    }

    public boolean intersects(O object, short r, int box)
	    throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
	// calculate the vector for the object
	short[] t = new short[pivotsCount];
	calculatePivotTuple(object, t);
	// calculate the rectangle
	float[][] qrect = new float[pivotsCount][2];
	generateRectangleFirstPass(t, r, qrect);

	return super.spaceTreeLeaves[box].intersects(qrect);
    }

    public int[] intersectingBoxes(O object, short r)
	    throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
	int max = super.totalBoxes();

	// calculate the vector for the object
	short[] t = new short[pivotsCount];
	calculatePivotTuple(object, t);
	// calculate the rectangle
	float[][] qrect = new float[pivotsCount][2];
	generateRectangleFirstPass(t, r, qrect);
	// obtain the hypercubes that have to be matched
	List<SpaceTreeLeaf> hyperRectangles = new LinkedList<SpaceTreeLeaf>();
	spaceTree.searchRange(qrect, hyperRectangles);
	int[] result = new int[hyperRectangles.size()];
	Iterator<SpaceTreeLeaf> it = hyperRectangles.iterator();
	int i = 0;
	while (it.hasNext()) {
	    SpaceTreeLeaf leaf = it.next();
	    result[i] = leaf.getSNo();
	    i++;
	}
	return result;
    }

    public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
	    throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
	short[] t = new short[pivotsCount];
	// calculate the pivot for the given object
	calculatePivotTuple(object, t);
	float[][] qrect = new float[pivotsCount][2]; // rectangular query
	generateRectangleFirstPass(t, r, qrect);
	List<SpaceTreeLeaf> hyperRectangles = new LinkedList<SpaceTreeLeaf>();
	// obtain the hypercubes that have to be matched

	spaceTree.searchRange(qrect, hyperRectangles);

	searchOBAux(object, r, result, qrect, t, hyperRectangles);
    }

    public void searchOB(O object, short r, OBPriorityQueueShort<O> result,
	    int[] boxes) throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
	short[] t = new short[pivotsCount];
	// calculate the pivot for the given object
	calculatePivotTuple(object, t);
	float[][] qrect = new float[pivotsCount][2]; // rectangular query
	generateRectangleFirstPass(t, r, qrect);
	List<SpaceTreeLeaf> hyperRectangles = new LinkedList<SpaceTreeLeaf>();
	int i = 0;
	int max = boxes.length;
	while (i < max) {
	    hyperRectangles.add(super.spaceTreeLeaves[boxes[i]]);
	    i++;
	}
	searchOBAux(object, r, result, qrect, t, hyperRectangles);
    }

    public void searchOBAux(O object, short r, OBPriorityQueueShort<O> result,
	    float[][] qrect, short[] t, List<SpaceTreeLeaf> hyperRectangles)
	    throws NotFrozenException, DatabaseException,
	    InstantiationException, IllegalIdException, IllegalAccessException,
	    OutOfRangeException, OBException {
	// check if we are frozen
	assertFrozen();

	// check if the result has been processed
	/*
         * OBQueryShort cachedResult = this.resultCache.get(object);
         * if(cachedResult != null && cachedResult.getDistance() == r){
         * 
         * Iterator<OBResultShort<O>> it =cachedResult.getResult().iterator();
         * while(it.hasNext()){ OBResultShort<O> element = it.next();
         * result.add(element.getId(), element.getObject(),
         * element.getDistance()); } return; }
         */
	int pyramidCount = 2 * pivotsCount;

	short myr = r;

	float[] lowHighResult = new float[2];

	Iterator<SpaceTreeLeaf> it = hyperRectangles.iterator();
	// this will hold the rectangle for the current hyperrectangle
	float[][] qw = new float[pivotsCount][2];
	float[][] q = new float[pivotsCount][2];
	while (it.hasNext()) {
	    SpaceTreeLeaf space = it.next();
	    if (!space.intersects(qrect)) {
		continue;
	    }

	    // for each space there are 2d pyramids that have to be browsed
	    int i = 0;
	    // update the current rectangle, we also have to center it
	    space.generateRectangle(qrect, qw);
	    centerQuery(qw); // center the rectangle

	    while (i < pyramidCount) {
		// intersect destroys q, so we have to copy it
		copyQuery(qw, q);
		if (intersect(q, i, lowHighResult)) {
		    int ri = (space.getSNo() * 2 * pivotsCount) + i; // real
		    // index
		    searchBTreeAndUpdate(object, t, myr, ri
			    + lowHighResult[HLOW], ri + lowHighResult[HHIGH],
			    result);

		    short nr = result.updateRange(myr);
		    // make the range shorter
		    if (nr < myr) {
			myr = nr; // regenerate the query with a smaller
			// range
			generateRectangleFirstPass(t, myr, qrect);
			space.generateRectangle(qrect, qw);
			if (!space.intersects(qrect)) {
			    break; // we have to skip the this space if
			    // suddenly we are out of range...
			    // otherwise we would end up
			    // searching all the space for the
			    // rest of the
			    // pyramids!
			}
			centerQuery(qw); // center the rectangle

		    }

		}
		i++;
	    }
	}

	// store the result in the cache
	// this.resultCache.put(object, new OBQueryShort<O>(object,r, result));
    }

    /**
         * This method reads from the B-tree appies l-infinite to discard false
         * positives Calculates the real distance and updates the result
         * priority queue It is left public so that junit can perform
         * validations on it Performance-wise this is one of the most important
         * methods
         * 
         * @param object
         * @param tuple
         * @param r
         * @param hlow
         * @param hhigh
         * @param result
         * @throws DatabaseException
         */
    public void searchBTreeAndUpdate(O object, short[] tuple, short r,
	    float hlow, float hhigh, OBPriorityQueueShort<O> result)
	    throws DatabaseException, IllegalAccessException,
	    InstantiationException, IllegalIdException, OBException {

	Cursor cursor = null;

	try {

	    DatabaseEntry keyEntry = new DatabaseEntry();
	    DatabaseEntry dataEntry = new DatabaseEntry();
	    cursor = cDB.openCursor(null, null);
	    SortedFloatBinding.floatToEntry(hlow, keyEntry);
	    OperationStatus retVal = cursor.getSearchKeyRange(keyEntry,
		    dataEntry, null);

	    if (retVal == OperationStatus.NOTFOUND) {
		return;
	    }

	    if (cursor.count() > 0) {
		float currentPyramidValue = SortedFloatBinding
			.entryToFloat(keyEntry);
		short max = Short.MIN_VALUE;
		short realDistance = Short.MIN_VALUE;
		while (retVal == OperationStatus.SUCCESS
			&& currentPyramidValue <= hhigh) {

		    TupleInput in = new TupleInput(dataEntry.getData());

		    int i = 0;
		    short t;
		    max = Short.MIN_VALUE;
		    // STATS
		    while (i < tuple.length) {
			t = (short) Math.abs(tuple[i] - in.readShort());
			if (t > max) {
			    max = t;
			    if (t > r) {
				break; // finish this loop this slice won't be
				// matched
				// after all!
			    }
			}
			i++;
		    }

		    if (max <= r && result.isCandidate(max)) {
			// there is a chance it is a possible match
			int id = in.readInt();
			O toCompare = super.getObject(id);
			realDistance = object.distance(toCompare);

			if (realDistance <= r) {

			    result.add(id, toCompare, realDistance);
			}
		    }

		    // read the next record
		    retVal = cursor.getNext(keyEntry, dataEntry, null);
		    // update the current pyramid value so that we know when
		    // to
		    // stop
		    currentPyramidValue = SortedFloatBinding
			    .entryToFloat(keyEntry);
		}
	    }
	} finally {
	    cursor.close();
	}
    }

    /*
         * public String toString(){ return " STATS: SMAP # " +
         * this.totalExecutedTimesSMAP + " Avg time: " +
         * ((double)this.accumExecutionTimeSMAP /
         * (double)this.totalExecutedTimesSMAP) + "\n STATS: D # " +
         * this.totalExecutedTimesD + " Avg time: " +
         * ((double)this.accumExecutionTimeD /
         * (double)this.totalExecutedTimesD); }
         */

    @Override
    protected byte insertFrozen(O object, int id) throws IllegalIdException,
	    OBException, DatabaseException, IllegalAccessException,
	    InstantiationException {
	short[] t = new short[pivotsCount];
	calculatePivotTuple(object, t); // calculate the tuple for the new //

	return insertFrozenAux(t, id);
    }

    public int getBox(O object) throws OBException {
	short[] t = new short[pivotsCount];
	calculatePivotTuple(object, t); // calculate the tuple for the new //
	float[] et = normalizeFirstPass(t);
	return super.spaceNumber(et);
    }

    /**
         * Inserts the given tuple and id into C
         * 
         * @param t
         * @param id
         * @return
         * @throws OutOfRangeException
         * @throws DatabaseException
         */
    protected byte insertFrozenAux(short[] t, int id)
	    throws OutOfRangeException, DatabaseException {
	float[] first = normalizeFirstPass(t);
	float ppTreeValue = super.ppvalue(first);
	return insertFrozenAuxAux(ppTreeValue, t, id);
    }
    
    protected byte insertFrozenAuxAux(float ppTreeValue, short[] t, int id) throws DatabaseException{
	DatabaseEntry keyEntry = new DatabaseEntry();
	DatabaseEntry dataEntry = new DatabaseEntry();
	TupleOutput out = new TupleOutput();
	// write the tuple
	for (short d : t) {
	    out.writeShort(d);
	}
	// write the object's id
	out.writeInt(id);
	// create the key
	SortedFloatBinding.floatToEntry(ppTreeValue, keyEntry);
	dataEntry.setData(out.getBufferBytes());

	// TODO: check the status result of all the operations
	if (cDB.put(null, keyEntry, dataEntry) != OperationStatus.SUCCESS) {
	    throw new DatabaseException();
	}
	return 1;
    }

    protected float[] normalizeFirstPass(short[] t) throws OutOfRangeException {
	assert t.length == pivotsCount;
	float[] res = new float[pivotsCount];

	int i = 0;
	while (i < t.length) {
	    res[i] = normalizeFirstPassAux(t[i]);
	    i++;
	}
	return res;
    }

    @Override
    protected void insertInB(int id, O object) throws OBException,
	    DatabaseException {
	DatabaseEntry keyEntry = new DatabaseEntry();
	TupleOutput out = new TupleOutput();
	short[] tuple = new short[pivotsCount];
	calculatePivotTuple(object, tuple);
	// write the tuple
	for (short d : tuple) {
	    out.writeShort(d);
	}
	// store the ID
	IntegerBinding.intToEntry(id, keyEntry);
	insertInDatabase(out, keyEntry, bDB);
    }

    /**
         * Read the given tuple from B database and load it into the given tuple
         * in a normalized form
         * 
         * @param id
         * @param tuple
         */
    protected void readFromB(int id, float[] tuple) throws DatabaseException,
	    OutOfRangeException {
	Cursor cursor = null;
	// check if the tuple is in cache
	float[] temp = this.bCache.get(id);
	if (temp != null) {
	    int i = 0;
	    // copy the contents to the result.
	    while (i < tuple.length) {
		tuple[i] = temp[i];
		i++;
	    }
	    return;
	}
	try {
	    DatabaseEntry keyEntry = new DatabaseEntry();
	    DatabaseEntry dataEntry = new DatabaseEntry();
	    cursor = bDB.openCursor(null, null);
	    IntegerBinding.intToEntry(id, keyEntry);
	    OperationStatus retVal = cursor.getSearchKey(keyEntry, dataEntry,
		    null);

	    if (retVal == OperationStatus.NOTFOUND) {
		assert false : "Trying to read : " + id
			+ " but the database is: " + this.databaseSize();
		return;
	    }

	    assert cursor.count() == 1;

	    TupleInput in = new TupleInput(dataEntry.getData());
	    int i = 0;
	    assert tuple.length == pivotsCount;
	    float[] tempTuple = new float[pivotsCount];
	    while (i < pivotsCount) {
		tuple[i] = normalizeFirstPassAux(in.readShort());
		tempTuple[i] = tuple[i];
		i++;
	    }
	    bCache.put(id, tempTuple);
	} finally {
	    cursor.close();
	}
    }

    /**
         * Normalize the given value This is a first pass normalization, any
         * value to [0,1]
         * 
         * @param x
         * @return the normalized value
         */
    protected float normalizeFirstPassAux(short x) throws OutOfRangeException {
	if (x < minInput || x > maxInput) {
	    throw new OutOfRangeException(minInput + "", maxInput + "", "" + x);
	}
	return ((float) (x - minInput)) / ((float) (maxInput - minInput));
    }

    @Override
    protected Index returnSelf() {
	return this;
    }

    /**
         * Calculates the tuple vector for the given object
         * 
         * @param obj
         *                object to be processed
         * @param tuple
         *                The resulting tuple will be stored here
         */
    protected void calculatePivotTuple(final O obj, short[] tuple)
	    throws OBException {
	assert tuple.length == this.pivotsCount;
	int i = 0;
	while (i < tuple.length) {
	    tuple[i] = obj.distance(this.pivots[i]);
	    i++;
	}
    }

    private Object readResolve() throws DatabaseException, NotFrozenException,
	    DatabaseException, IllegalAccessException, InstantiationException {
	super.initSpaceTreeLeaves();
	resultCache = new HashMap<O, OBQueryShort<O>>(resultCacheSize);
	return this;
    }
    
    public boolean exists(O object) throws DatabaseException, OBException,
	    IllegalAccessException, InstantiationException {	
	
	short[] tuple = new short[pivotsCount];
	// calculate the pivot for the given object
	calculatePivotTuple(object, tuple);
	float[] first = normalizeFirstPass(tuple);

	// now we just have to search this guy
	float ppTreeValue = super.ppvalue(first);

	Cursor cursor = null;

	try {

	    DatabaseEntry keyEntry = new DatabaseEntry();
	    DatabaseEntry dataEntry = new DatabaseEntry();
	    cursor = cDB.openCursor(null, null);
	    SortedFloatBinding.floatToEntry(ppTreeValue, keyEntry);
	    OperationStatus retVal = cursor.getSearchKeyRange(keyEntry,
		    dataEntry, null);

	    if (retVal == OperationStatus.NOTFOUND) {
		return false;
	    }

	    if (cursor.count() > 0) {
		float currentPyramidValue = SortedFloatBinding
			.entryToFloat(keyEntry);
		short max = Short.MIN_VALUE;
		while (retVal == OperationStatus.SUCCESS
			&& currentPyramidValue == ppTreeValue) {

		    TupleInput in = new TupleInput(dataEntry.getData());

		    int i = 0;
		    short t;
		    max = Short.MIN_VALUE;
		    // STATS
		    while (i < tuple.length) {
			t = (short) Math.abs(tuple[i] - in.readShort());
			if (t > max) {
			    max = t;
			    if (t != 0) {
				break; // finish this loop this slice won't be
				// matched
				// after all!
			    }
			}
			i++;
		    }

		    // if max == 0 we can check the candidate

		    if (max == 0) {
			// there is a chance it is a possible match
			int id = in.readInt();
			O toCompare = super.getObject(id);
			if (object.equals(toCompare)) {
			    return true;
			}

		    }
		    // read the next record
		    retVal = cursor.getNext(keyEntry, dataEntry, null);
		    // update the current pyramid value so that we know when
		    // to
		    // stop
		    currentPyramidValue = SortedFloatBinding
			    .entryToFloat(keyEntry);
		}
	    }

	} finally {
	    if (cursor != null) {
		cursor.close();
	    }
	}
	return false;
    }
    
    
    // re-implemented here to improve performance
    // This code has some bugs
    public int insert(O object) throws DatabaseException, OBException,
	    IllegalAccessException, InstantiationException {
	int resId = -1;
	if (isFrozen()) {
	   resId = insertAux(object);
	} else {
	    resId = id.getAndIncrement();
	    insertUnFrozen(object, resId);
	}
	return resId;
    }
    
    private int insertAux(O object)throws DatabaseException, OBException,
    IllegalAccessException, InstantiationException{
		// if the object is not in the database, we can insert it
		
		int resId = -1;
		short[] tuple = new short[pivotsCount];
		// calculate the pivot for the given object
		calculatePivotTuple(object, tuple);
		float[] first = normalizeFirstPass(tuple);

		// now we just have to search this guy
		float ppTreeValue = super.ppvalue(first);

		Cursor cursor = null;
		boolean exists = false;
		try {

		    DatabaseEntry keyEntry = new DatabaseEntry();
		    DatabaseEntry dataEntry = new DatabaseEntry();
		    cursor = cDB.openCursor(null, null);
		    SortedFloatBinding.floatToEntry(ppTreeValue, keyEntry);
		    OperationStatus retVal = cursor.getSearchKeyRange(keyEntry,
			    dataEntry, null);

		    if (retVal == OperationStatus.NOTFOUND) {
			exists = false;
		    }else
		    if (cursor.count() > 0) {
			float currentPyramidValue = SortedFloatBinding
				.entryToFloat(keyEntry);
			short max = Short.MIN_VALUE;
			short realDistance = Short.MIN_VALUE;
			while (retVal == OperationStatus.SUCCESS
				&& currentPyramidValue == ppTreeValue) {

			    TupleInput in = new TupleInput(dataEntry.getData());

			    int i = 0;
			    short t;
			    max = Short.MIN_VALUE;
			    // STATS
			    while (i < tuple.length) {
				t = (short) Math.abs(tuple[i] - in.readShort());
				if (t > max) {
				    max = t;
				    if (t != 0) {
					break; // finish this loop this slice won't be
					// matched
					// after all!
				    }
				}
				i++;
			    }

			    // if max == 0 we can check the candidate

			    if (max == 0) {
				// there is a chance it is a possible match
				int id = in.readInt();
				O toCompare = super.getObject(id);
				if (object.equals(toCompare)) {				    
				    exists  = true;
				    break;
				}

			    }
			    // read the next record
			    retVal = cursor.getNext(keyEntry, dataEntry, null);
			    // update the current pyramid value so that we know when
			    // to
			    // stop
			    currentPyramidValue = SortedFloatBinding
				    .entryToFloat(keyEntry);
			}
			
		    }

		} finally {
		    if (cursor != null) {
			cursor.close();
		    }
		}
		if(exists){
		    resId =  -1;
		}else{
		    // we have to insert the record. 
		    resId = id.getAndIncrement();
		    insertA(object, resId);
		    insertFrozenAuxAux(ppTreeValue, tuple, resId);
		}
		return resId;		
	    
    }
    
    protected  int deleteAux(final O object) throws DatabaseException, OBException,
	IllegalAccessException, InstantiationException {
	int resId = -1;
	short[] tuple = new short[pivotsCount];
	// calculate the pivot for the given object
	calculatePivotTuple(object, tuple);
	float[] first = normalizeFirstPass(tuple);

	// now we just have to search this guy
	float ppTreeValue = super.ppvalue(first);

	Cursor cursor = null;
	boolean exists = false;
	try {

	    DatabaseEntry keyEntry = new DatabaseEntry();
	    DatabaseEntry dataEntry = new DatabaseEntry();
	    cursor = cDB.openCursor(null, null);
	    SortedFloatBinding.floatToEntry(ppTreeValue, keyEntry);
	    OperationStatus retVal = cursor.getSearchKeyRange(keyEntry,
		    dataEntry, null);

	    if (retVal == OperationStatus.NOTFOUND) {
		exists = false;
	    }else
	    if (cursor.count() > 0) {
		float currentPyramidValue = SortedFloatBinding
			.entryToFloat(keyEntry);
		short max = Short.MIN_VALUE;
		while (retVal == OperationStatus.SUCCESS
			&& currentPyramidValue == ppTreeValue) {

		    TupleInput in = new TupleInput(dataEntry.getData());

		    int i = 0;
		    short t;
		    max = Short.MIN_VALUE;
		    // STATS
		    while (i < tuple.length) {
			t = (short) Math.abs(tuple[i] - in.readShort());
			if (t > max) {
			    max = t;
			    if (t != 0) {
				break; // finish this loop this slice won't be
				// matched
				// after all!
			    }
			}
			i++;
		    }

		    // if max == 0 we can check the candidate

		    if (max == 0) {
			// there is a chance it is a possible match
			int id = in.readInt();
			O toCompare = super.getObject(id);
			if (object.equals(toCompare)) {
			    resId = id;
			    exists  = true;
			    retVal = cursor.delete();
			    if(retVal != OperationStatus.SUCCESS){
				throw new DatabaseException();
			    }
			    break;
			}

		    }
		    // read the next record
		    retVal = cursor.getNext(keyEntry, dataEntry, null);
		    // update the current pyramid value so that we know when
		    // to
		    // stop
		    currentPyramidValue = SortedFloatBinding
			    .entryToFloat(keyEntry);
		}
		
	    }

	} finally {
	    if (cursor != null) {
		cursor.close();
	    }
	}
	return resId;
    }

}
