package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.pptree.SpaceTreeLeaf;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
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
 * PPTreeShort Implementation of a P+Tree that stores OB objects whose distance
 * functions generate shorts. We take the burden of maintaining one class per
 * datatype for efficiency reasons.
 * The spore file name is: PPTreeShort
 * @author Arnoldo Jose Muller Molina
 * @param <O>
 *            The type of object to be stored in the Index.
 * @since 0.7
 */

public class PPTreeShort < O extends OBShort >
        extends AbstractPPTree < O > implements IndexShort < O > {

    /**
     * Minimum value to be returned by the distance function.
     */
    private short minInput;

    /**
     * Maximum value to be returned by the distance function.
     */
    private short maxInput;

    /**
     * Optimization value for minInput and maxInput.
     */
    private float opt;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(PPTreeShort.class);

    /**
     * Used to cache results. Currently disabled.
     */
    protected transient HashMap < O, OBQueryShort < O >> resultCache;

    /**
     * Size of the cache of {@link #resultCache}.
     */
    protected  static final int resultCacheSize = 3000;

    /**
     * Cache used for getting objects from B. Only used before freezing
     */
    private transient OBCache < float[] > bCache;
    
    
 
 
    /**
     * Constructor.
     * @param databaseDirectory
     *            Directory were the index will be stored
     * @param pivots
     *            Numbe rof pivots to be used.
     * @param od
     *            Partitions for the space tree (please check the P+tree paper)
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws IOException
     *             If the databaseDirectory directory does not exist.
     */
    public PPTreeShort(File databaseDirectory, short pivots, byte od)
            throws DatabaseException, IOException {
        this(databaseDirectory, pivots, od, Short.MIN_VALUE, Short.MAX_VALUE);
    }

    /**
     * Creates a new PPTreeShort. Ranges accepted by this index will be defined
     * by the user. We recommend the use of this constructor. We believe it will
     * give better resolution to the float transformation. The values returned
     * by the distance function must be within [minInput, maxInput]. These two
     * values can be over estimated but not under estimated.
     * @param databaseDirectory
     *            Directory were the index will be stored
     * @param pivots
     *            Numbe rof pivots to be used.
     * @param minInput
     *            Minimum value to be returned by the distance function
     * @param maxInput
     *            Maximum value to be returned by the distance function
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws IOException
     *             If the databaseDirectory directory does not exist.
     */
    public PPTreeShort(File databaseDirectory, short pivots, byte od,
            short minInput, short maxInput) throws DatabaseException,
            IOException {
        super(databaseDirectory, pivots, od);
        assert minInput < maxInput;
        this.minInput = minInput;
        this.maxInput = maxInput;
        // this optimization reduces the computations required
        // for the first level normalization.
	this.opt = 1 / ((float) (maxInput - minInput));
        resultCache = new HashMap < O, OBQueryShort < O >>(3000);
        bCache = new OBCache < float[] >(this.databaseSize());
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
        return "PPTreeShort";
    }

    /**
     * Number of bytes that it takes to encode a short.
     * @return Number of bytes that it takes to encode a short.
     */
    protected int distanceValueSizeInBytes() {
        return Short.SIZE / 8;
    }

    /**
     * Method that takes the values already calculated in B and puts them into C
     * This is to save some time when rebuilding the index
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
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
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                assert i == IntegerBinding.entryToInt(foundKey);
                // i contains the actual id of the tuple
                TupleInput in = new TupleInput(foundData.getData());
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
        assert aDB.count() == bDB.count() && cDB.count() == bDB.count(): "Count c: " + cDB.count()  + " count b: " + bDB.count();

    }

    /**
     * Generates a query rectangle based on the given range and the given tuple.
     * It normalizes the query first level only
     * @param t
     *            the tuple to be processed
     * @param r
     *            the range
     * @param q
     *            resulting rectangle query
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     */

    protected final void generateRectangleFirstPass(short[] t, short r,
            float[][] q) throws OutOfRangeException {
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

        // calculate the vector for the object
        short[] t = new short[pivotsCount];
        calculatePivotTuple(object, t);
        // calculate the rectangle
        float[][] qrect = new float[pivotsCount][2];
        generateRectangleFirstPass(t, r, qrect);
        float [] center = normalizeFirstPass(t);
        // obtain the hypercubes that have to be matched
        List < SpaceTreeLeaf > hyperRectangles = new LinkedList < SpaceTreeLeaf >();
        spaceTree.searchRange(qrect, center, hyperRectangles);
        int[] result = new int[hyperRectangles.size()];
        Iterator < SpaceTreeLeaf > it = hyperRectangles.iterator();
        int i = 0;
        while (it.hasNext()) {
            SpaceTreeLeaf leaf = it.next();
            result[i] = leaf.getSNo();
            i++;
        }
        return result;
    }
    
    /**
     * All the statistics values are kept here so that we will erase them at
     * some point in the future. This is ugly but it is the only way that we will
     * remove all the code related to stats to squeeze the last ounce of performance
     * out of OBSearch :)
     */
    public long initialHyperRectangleTotal = 0;
    public long queryCount = 0;    
    public long finalHyperRectangleTotal = 0;
    public long finalPyramidTotal = 0;
    public long smapRecordsCompared = 0;
    public long distanceComputations = 0;

    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        short[] t = new short[pivotsCount];
        // calculate the pivot for the given object
        calculatePivotTuple(object, t);
        float[][] qrect = new float[pivotsCount][2]; // rectangular query
        generateRectangleFirstPass(t, r, qrect);
        List < SpaceTreeLeaf > hyperRectangles = new LinkedList < SpaceTreeLeaf >();
        // obtain the hypercubes that have to be matched
        float [] center = normalizeFirstPass(t);
        spaceTree.searchRange(qrect, center, hyperRectangles);        
        searchOBAux(object, r, result, qrect, t, hyperRectangles);
        
        // stats
        queryCount++;
        initialHyperRectangleTotal += hyperRectangles.size();
    }
    
    
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result,
            int[] boxes) throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        short[] t = new short[pivotsCount];
        // calculate the pivot for the given object
        calculatePivotTuple(object, t);
        float[][] qrect = new float[pivotsCount][2]; // rectangular query
        generateRectangleFirstPass(t, r, qrect);
        List < SpaceTreeLeaf > hyperRectangles = new LinkedList < SpaceTreeLeaf >();
        int i = 0;
        int max = boxes.length;
        while (i < max) {
            hyperRectangles.add(super.spaceTreeLeaves[boxes[i]]);
            i++;
        }
        searchOBAux(object, r, result, qrect, t, hyperRectangles);
    }

    /**
     * Helper function to search for objects.
     * @param object
     *            The object that has to be searched
     * @param r
     *            The range to be used
     * @param result
     *            A priority queue that will hold the result
     * @param qrect
     *            Query rectangle
     * @param t
     *            Tuple in raw form (short)
     * @param hyperRectangles
     *            The space tree leaves that will be searched.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws NotFrozenException
     *             if the index has not been frozen.
     * @throws IllegalIdException
     *             This exception is left as a Debug flag. If you receive this
     *             exception please report the problem to:
     *             http://code.google.com/p/obsearch/issues/list
     */
    public void searchOBAux(O object, short r,
            OBPriorityQueueShort < O > result, float[][] qrect, short[] t,
            List < SpaceTreeLeaf > hyperRectangles) throws NotFrozenException,
            DatabaseException, InstantiationException, IllegalIdException,
            IllegalAccessException, OutOfRangeException, OBException {
        // check if we are frozen
        assertFrozen();

        // check if the result has been processed
        /*
         * OBQueryShort cachedResult = this.resultCache.get(object);
         * if(cachedResult != null && cachedResult.getDistance() == r){ Iterator<OBResultShort<O>>
         * it =cachedResult.getResult().iterator(); while(it.hasNext()){
         * OBResultShort<O> element = it.next(); result.add(element.getId(),
         * element.getObject(), element.getDistance()); } return; }
         */
        int pyramidCount = 2 * pivotsCount;

        short myr = r;

        float[] lowHighResult = new float[2];

        Iterator < SpaceTreeLeaf > it = hyperRectangles.iterator();
        // this will hold the rectangle for the current hyperrectangle
        float[][] qw = new float[pivotsCount][2];
        float[][] q = new float[pivotsCount][2];
        while (it.hasNext()) {
            SpaceTreeLeaf space = it.next();
            if (!space.intersects(qrect)) {
                continue;
            }
            // stats
            finalHyperRectangleTotal++;
            
            // for each space there are 2d pyramids that have to be browsed
            int i = 0;
            // update the current rectangle, we also have to center it
            space.generateRectangle(qrect, qw);
            centerQuery(qw); // center the rectangle

            while (i < pyramidCount) {
                // intersect destroys q, so we have to copy it
                copyQuery(qw, q);
                if (intersect(q, i, lowHighResult)) {
                    this.finalPyramidTotal++;
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
     * positives. This technique is called SMAP. Calculates the real distance
     * and updates the result priority queue It is left public so that junit can
     * perform validations on it Performance-wise this is one of the most
     * important methods
     * @param object
     *            object to search
     * @param tuple
     *            tuple of the object
     * @param r
     *            range
     * @param hlow
     *            lowest pyramid value
     * @param hhigh
     *            highest pyramid value
     * @param result
     *            result of the search operation
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws IllegalIdException
     *             This exception is left as a Debug flag. If you receive this
     *             exception please report the problem to:
     *             http://code.google.com/p/obsearch/issues/list
     */
    public void searchBTreeAndUpdate(O object, short[] tuple, short r,
            float hlow, float hhigh, OBPriorityQueueShort < O > result)
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
                
                int i = 0;
                short t;
                MyTupleInput in = new MyTupleInput();
                while (retVal == OperationStatus.SUCCESS
                        && currentPyramidValue <= hhigh) {

                    in.setBuffer(dataEntry.getData());
                    
                    //this.smapRecordsCompared++;
                    
                    i = 0;
                    
                    max = Short.MIN_VALUE;
                    // STATS
                    while (i < pivotsCount) {
                        t = (short) Math.abs(tuple[i] - in.readShortFast());
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
                        this.distanceComputations++;
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
     * ((double)this.accumExecutionTimeD / (double)this.totalExecutedTimesD); }
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
     * @param t
     *            tuple
     * @param id
     *            internal id
     * @return 1 if everything was sucessful
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     */
    protected byte insertFrozenAux(short[] t, int id)
            throws OutOfRangeException, DatabaseException {
        float[] first = normalizeFirstPass(t);
        float ppTreeValue = super.ppvalue(first);
        return insertFrozenAuxAux(ppTreeValue, t, id);
    }

    /**
     * Inserts the given tuple and id into C with the given ppTreeValue.
     * @param t
     *            tuple
     * @param ppTreeValue
     *            P+Tree value for the tuple
     * @param id
     *            internal id
     * @return 1 if everything was sucessful
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     */
    protected byte insertFrozenAuxAux(float ppTreeValue, short[] t, int id)
            throws DatabaseException {
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

        if (cDB.put(null, keyEntry, dataEntry) != OperationStatus.SUCCESS) {
            throw new DatabaseException();
        }
        return 1;
    }

    /**
     * Normalizes the given t. The idea is to convert each of the values in t in
     * the range [0,1].
     * @param t
     * @return A float array of values in the range[0,1]
     * @throws OutOfRangeException
     *             If any of the values in t goes beyond the ranges defined at
     *             construction time.
     */
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
     * Read the given tuple from B database and load it into the given tuple in
     * a normalized form.
     * @param id
     *            local id of the tuple we want
     * @param tuple
     *            The tuple is loaded and stored here.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     */
    @Override
    protected final float[] readFromB(int id)
            throws DatabaseException, OutOfRangeException {
        Cursor cursor = null;
        // check if the tuple is in cache
        float[] temp = this.bCache.get(id);
        if (temp != null) {
            int i = 0;
            
            return temp;
        }
        float[] tempTuple = new float[pivotsCount];
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
                throw new OutOfRangeException();
            }

            assert cursor.count() == 1;

            TupleInput in = new TupleInput(dataEntry.getData());
            int i = 0;
            
            while (i < pivotsCount) {
                tempTuple[i] = normalizeFirstPassAux(in.readShort());
                i++;
            }
            bCache.put(id, tempTuple);
        } finally {
            cursor.close();
        }
        return tempTuple;
    }

    /**
     * Normalize the given value This is a first pass normalization, any value
     * to [0,1].
     * @param x
     *            value to be normalized
     * @return the normalized value
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     */
    protected float normalizeFirstPassAux(short x) throws OutOfRangeException {
        if (x < minInput || x > maxInput) {
            throw new OutOfRangeException(minInput + "", maxInput + "", "" + x);
        }
        return ((float) (x - minInput)) * opt;
    }

    @Override
    protected Index returnSelf() {
        return this;
    }

    /**
     * Calculates the tuple vector for the given object.
     * @param obj
     *            object to be processed
     * @param tuple
     *            The resulting tuple will be stored here
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
        resultCache = new HashMap < O, OBQueryShort < O >>(resultCacheSize);
        return this;
    }

    public Result exists(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        Result res = new Result(Result.Status.NOT_EXISTS);
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
                return res;
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
                            res = new Result(Result.Status.EXISTS);
                            res.setId(id);
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
        return res;
    }

    // re-implemented here to improve performance
    @Override
    public Result insert(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        if (isFrozen()) {
            return insertAux(object);
        } else {
            // do the default method if we are in frozen mode.
            return super.insert(object);
        }
    }

    /**
     * Auxiliary insert operation.
     * @param object
     *            object to insert.
     * @return {@link org.ajmm.obsearch.Result#OK} and the deleted object's id
     *         if the object was found and successfully deleted.
     *         {@link org.ajmm.obsearch.Result#NOT_EXISTS} if the object is not
     *         in the database.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    private Result insertAux(final O object) throws DatabaseException,
            OBException, IllegalAccessException, InstantiationException {
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
            } else if (cursor.count() > 0) {
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
                            resId = id;
                            exists = true;
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
        Result res;
        if (exists) {
            res = new Result(Result.Status.EXISTS);
           
        } else {
            // we have to insert the record.
            res = new Result(Result.Status.OK);
            resId = id.getAndIncrement();
            insertA(object, resId);
            insertFrozenAuxAux(ppTreeValue, tuple, resId);
        }
        res.setId(resId);
        return res;

    }

    /**
     * Auxiliary delete operation.
     * @param object
     *            object to delete.
     * @return -1 if the object was not found, otherwise the object's id
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    @Override
    protected final Result deleteAux(final O object) throws DatabaseException,
            OBException, IllegalAccessException, InstantiationException {
        int resId = -1;
        Result res = new Result(Result.Status.NOT_EXISTS);
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
            } else if (cursor.count() > 0) {
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
                            exists = true;
                            retVal = cursor.delete();
                            res = new Result(Result.Status.OK);
                            res.setId(resId);
                            if (retVal != OperationStatus.SUCCESS) {
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
        return res;
    }

    public  float distance(O a, O b) throws OBException{
        short result = ((OBShort)a).distance((OBShort)b);
        return normalizeFirstPassAux(result);
    }
    
    
}
