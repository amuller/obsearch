package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.SortedFloatBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
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
 * ExtendedPyramidIndexShort is an index that uses the pyramid technique. The
 * distance function used must return short values. The spore file name is:
 * ExtendedPyramidTechniqueShort
 * @param <O>
 *                The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public class ExtendedPyramidIndexShort < O extends OBShort >
        extends AbstractExtendedPyramidIndex < O > implements IndexShort < O > {

    /**
     * Minimum value to be returned by the distance function.
     */
    private short minInput;

    /**
     * Maximum value to be returned by the distance function.
     */
    private short maxInput;

    /**
     * Creates a new ExtendedPyramidIndexShort. Ranges accepted by this pyramid
     * will be between 0 and Short.MAX_VALUE
     * @param databaseDirectory
     *                Directory were the index will be stored
     * @param pivots
     *                Numbe rof pivots to be used.
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public ExtendedPyramidIndexShort(final File databaseDirectory,
            final short pivots) throws DatabaseException, IOException {

        this(databaseDirectory, pivots, (short) 0, Short.MAX_VALUE);
    }

    /**
     * Creates a new ExtendedPyramidIndexShort. Ranges accepted by this index
     * will be defined by the user. We recommend the use of this constructor. We
     * believe it will give better resolution to the float transformation. The
     * values returned by the distance function must be within [minInput,
     * maxInput]. These two values can be over estimated but not under
     * estimated.
     * @param databaseDirectory
     *                Directory were the index will be stored
     * @param pivots
     *                Numbe rof pivots to be used.
     * @param minInput
     *                Minimum value to be returned by the distance function
     * @param maxInput
     *                Maximum value to be returned by the distance function
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public ExtendedPyramidIndexShort(final File databaseDirectory,
            final short pivots, final short minInput, final short maxInput)
            throws DatabaseException, IOException {
        super(databaseDirectory, pivots);
        this.minInput = minInput;
        this.maxInput = maxInput;
        assert minInput < maxInput;
    }

    @Override
    public String getSerializedName() {
        return "ExtendedPyramidTechniqueShort";
    }

    @Override
    protected float[] extractTuple(final TupleInput in)
            throws OutOfRangeException {
        int i = 0;
        float[] res = new float[pivotsCount];
        while (i < pivotsCount) {
            res[i] = normalize(in.readShort());
            i++;
        }
        return res;
    }

    public boolean intersects(final O object, final short r, final int box)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        short[] t = new short[pivotsCount];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        float[][] q = new float[pivotsCount][2]; // rectangular query
        generateRectangle(t, r, q);
        float[] lowHighResult = new float[2];
        return intersect(q, box, lowHighResult);
    }

    public int[] intersectingBoxes(final O object, final short r)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        BitSet result = new BitSet(super.totalBoxes());
        short[] t = new short[pivotsCount];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        int pyramidCount = 2 * pivotsCount;
        float[][] qorig = new float[pivotsCount][2]; // rectangular query
        float[][] q = new float[pivotsCount][2]; // rectangular query
        short myr = r;
        float[] lowHighResult = new float[2];
        generateRectangle(t, myr, qorig);
        int i = 0;
        while (i < pyramidCount) {
            copyQuery(qorig, q);
            if (intersect(q, i, lowHighResult)) {
                result.set(i);
            }
            i++;
        }
        int[] res = new int[result.cardinality()];
        i = 0;
        int index = 0;
        while (i < res.length) {
            index = result.nextSetBit(index);
            res[i] = index;
            index++;
            i++;
        }
        return res;
    }

    public void searchOB(final O object, final short r,
            final OBPriorityQueueShort < O > result) throws NotFrozenException,
            DatabaseException, InstantiationException, IllegalIdException,
            IllegalAccessException, OutOfRangeException, OBException {
        // check if we are frozen
        assertFrozen();

        short[] t = new short[pivotsCount];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        // object

        int pyramidCount = 2 * pivotsCount;
        float[][] qorig = new float[pivotsCount][2]; // rectangular query
        float[][] q = new float[pivotsCount][2]; // rectangular query
        short myr = r;
        generateRectangle(t, myr, qorig);
        int i = 0;
        float[] lowHighResult = new float[2];
        while (i < pyramidCount) {
            copyQuery(qorig, q);
            if (intersect(q, i, lowHighResult)) {
                searchBTreeAndUpdate(object, t, myr, i + lowHighResult[HLOW], i
                        + lowHighResult[HHIGH], result);
                short nr = result.updateRange(myr);
                if (nr < myr) {
                    myr = nr;
                    // regenerate the query with a smaller range
                    generateRectangle(t, myr, qorig);
                }
            }
            i++;
        }
    }

    public void searchOB(final O object, final short r,
            final OBPriorityQueueShort < O > result, final int[] boxes)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        // check if we are frozen
        assertFrozen();
        BitSet boxesBit = new BitSet();
        int i = 0;
        while (i < boxes.length) {
            boxesBit.set(boxes[i]);
            i++;
        }
        short[] t = new short[pivotsCount];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        // object

        int pyramidCount = 2 * pivotsCount;
        float[][] qorig = new float[pivotsCount][2]; // rectangular query
        float[][] q = new float[pivotsCount][2]; // rectangular query
        short myr = r;
        generateRectangle(t, myr, qorig);
        i = 0;
        float[] lowHighResult = new float[2];
        // TODO: select the pyramids randomly just like quicksort
        while (i < pyramidCount) {
            copyQuery(qorig, q);
            if (intersect(q, i, lowHighResult) && boxesBit.get(i)) {
                searchBTreeAndUpdate(object, t, myr, i + lowHighResult[HLOW], i
                        + lowHighResult[HHIGH], result);
                short nr = result.updateRange(myr);
                if (nr < myr) {
                    myr = nr;
                    // regenerate the query with a smaller range
                    generateRectangle(t, myr, qorig);
                }
            }
            i++;
        }
    }

    /**
     * This method reads from the B-tree appies l-infinite to discard false
     * positives. This technique is called SMAP. Calculates the real distance
     * and updates the result priority queue It is left public so that junit can
     * perform validations on it Performance-wise this is one of the most
     * important methods
     * @param object
     *                object to search
     * @param tuple
     *                tuple of the object
     * @param r
     *                range
     * @param hlow
     *                lowest pyramid value
     * @param hhigh
     *                highest pyramid value
     * @param result
     *                result of the search operation
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws IllegalIdException
     *                 This exception is left as a Debug flag. If you receive
     *                 this exception please report the problem to:
     *                 http://code.google.com/p/obsearch/issues/list
     */
    private void searchBTreeAndUpdate(final O object, final short[] tuple,
            final short r, final float hlow, final float hhigh,
            final OBPriorityQueueShort < O > result) throws DatabaseException,
            IllegalAccessException, InstantiationException, IllegalIdException,
            OBException {

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
                    // update the current pyramid value so that we know when to
                    // stop
                    currentPyramidValue = SortedFloatBinding
                            .entryToFloat(keyEntry);
                }
            }
        } finally {
            cursor.close();
        }
    }

    /**
     * Generates min and max for the given tuple It is actually the query. If
     * you want non-rectangular queries you have to override this method and
     * make sure your modification works well with searchOB
     * @param t
     *                the tuple to be processed
     * @param r
     *                the range
     * @param q
     *                resulting rectangle query
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */

    protected final void generateRectangle(final short[] t, final short r,
            final float[][] q) throws OutOfRangeException {
        // range
        int i = 0;
        while (i < q.length) { //
            q[i][MIN] = extendedPyramidNormalization(normalize((short) Math
                    .max(t[i] - r, minInput)), i);
            q[i][MAX] = extendedPyramidNormalization(normalize((short) Math
                    .min(t[i] + r, maxInput)), i);
            i++;
        }
        centerQuery(q);
    }

    /**
     * Method that takes the values already calculated in B and puts them into C
     * This is to save some time when rebuilding the index.
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
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

    }

    /**
     * Transforms the given tuple into an extended pyramid technique normalized
     * value that considers the "center" of the dimension.
     * @param tuple
     *                The original tuple in the default dimension
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @return resulting normalized tuple
     */
    protected final float[] extendedPyramidTransform(final short[] tuple)
            throws OutOfRangeException {
        int i = 0;
        float[] result = new float[tuple.length];
        assert tuple.length == result.length;
        while (i < tuple.length) {
            float norm = normalize(tuple[i]);
            float norm2 = extendedPyramidNormalization(norm, i);
            result[i] = norm2;
            i++;
        }
        return result;
    }

    /**
     * Normalize the given value.
     * @param x
     *                value to normalize
     * @return the normalized value
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */
    protected float normalize(final short x) throws OutOfRangeException {
        if (x < minInput || x > maxInput) {
            throw new OutOfRangeException(minInput + "", maxInput + "", "" + x);
        }
        return ((float) (x - minInput)) / ((float) (maxInput - minInput));
    }

    @Override
    protected byte insertFrozen(final O object, final int id)
            throws IllegalIdException, OBException, DatabaseException,
            IllegalAccessException, InstantiationException {
        short[] t = new short[pivotsCount];
        calculatePivotTuple(object, t); // calculate the tuple for the new //

        return insertFrozenAux(t, id);
    }

    public int getBox(final O object) throws OBException {
        short[] t = new short[pivotsCount];
        calculatePivotTuple(object, t); // calculate the tuple for the new //
        float[] et = extendedPyramidTransform(t);
        return super.pyramidOfPoint(et);
    }

    public Result exists(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        Result res = new Result(Result.Status.NOT_EXISTS);
        
        short[] tuple = new short[pivotsCount];
        // calculate the pivot for the given object
        calculatePivotTuple(object, tuple);
        
        float[] et = extendedPyramidTransform(tuple);
        float pyramidValue = pyramidValue(et);
        
        Cursor cursor = null;

        try {

            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry dataEntry = new DatabaseEntry();
            cursor = cDB.openCursor(null, null);
            SortedFloatBinding.floatToEntry(pyramidValue, keyEntry);
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
                        && currentPyramidValue == pyramidValue) {

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

    /**
     * Inserts the given tuple and id into C
     * @param t
     *                tuple
     * @param id
     *                internal id
     * @return 1 if everything was sucessful
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     */
    protected byte insertFrozenAux(final short[] t, final int id)
            throws OutOfRangeException, DatabaseException {
        float[] et = extendedPyramidTransform(t);
        float pyramidValue = pyramidValue(et);

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
        SortedFloatBinding.floatToEntry(pyramidValue, keyEntry);
        dataEntry.setData(out.getBufferBytes());

        if (cDB.put(null, keyEntry, dataEntry) != OperationStatus.SUCCESS) {
            throw new DatabaseException();
        }
        return 1;
    }

    @Override
    protected void insertInB(final int id, final O object) throws OBException,
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

    @Override
    protected Index returnSelf() {
        return this;
    }

    /**
     * Calculates the tuple vector for the given object.
     * @param obj
     *                object to be processed
     * @param tuple
     *                The resulting tuple will be stored here
     * @throws OBException
     *                 User generated exception
     */
    protected final void calculatePivotTuple(final O obj, final short[] tuple)
            throws OBException {
        assert tuple.length == pivotsCount;
        int i = 0;
        while (i < tuple.length) {
            tuple[i] = obj.distance(pivots[i]);
            i++;
        }
    }

    /*public Result exists(final O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        Result res = new Result(Result.Status.NOT_EXISTS);
        OBPriorityQueueShort < O > result = new OBPriorityQueueShort < O >(
                (byte) 1);
        searchOB(object, (short) 1, result);
        if (result.getSize() == 1) {
            Iterator < OBResultShort < O >> it = result.iterator();
            assert it.hasNext();
            OBResultShort < O > r = it.next();
            if (object.equals(r.getObject())) {
                res = new Result(Result.Status.EXISTS);
                res.setId(r.getId());
            }
        }
        return res;
    }*/

    @Override
    protected Result deleteAux(final O object) throws DatabaseException,
            OBException, IllegalAccessException, InstantiationException {
        int resId = -1;
        Result res = new Result(Result.Status.NOT_EXISTS);
        short[] tuple = new short[pivotsCount];
        // calculate the pivot for the given object
        calculatePivotTuple(object, tuple);
        float[] et = extendedPyramidTransform(tuple);
        float pyramidValue = pyramidValue(et);

        Cursor cursor = null;
        try {

            CursorConfig config = new CursorConfig();
            config.setReadUncommitted(true);
            DatabaseEntry keyEntry = new DatabaseEntry();
            DatabaseEntry dataEntry = new DatabaseEntry();
            cursor = cDB.openCursor(null, null);
            SortedFloatBinding.floatToEntry(pyramidValue, keyEntry);
            OperationStatus retVal = cursor.getSearchKeyRange(keyEntry,
                    dataEntry, null);

            if (retVal == OperationStatus.NOTFOUND) {
                // nothing to do here
            } else if (cursor.count() > 0) {
                float currentPyramidValue = SortedFloatBinding
                        .entryToFloat(keyEntry);
                short max = Short.MIN_VALUE;
                while (retVal == OperationStatus.SUCCESS
                        && currentPyramidValue == pyramidValue) {

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
                            res = new Result(Result.Status.OK);
                            res.setId(resId);
                            retVal = cursor.delete();
                            // txn.commit();
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

    public float distance(O a, O b) throws OBException {
        short result = ((OBShort) a).distance((OBShort) b);
        return normalize(result);
    }

}
