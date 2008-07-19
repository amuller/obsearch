package net.obsearch.index.pyramid.imp;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Iterator;

import net.obsearch.Index;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.pyramid.AbstractExtendedPyramidIndex;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.bdb.AbstractBDBOBStore;
import net.obsearch.utils.bytes.ByteBufferFactoryConversion;

import net.obsearch.index.IndexShort;
import net.obsearch.ob.OBShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;
import net.obsearch.storage.OBStoreDouble;
import net.obsearch.storage.TupleDouble;
import net.obsearch.storage.TupleLong;


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
     *                Number of pivots to be used.
     * @param pivotSelector
     *                The pivot selector that will be used by this index.
     * @param type
     *                The class of the object O that will be used.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public ExtendedPyramidIndexShort(Class < O > type,
            IncrementalPivotSelector < O > pivotSelector, int pivotCount,
            short minValue, short maxValue) throws OBStorageException,
            OBException, IOException {

        super(type, pivotSelector, pivotCount);
        this.minInput = minValue;
        this.maxInput = maxValue;
        assert minInput < maxInput;
    }

    @Override
    protected double[] extractTuple(ByteBuffer in) throws OutOfRangeException {
        int i = 0;
        double[] res = new double[getPivotCount()];
        while (i < getPivotCount()) {
            res[i] = normalize(in.getShort());
            i++;
        }
        return res;
    }

    public boolean intersects(final O object, final short r, final int box)
            throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        double[][] q = new double[getPivotCount()][2]; // rectangular query
        generateRectangle(t, r, q);
        double[] lowHighResult = new double[2];
        double[] minArray = generateMinArray(q);
        return intersect(q, box, minArray, lowHighResult);
    }

    public Iterator < Long > intersectingBoxes(final O object, final short r)
            throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        throw new UnsupportedOperationException();
    }

    public void searchOB(final O object, final short r,
            final OBPriorityQueueShort < O > result) throws NotFrozenException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        // check if we are frozen
        assertFrozen();

        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        // object

        int pyramidCount = 2 * getPivotCount();
        double[][] qorig = new double[getPivotCount()][2]; // rectangular query
        double[][] q = new double[getPivotCount()][2]; // rectangular query
        short myr = r;
        generateRectangle(t, myr, qorig);
        int i = 0;
        double[] lowHighResult = new double[2];
        double[] minArray = generateMinArray(qorig);
        while (i < pyramidCount) {
            copyQuery(qorig, q);

            if (intersect(q, i, minArray, lowHighResult)) {
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
            final OBPriorityQueueShort < O > result, final long[] boxes)
            throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        // check if we are frozen
        assertFrozen();
        BitSet boxesBit = new BitSet();
        int i = 0;
        while (i < boxes.length) {
            boxesBit.set((int) boxes[i]);
            i++;
        }
        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        // object

        int pyramidCount = 2 * getPivotCount();
        double[][] qorig = new double[getPivotCount()][2]; // rectangular query
        double[][] q = new double[getPivotCount()][2]; // rectangular query
        short myr = r;
        generateRectangle(t, myr, qorig);
        i = 0;
        double[] lowHighResult = new double[2];
        // TODO: select the pyramids randomly just like quicksort
        double[] minArray = generateMinArray(qorig);
        while (i < pyramidCount) {
            copyQuery(qorig, q);
            if (intersect(q, i, minArray, lowHighResult) && boxesBit.get(i)) {
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
            final short r, final double hlow, final double hhigh,
            final OBPriorityQueueShort < O > result)
            throws IllegalAccessException, InstantiationException,
            IllegalIdException, OBException {

        CloseIterator < TupleDouble > it = C.processRange(hlow, hhigh);
        try{
        short max = Short.MIN_VALUE;
        short realDistance = Short.MIN_VALUE;
        while (it.hasNext()) {
            TupleDouble tup = it.next();
            ByteBuffer in = tup.getValue();

            int i = 0;
            short t;
            max = Short.MIN_VALUE;
            while (i < tuple.length) {
                t = (short) Math.abs(tuple[i] - in.getShort());
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
                long id = in.getLong();
                O toCompare = getObject(id);
                realDistance = object.distance(toCompare);
                if (realDistance <= r) {
                    result.add(id, toCompare, realDistance);
                }
            }
        }
        }finally{
            it.closeCursor();
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
            final double[][] q) throws OutOfRangeException {
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
     * Transforms the given tuple into an extended pyramid technique normalized
     * value that considers the "center" of the dimension.
     * @param tuple
     *                The original tuple in the default dimension
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @return resulting normalized tuple
     */
    protected final double[] extendedPyramidTransform(final short[] tuple)
            throws OutOfRangeException {
        int i = 0;
        double[] result = new double[tuple.length];
        assert tuple.length == result.length;
        while (i < tuple.length) {
            double norm = normalize(tuple[i]);
            double norm2 = extendedPyramidNormalization(norm, i);
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
    protected double normalize(final short x) throws OutOfRangeException {
        if (x < minInput || x > maxInput) {
            throw new OutOfRangeException(minInput + "", maxInput + "", "" + x);
        }
        return ((double) (x - minInput)) / ((double) (maxInput - minInput));
    }

    

    /*
     * (non-Javadoc)
     * @see net.obsearch.index.AbstractOBIndex#insertAux(long,
     *      net.obsearch.result.OB)
     */
    @Override
    protected net.obsearch.OperationStatus insertAux(long id, O object)
            throws OBStorageException, OBException, IllegalAccessException,
            InstantiationException {
        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t); // calculate the tuple for the new //

        double[] et = extendedPyramidTransform(t);
        double pyramidValue = pyramidValue(et);
        ByteBuffer out = ByteBufferFactoryConversion.createByteBuffer(0, super
                .getPivotCount(), 0, 1, 0, 0);
        // write the tuple
        for (short d : t) {
            out.putShort(d);
        }
        // write the object's id
        out.putLong(id);
        C.put(pyramidValue, out);
        OperationStatus result = new OperationStatus();
        result.setStatus(Status.OK);
        result.setId(id);
        return result;
    }

    public long getBox(final O object) throws OBException {
        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t); // calculate the tuple for the new //
        double[] et = extendedPyramidTransform(t);
        return super.pyramidOfPoint(et);
    }

    public OperationStatus exists(O object) throws OBException,
            IllegalAccessException, InstantiationException {
        OperationStatus res = new OperationStatus(Status.NOT_EXISTS);
        OBPriorityQueueShort < O > result = new OBPriorityQueueShort < O >(
                (byte) 1);
        searchOB(object, (short) 1, result);
        
        if (result.getSize() == 1 ){
            OBResultShort < O > r = result.iterator().next();
                        if( r.getObject().equals(object)) {
                            res.setStatus(Status.EXISTS);
                            res.setId(r.getId());
                        }
        }
        return res;
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
        assert tuple.length == getPivotCount();
        int i = 0;
        while (i < tuple.length) {
            tuple[i] = obj.distance(pivots[i]);
            i++;
        }
    }

    @Override
    protected OperationStatus deleteAux(final O object) throws OBException,
            IllegalAccessException, InstantiationException {
        long resId = -1;
        OperationStatus res = new OperationStatus(Status.NOT_EXISTS);
        short[] tuple = new short[getPivotCount()];
        // calculate the pivot for the given object
        calculatePivotTuple(object, tuple);
        double[] et = extendedPyramidTransform(tuple);
        double pyramidValue = pyramidValue(et);

        CloseIterator < TupleDouble > it = C
                .processRange(pyramidValue, pyramidValue);
        try{
        short max = Short.MIN_VALUE;
        while (it.hasNext()) {
            TupleDouble tup = it.next();
            ByteBuffer in = tup.getValue();

            int i = 0;
            short t;
            max = Short.MIN_VALUE;
            // STATS
            while (i < tuple.length) {
                t = (short) Math.abs(tuple[i] - in.getShort());
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

            if (max == 0) {
                // there is a chance it is a possible match
                long id = in.getLong();
                O toCompare = super.getObject(id);
                if (object.equals(toCompare)) {
                    resId = id;
                    res = new OperationStatus(Status.OK);
                    res.setId(resId);
                    it.remove();
                    break;
                }

            }

        }
        }catch(Exception e){
            throw new OBException(e);
        }
        finally{
            it.closeCursor();
        }
        return res;
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.index.pivot.AbstractPivotOBIndex#objectToProjectionBytes(net.obsearch.result.OB)
     */
    @Override
    protected ByteBuffer objectToProjectionBytes(O object) throws OBException {
        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t); // calculate the pivot for the given
        ByteBuffer out = ByteBufferFactoryConversion.createByteBuffer(0, super
                .getPivotCount(), 0, 0, 0, 0);
        for (short d : t) {
            out.putShort(d);
        }
        return out;
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.index.IndexShort#searchOB(net.obsearch.result.ob.OBShort,
     *      short, net.obsearch.result.result.OBPriorityQueueShort, int[])
     */
    @Override
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result,
            int[] boxes) throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        // TODO Auto-generated method stub

    }

    public double distance(O a, O b) throws OBException {
        short result = ((OBShort) a).distance((OBShort) b);
        return normalize(result);
    }

}
