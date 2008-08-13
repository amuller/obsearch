package net.obsearch.index.pptree.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import net.obsearch.Index;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCache;
import net.obsearch.cache.OBCacheLoaderInt;
import net.obsearch.cache.OBCacheLoaderLong;
import net.obsearch.cache.OBCacheLong;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.filter.Filter;
import net.obsearch.index.pptree.AbstractPPTree;
import net.obsearch.index.pptree.SpaceTreeLeaf;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.storage.CloseIterator;
import net.obsearch.utils.bytes.ByteBufferFactoryConversion;


import net.obsearch.index.IndexShort;

import net.obsearch.ob.OBShort;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultShort;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleDouble;
import org.apache.log4j.Logger;

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
 * datatype for efficiency reasons. The spore file name is: PPTreeShort
 * @author Arnoldo Jose Muller Molina
 * @param <O>
 *                The type of object to be stored in the Index.
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

    
    private OBCacheLong < double[] > bCache;

    /**
     * Constructor.
     * @param databaseDirectory
     *                Directory were the index will be stored
     * @param pivots
     *                Numbe rof pivots to be used.
     * @param od
     *                Partitions for the space tree (please check the P+tree
     *                paper)
     * @param pivotSelector
     *                The pivot selector that will be used by this index.
     * @param type
     *                The class of the object O that will be used.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public PPTreeShort(Class < O > type,
            IncrementalPivotSelector < O > pivotSelector, int pivotCount, int od)
            throws IOException, OBException {
        this(type, pivotSelector, pivotCount, od, Short.MIN_VALUE,
                Short.MAX_VALUE);
    }

    /**
     * Creates a new PPTreeShort. Ranges accepted by this index will be defined
     * by the user. We recommend the use of this constructor. We believe it will
     * give better resolution to the float transformation. The values returned
     * by the distance function must be within [minInput, maxInput]. These two
     * values can be over estimated but not under estimated.
     * @param databaseDirectory
     *                Directory were the index will be stored
     * @param pivots
     *                Numbe rof pivots to be used.
     * @param minInput
     *                Minimum value to be returned by the distance function
     * @param maxInput
     *                Maximum value to be returned by the distance function
     * @param cpus
     *                Number of CPUS to use.
     * @param pivotSelector
     *                The pivot selector that will be used by this index.
     * @param type
     *                The class of the object O that will be used.
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws IOException
     *                 If the databaseDirectory directory does not exist.
     */
    public PPTreeShort(Class < O > type,
            IncrementalPivotSelector < O > pivotSelector, int pivotCount,
            int od, short minInput, short maxInput) throws IOException,
            OBException {
        super(type, pivotSelector, pivotCount, od);
        OBAsserts.chkAssert(minInput < maxInput,
                "minInput must be smaller than maxInput");
        this.minInput = minInput;
        this.maxInput = maxInput;
        // this optimization reduces the computations required
        // for the first level normalization.
        this.opt = 1 / ((float) (maxInput - minInput));
        
    }

    @Override
    protected double[] extractTuple(ByteBuffer in) throws OutOfRangeException {
        int i = 0;
        double[] res = new double[getPivotCount()];
        while (i < getPivotCount()) {
            res[i] = normalizeFirstPassAux(in.getShort());
            i++;
        }
        return res;
    }
    
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

    /**
     * Number of bytes that it takes to encode a short.
     * @return Number of bytes that it takes to encode a short.
     */
    protected int distanceValueSizeInBytes() {
        return Short.SIZE / 8;
    }

    /**
     * Generates a query rectangle based on the given range and the given tuple.
     * It normalizes the query first level only
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

    protected final void generateRectangleFirstPass(short[] t, short r,
            double[][] q) throws OutOfRangeException {
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
            throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        // calculate the vector for the object
        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t);
        // calculate the rectangle
        double[][] qrect = new double[getPivotCount()][2];
        generateRectangleFirstPass(t, r, qrect);

        return super.spaceTreeLeaves[box].intersects(qrect);
    }

    public Iterator < Long > intersectingBoxes(O object, short r)
            throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException {
        throw new UnsupportedOperationException();
    }

    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        OBAsserts.chkPositive(r);
        short[] t = new short[getPivotCount()];
        // calculate the pivot for the given object
        calculatePivotTuple(object, t);
        double[][] qrect = new double[getPivotCount()][2]; // rectangular query
        generateRectangleFirstPass(t, r, qrect);
        List < SpaceTreeLeaf > hyperRectangles = new LinkedList < SpaceTreeLeaf >();
        // obtain the hypercubes that have to be matched
        double[] center = normalizeFirstPass(t);
        spaceTree.searchRange(qrect, center, hyperRectangles);
        searchOBAux(object, r, result, qrect, t, hyperRectangles);

        stats.incQueryCount();
        stats.incBucketsRead(hyperRectangles.size());
    }

    public void searchOB(O object, short r, OBPriorityQueueShort < O > result,
            int[] boxes) throws NotFrozenException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        short[] t = new short[getPivotCount()];
        // calculate the pivot for the given object
        calculatePivotTuple(object, t);
        double[][] qrect = new double[getPivotCount()][2]; // rectangular query
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
     *                The object that has to be searched
     * @param r
     *                The range to be used
     * @param result
     *                A priority queue that will hold the result
     * @param qrect
     *                Query rectangle
     * @param t
     *                Tuple in raw form (short)
     * @param hyperRectangles
     *                The space tree leaves that will be searched.
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
     * @throws NotFrozenException
     *                 if the index has not been frozen.
     * @throws IllegalIdException
     *                 This exception is left as a Debug flag. If you receive
     *                 this exception please report the problem to:
     *                 http://code.google.com/p/obsearch/issues/list
     */
    public void searchOBAux(O object, short r,
            OBPriorityQueueShort < O > result, double[][] qrect, short[] t,
            List < SpaceTreeLeaf > hyperRectangles) throws NotFrozenException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
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
        int pyramidCount = 2 * getPivotCount();

        short myr = r;

        double[] lowHighResult = new double[2];

        Iterator < SpaceTreeLeaf > it = hyperRectangles.iterator();
        // this will hold the rectangle for the current hyperrectangle
        double[][] qw = new double[getPivotCount()][2];
        double[][] q = new double[getPivotCount()][2];
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
            double[] minArray = generateMinArray(qw);
            while (i < pyramidCount) {
                // intersect destroys q, so we have to copy it
                copyQuery(qw, q);
                if (intersect(q, i, minArray, lowHighResult)) {
                    int ri = (space.getSNo() * 2 * getPivotCount()) + i; // real
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
        try {
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
        } finally {
            it.closeCursor();
        }

    }

    protected net.obsearch.OperationStatus insertAux(long id, O object)
            throws OBStorageException, OBException, IllegalAccessException,
            InstantiationException {
        short[] t = new short[getPivotCount()];
        calculatePivotTuple(object, t); // calculate the tuple for the new //
        double[] first = normalizeFirstPass(t);
        double ppTreeValue = super.ppvalue(first);
        ByteBuffer out = ByteBufferFactoryConversion.createByteBuffer(0, super
                .getPivotCount(), 0, 1, 0, 0);
        // write the tuple
        for (short d : t) {
            out.putShort(d);
        }
        // write the object's id
        out.putLong(id);
        C.put(ppTreeValue, out);
        OperationStatus result = new OperationStatus();
        result.setStatus(Status.OK);
        result.setId(id);
        return result;
    }

    /**
     * Normalizes the given t. The idea is to convert each of the values in t in
     * the range [0,1].
     * @param t
     * @return A double array of values in the range[0,1]
     * @throws OutOfRangeException
     *                 If any of the values in t goes beyond the ranges defined
     *                 at construction time.
     */
    protected double[] normalizeFirstPass(short[] t) throws OutOfRangeException {
        assert t.length == getPivotCount();
        double[] res = new double[getPivotCount()];

        int i = 0;
        while (i < t.length) {
            res[i] = normalizeFirstPassAux(t[i]);
            i++;
        }
        return res;
    }
    
    public void init(OBStoreFactory fact) throws OBStorageException,
    OBException, NotFrozenException, IllegalAccessException,
    InstantiationException, OBException {
        super.init(fact);
        bCache = new OBCacheLong < double[] >(new BLoader());
    }

    private class BLoader implements OBCacheLoaderLong < double[] > {

        public long getDBSize() throws OBStorageException {
            return B.size();
        }

        public double[] loadObject(long id) throws OutOfRangeException,
                OBException, InstantiationException, IllegalAccessException {
            double[] tempTuple = new double[getPivotCount()];
            ByteBuffer in = B.getValue(id);
            int i = 0;
            while (i < getPivotCount()) {
                tempTuple[i] = normalizeFirstPassAux(in.getShort());
                i++;
            }
            return tempTuple;
        }

    }

    /**
     * Read the given tuple from B database and load it into the given tuple in
     * a normalized form.
     * @param id
     *                local id of the tuple we want
     * @param tuple
     *                The tuple is loaded and stored here.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */
    @Override
    protected final double[] readFromB(long id) throws OutOfRangeException,
            OBException {
        double[] res;
        try {
            res = this.bCache.get(id);
        } catch (Exception e) {
            throw new OBException(e);
        }
        return res;
    }

    /**
     * Normalize the given value This is a first pass normalization, any value
     * to [0,1].
     * @param x
     *                value to be normalized
     * @return the normalized value
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */
    protected double normalizeFirstPassAux(short x) throws OutOfRangeException {
        if (x < minInput || x > maxInput) {
            throw new OutOfRangeException(minInput + "", maxInput + "", "" + x);
        }
        return ((double) (x - minInput)) * opt;
    }

    /**
     * Calculates the tuple vector for the given object.
     * @param obj
     *                object to be processed
     * @param tuple
     *                The resulting tuple will be stored here
     */
    protected void calculatePivotTuple(final O obj, short[] tuple)
            throws OBException {
        assert tuple.length == this.getPivotCount();
        int i = 0;
        while (i < tuple.length) {
            tuple[i] = obj.distance(this.pivots[i]);
            i++;
        }
    }

    /*
     * private Object readResolve() throws DatabaseException,
     * NotFrozenException, DatabaseException, IllegalAccessException,
     * InstantiationException { super.initSpaceTreeLeaves(); resultCache = new
     * HashMap < O, OBQueryShort < O >>(resultCacheSize); return this; }
     */

    /**
     * Returns i >=0 if both tuples are the same, otherwise, it returns -1 the
     * first non zero pair. This is used to find pairs of tuples that are likely
     * to be similar The second tuple is made of shorts except its first item.
     * Warning: the given tuple input will be modified. Only if all the 30
     * pivots have the same value this method will return true. This also means
     * that the next in.getInt() will correctly return the id of the object.
     * Only in those cases in.getInt() will return something meaningful.
     * @param tuple
     * @param tuple2
     * @return true i >=0 if both tuples are the same, otherwise, it returns -1
     */
    protected long equalTuples(short[] tuple, ByteBuffer in) {

        int i = 0;
        while (i < tuple.length) {
            if (tuple[i] != in.getShort()) {
                return -1;
            }
            i++;
        }
        return in.getLong();
    }

    public OperationStatus exists(O object) throws OBException,
            IllegalAccessException, InstantiationException {
        OperationStatus res = new OperationStatus(Status.NOT_EXISTS);
        OBPriorityQueueShort < O > result = new OBPriorityQueueShort < O >(
                (byte) 1);
        searchOB(object, (short) 1, result);

        if (result.getSize() == 1) {
            OBResultShort < O > r = result.iterator().next();
            if (r.getObject().equals(object)) {
                res.setStatus(Status.EXISTS);
                res.setId(r.getId());
            }
        }
        return res;
    }

  
    @Override
    protected OperationStatus deleteAux(final O object) throws OBException,
            IllegalAccessException, InstantiationException {
        long resId = -1;
        OperationStatus res = new OperationStatus(Status.NOT_EXISTS);
        short[] tuple = new short[getPivotCount()];
        // calculate the pivot for the given object
        calculatePivotTuple(object, tuple);
       
        double[] first = normalizeFirstPass(tuple);
        double ppTreeValue = super.ppvalue(first);

        CloseIterator < TupleDouble > it = C
                .processRange(ppTreeValue, ppTreeValue);
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
    

    
	
    

    @Override
	public void searchOB(O object, short r, Filter<O> filter,
			OBPriorityQueueShort<O> result) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		
    	throw new UnsupportedOperationException();
	}

	public double distance(O a, O b) throws OBException {
        short result = ((OBShort) a).distance((OBShort) b);
        return normalizeFirstPassAux(result);
    }

}
