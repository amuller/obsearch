package net.obsearch.index.pyramid;

import hep.aida.bin.QuantileBin1D;
import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import net.obsearch.OB;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.pivot.AbstractPivotOBIndex;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.TupleBytes;


import net.obsearch.storage.OBStoreDouble;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleDouble;
import net.obsearch.storage.TupleLong;
import org.apache.log4j.Logger;

import cern.colt.list.DoubleArrayList;
import cern.colt.list.DoubleArrayList;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.PreloadConfig;
import com.thoughtworks.xstream.annotations.XStreamAlias;

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
 * This Index uses the extended pyramid technique and SMAP to store arbitrary
 * objects.
 * @param <O>
 *                The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
@XStreamAlias("ExtendedPyramidIndex")
public abstract class AbstractExtendedPyramidIndex < O extends OB >
        extends AbstractPivotOBIndex < O > {

    /**
     * Used to access the first item of a two item array.
     */
    protected static final int MIN = 0;

    /**
     * Used to access the second item of a two item array.
     */
    protected static final int MAX = 1;

    /**
     * Used to access the first item of a two item array. (Used for queries)
     */
    protected static final int HLOW = 0;

    /**
     * Used to access the second item of a two item array. (Used for queries)
     */
    protected static final int HHIGH = 1;

    /**
     * Logger.
     */
    // private static final transient Logger logger = Logger
    // .getLogger(AbstractExtendedPyramidIndex.class);
    private static Logger logger = Logger
            .getLogger(AbstractExtendedPyramidIndex.class.getSimpleName());

    /**
     * Median points of the extended pyramid technique.
     */
    private double[] mp;

    /**
     * Pyramid values
     */
    protected OBStoreDouble C;

    /**
     * Pyramid values
     */
    protected OBStoreLong B;

    /**
     * Constructs an extended pyramid index.
     */
    public AbstractExtendedPyramidIndex(Class < O > type,
            IncrementalPivotSelector < O > pivotSelector, int pivotCount)
            throws OBStorageException, OBException {
        super(type, pivotSelector, pivotCount); // initializes the databases
        mp = new double[super.getPivotCount()];
    }

    public void init(OBStoreFactory fact) throws OBStorageException,
            OBException, NotFrozenException, IllegalAccessException,
            InstantiationException, OBException {
        super.init(fact);
        this.C = fact.createOBStoreDouble("C", false, true);
        if (!this.isFrozen()) {
            this.B = fact.createOBStoreLong("B", true, false);
        }

    }

    @Override
    public void close() throws OBException {
        C.close();
        B.close();
        super.close();
        
    }

    /**
     * Calculates the pyramid's median values. We basically have to get the
     * median from each dimension and use the median to approximate the center
     * of the data Using Colt's QuantileBin1D to extract the median. It allows
     * an approximate but memory friendly processing! :)
     * @throws DatabaseException
     *                 If a database error occurs.
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws OBException
     *                 User generated exception
     */
    protected void calculateIndexParameters() throws IllegalAccessException,
            InstantiationException, OutOfRangeException, OBException {

        // each median for each dimension will be stored in this array
        QuantileBin1D[] medianHolder = createMedianHolders(B.size());
        
        StaticBin1D qt = new StaticBin1D();


        CloseIterator < TupleLong > it = this.B.processAll();
        try {
            while (it.hasNext()) {
                TupleLong t = it.next();
                double [] vect = extractTuple(t.getValue());
                updateMedianHolder(vect, medianHolder);
                calculateDistances(vect, qt);
            }
        } finally {
            it.closeCursor();
        }
        logger.info("Separation: " + qt.mean() + "std:" + qt.standardDeviation() + " min " + qt.min());
        int i = 0;
        while (i < mp.length) {
            double median = medianHolder[i].median();
            assert median >= 0 && median <= 1;
            mp[i] = median;
            i++;
        }
        logger.debug("Median calculation finished");
    }
    
    private void calculateDistances(double[] vector, StaticBin1D medianHolder){
        Arrays.sort(vector);
        
        int i = 0;
        while(i < vector.length-1){
            medianHolder.add(Math.abs(vector[i] - vector[i+1]));
            i++;
        }
    }

    /**
     * Extracts the tuple values from in and leaves the values as doubles. The
     * resulting values must be normalized.
     * @param in
     *                Extracts a tuple from the given byte stream.
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @return A double tuple (the input of the pyramid technique)
     */
    protected abstract double[] extractTuple(ByteBuffer in)
            throws OutOfRangeException;

    @Override
    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBStorageException, OutOfRangeException, OBException {
        super.freeze();
        // now process each object and put it in B.
        CloseIterator < TupleLong > it = A.processAll();
        try {
            while (it.hasNext()) {
                TupleLong tup = it.next();
                O obj = bytesToObject(tup.getValue());
                ByteBuffer pivots = objectToProjectionBytes(obj);
                B.put(tup.getKey(), pivots);
            }
        } finally {
            it.closeCursor();
        }

        calculateIndexParameters();

        //B.deleteAll();

        it = A.processAll();

        // now we have to insert all the data again.
        try {
            while (it.hasNext()) {
                TupleLong tup = it.next();
                O obj = bytesToObject(tup.getValue());
                insertAux(tup.getKey(), obj);
            }
        } finally {
            it.closeCursor();
        }
        // done.
    }

    /**
     * Updates each median holder with the given double tuple.
     * @param tuple
     *                TupleInput of doubles
     * @param medianHolder
     *                an array of QuantileBin1D objects used to find an
     *                approximate median.
     */
    protected final void updateMedianHolder(final double[] tuple,
            QuantileBin1D[] medianHolder) {
        int i = 0;
        assert tuple.length == medianHolder.length;
        assert medianHolder.length == super.getPivotCount();
        while (i < medianHolder.length) {
            medianHolder[i].add(tuple[i]);
            i++;
        }
    }

    protected final void updateDoubleHolder(final double[] tuple,
            DoubleArrayList[] medianHolder) {
        int i = 0;
        assert tuple.length == medianHolder.length;
        assert medianHolder.length == getPivotCount();
        while (i < medianHolder.length) {
            medianHolder[i].add(tuple[i]);
            i++;
        }
    }

    /**
     * Creates pivotsCount QuantileBin1D objects that will be used to calculate
     * the medians of the data.
     * @param size
     *                the size of the data to be processed
     * @return an array of QuantileBin1D objects
     */
    protected final QuantileBin1D[] createMedianHolders(final long size) {
        QuantileBin1D[] res = new QuantileBin1D[getPivotCount()];
        int i = 0;
        while (i < res.length) {
            // TODO: move these parameters to a centralized
            // configuration file.
            res[i] = new QuantileBin1D(true, size, 0.00001, 0.00001, 10000,
                    new cern.jet.random.engine.DRand(new java.util.Date()),
                    true, true, 2);
            i++;
        }
        return res;
    }

    protected final DoubleArrayList[] createDoubleHolders(int size) {
        DoubleArrayList[] res = new DoubleArrayList[getPivotCount()];
        int i = 0;
        while (i < res.length) {
            // TODO: move these parameters to a centralized
            // configuration file.
            res[i] = new DoubleArrayList(size);
            i++;
        }
        return res;
    }

    /**
     * Normalizes a value in the given dimension. The value must have been
     * converted into a double [0,1] before using this method
     * @param norm
     *                Normalizes the value in the given dimension
     * @param i
     *                Dimension to use.
     * @return Normalized version of the value.
     */
    protected final double extendedPyramidNormalization(final double norm,
            final int i) {
        return (double) Math.pow(norm, -1.d / (Math.log(mp[i]) / Math.log(2)));
    }

    /**
     * For the given point and the pyramid number we return the height of that
     * point.
     * @param tuple
     *                tuple to be processed
     * @param pyramidNumber
     *                which pyramid number will be processed.
     * @return height of the point
     */
    protected final double heightOfPoint(final double[] tuple,
            final int pyramidNumber) {
        double res = Math.abs(0.5 - tuple[pyramidNumber % getPivotCount()]);
        assert res >= 0 && res <= 0.5;
        return res;
    }

    /**
     * Returns the pyramid value for the given tuple.
     * @param Normalized
     *                tuple (first pass)
     * @return The pyramid value for the given tuple.
     */
    public final double pyramidValue(final double[] tuple) {
        int pyramid = pyramidOfPoint(tuple);
        assert pyramid >= 0 && pyramid < getPivotCount() * 2 : " Pyramid value:"
                + pyramid;
        return pyramid + heightOfPoint(tuple, pyramid);
    }

    /**
     * Calculates the pyramid # for the given point.
     * @param tuple
     *                Normalized tuple (first pass)
     * @return The pyramid # for the given tuple
     */
    public final int pyramidOfPoint(final double[] tuple) {
        int jmax = pyramidOfPointAux(tuple);
        if (tuple[jmax] < 0.5) {
            return jmax;
        } else {
            return jmax + getPivotCount();
        }
    }

    /**
     * For the given tuple, returns the pyramid # for the tuple.
     * @param tuple
     *                Normalized tuple (first pass)
     * @return pyramid # for the given tuple
     */
    protected final int pyramidOfPointAux(final double[] tuple) {
        int j = 0;
        assert getPivotCount() == tuple.length;
        while (j < getPivotCount()) {
            int k = 0;
            boolean failed = false;
            while (k < getPivotCount() && !failed) {
                if (k == j) { // can't process k == j
                    k++;
                    continue;
                }
                if (!(Math.abs(0.5 - tuple[j]) >= Math.abs(0.5 - tuple[k]))) {
                    // if this property is not fullfilled we have to finish the
                    // loop
                    // we also failed for this j, need to try the next j
                    failed = true;
                }
                k++;
            }
            if (!failed) {
                // we did not fail, let's return j_max
                // we should always exit the method here
                return j;
            }
            j++;
        }
        assert false : " Catastrophic Failure "; // we shouldn't have reached
        return Integer.MIN_VALUE;
    }

    /**
     * Returns true if the given query (min[] and max[]) intersects pyramid p.
     * It also modifies lowHighResult so that we can perform the range
     * accordingly.
     * @param q
     *                A query rectangle.
     * @param p
     *                The pyramid number
     * @param lowHighResult
     *                Where the lowHighResult will be stored.
     * @return True if rectangle q intersects pyramid p
     */
    protected final boolean intersect(final double[][] q, final int p,
            double[] minArray, double[] lowHighResult) {
        // strategy: as soon as we find something is false, we stop processing
        // otherwise we return true! :)
        int i = p;
        assert i < this.getPivotCount() * 2;
        int j = 0;
        if (i < this.getPivotCount()) { // the case where i < d
            double minimum = q[i][MIN];
            while (j < q.length) {
                if (j != i) {
                    if (!(minimum <= -minArray[j])) {
                        return false;
                    }
                }
                j++;
            }
            assert j == getPivotCount();
            if (q[i][MAX] > 0) {
                q[i][MAX] = 0;
            }
        } else { // i >= d
            i = i - this.getPivotCount();
            double maximum = q[i][MAX];
            while (j < q.length) { // this is the first definition!!!
                if (j != i) {
                    if (!(maximum >= minArray[j])) {
                        return false;
                    }
                }
                j++;
            }
            assert j == getPivotCount();
            if (q[i][MIN] < 0) {
                q[i][MIN] = 0;
            }
        }
        // if we reach this, is because the pyramid intersects
        // we have to get the ranges now.
        determineRanges(i, q, lowHighResult);
        return true;
    }

    /**
     * Receives a query and returns a new array generated of calculating min()
     * to each of the elements
     * @param query
     * @return The array of the min(qj) for each element of the query
     */
    protected final double[] generateMinArray(double[][] query) {
        int i = 0;
        double[] res = new double[query.length];
        while (i < query.length) {
            res[i] = min(query[i]);
            i++;
        }
        return res;
    }

    /**
     * @return The total number of boxes served by this index.
     */
    public long totalBoxes() {
        return getPivotCount() * 2;
    }

    /**
     * Determines the ranges that have to be searched in the b-tree.
     * @param p
     *                Pyramid #
     * @param q
     *                the query
     * @param lowHighResult
     *                where the high a low will be stored (a two element array)
     */
    private final void determineRanges(int p, double[][] q,
            double[] lowHighResult) {

        int i = p;
        assert i < getPivotCount();
        lowHighResult[HHIGH] = max(q[i]);
        if (isEasyCase(q)) { // do not use max2 here
            lowHighResult[HLOW] = 0;
        } else {
            int j = 0;
            double max = 0;
            while (j < getPivotCount()) {
                if (i != j) {
                    double t = qjmin(q, j, i);
                    if (t > max) {
                        max = t;
                    }
                }
                j++;
            }
            lowHighResult[HLOW] = max;
        }
    }

    /**
     * Finds qjmin for the given j, and the given i pyramid and the ranges of
     * the query max and min. Please see the paper on the pyramid technique.
     * @param q
     *                Query rectangle
     * @param j
     *                j parameter
     * @param i
     *                i parameter
     * @return qjmin for the given q,j,i.
     */
    private final double qjmin(final double[][] q, final int j, final int i) {
        if (min(q[j]) > min(q[i])) {
            return min(q[j]);
        } else {
            return min(q[i]);
        }
    }

    /**
     * Returns true if the query is an easy case. Please see the paper on the
     * pyramid technique.
     * @param q
     *                A query rectangle
     * @return True if this query is an "easy case" query.
     */
    private final boolean isEasyCase(final double[][] q) {
        int i = 0;
        while (i < q.length) {
            if (!((q[i][MIN] <= 0) && (0 <= q[i][MAX]))) {
                return false;
            }
            i++;
        }
        return true;
    }

    /**
     * Calculates min for the given 2 element array.
     * @param minMax
     *                2 element array.
     * @return min
     */
    private final double min(final double[] minMax) {
        if (minMax[MIN] <= 0 && 0 <= minMax[MAX]) {
            return 0;
        } else {
            return Math.min(Math.abs(minMax[MAX]), Math.abs(minMax[MIN]));
        }
    }

    /**
     * Calculates max for the given 2 element array.
     * @param minMax
     *                2 element array.
     * @return max
     */
    private final double max(final double[] minMax) {
        return Math.max(Math.abs(minMax[MAX]), Math.abs(minMax[MIN]));
    }

    /**
     * Queries are aligned to the center of the space. Aligns the given query to
     * the center of the space.
     * @param q
     *                query.
     */
    protected final void centerQuery(final double[][] q) {
        int i = 0;
        while (i < q.length) {
            q[i][MIN] = q[i][MIN] - 0.5f;
            q[i][MAX] = q[i][MAX] - 0.5f;
            i++;
        }
    }

    /**
     * Copies the contents of query src into dest.
     * @param src
     *                source
     * @param dest
     *                destination
     */
    protected final void copyQuery(final double[][] src, double[][] dest) {
        int i = 0;
        while (i < src.length) {
            dest[i][MIN] = src[i][MIN];
            dest[i][MAX] = src[i][MAX];
            i++;
        }
    }

}
