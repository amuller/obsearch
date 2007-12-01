package org.ajmm.obsearch.index;

import hep.aida.bin.QuantileBin1D;

import java.io.File;
import java.io.IOException;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.apache.log4j.Logger;

import cern.colt.list.FloatArrayList;

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
 *            The type of object to be stored in the Index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
@XStreamAlias("ExtendedPyramidIndex")
public abstract class AbstractExtendedPyramidIndex < O extends OB >
        extends AbstractPivotIndex < O > {

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
    private static final transient Logger logger = Logger
            .getLogger(AbstractExtendedPyramidIndex.class);

    /**
     * Median points of the extended pyramid technique.
     */
    protected float[] mp;

    /**
     * The database where the pyramid values are stored.
     */
    protected transient Database cDB;

    /**
     * Constructs an extended pyramid index.
     * @param databaseDirectory
     *            the database directory
     * @param pivots
     *            how many pivots will be used
     * @throws DatabaseException
     *             If a database error occurs.
     * @throws IOException
     *             If a serialization issue occurs.
     */
    public AbstractExtendedPyramidIndex(final File databaseDirectory,
            final short pivots) throws DatabaseException, IOException {
        super(databaseDirectory, pivots); // initializes the databases
        mp = new float[super.pivotsCount];
    }

    /**
     * This method will be called by the super class. Initializes the C
     * database(s).
     * @throws DatabaseException
     *             If a database error occurs.
     */
    @Override
    protected final void initC() throws DatabaseException {
        DatabaseConfig dbConfig = createDefaultDatabaseConfig();
        
        dbConfig.setSortedDuplicates(true);
        dbConfig.setTransactional(false);
        cDB = databaseEnvironment.openDatabase(null, "C", dbConfig);
                
        PreloadConfig pc = new PreloadConfig();
        pc.setLoadLNs(true);
        // TODO: uncomment this?
        cDB.preload(pc);
    }

    /**
     * Calculates the pyramid's median values. We basically have to get the
     * median from each dimension and use the median to approximate the center
     * of the data Using Colt's QuantileBin1D to extract the median. It allows
     * an approximate but memory friendly processing! :)
     * @throws DatabaseException
     *             If a database error occurs.
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws OBException
     *             User generated exception
     */
    @Override
    protected void calculateIndexParameters() throws DatabaseException,
            IllegalAccessException, InstantiationException,
            OutOfRangeException, OBException {
        long count = super.bDB.count();
        // each median for each dimension will be stored in this array
        QuantileBin1D[] medianHolder = createMedianHolders(count);
        Cursor cursor = null;
        DatabaseEntry foundKey = new DatabaseEntry();
        DatabaseEntry foundData = new DatabaseEntry();

        try {
            int i = 0;
            cursor = bDB.openCursor(null, null);
            while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
                assert i == IntegerBinding.entryToInt(foundKey);

                TupleInput in = new TupleInput(foundData.getData());
                updateMedianHolder(extractTuple(in), medianHolder);
                i++;
            }
            assert i == count; // pivot count and read # of pivots
            // should be the same
        } finally {
            cursor.close();
        }

        int i = 0;
        while (i < mp.length) {
            double median = medianHolder[i].median();
            assert median >= 0 && median <= 1;
            mp[i] = (float) median;
            i++;
        }
        logger.debug("Median calculation finished");
    }

    /**
     * Extracts the tuple values from in and returns a normalized vector with
     * values ranging from 1 to 0. Note that this is first level normalization.
     * The extended pyramid technique algorithm performs another normalization
     * on top of this one.
     * @param in
     *            Extracts a tuple from the given byte stream.
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @return A float tuple (the input of the pyramid technique)
     */
    protected abstract float[] extractTuple(TupleInput in)
            throws OutOfRangeException;

    /**
     * Updates each median holder with the given float tuple.
     * @param tuple
     *            TupleInput of floats
     * @param medianHolder
     *            an array of QuantileBin1D objects used to find an approximate
     *            median.
     */
    protected final void updateMedianHolder(final float[] tuple,
            QuantileBin1D[] medianHolder) {
        int i = 0;
        assert tuple.length == medianHolder.length;
        assert medianHolder.length == pivotsCount;
        while (i < medianHolder.length) {
            medianHolder[i].add(tuple[i]);
            i++;
        }
    }
    
    protected final void updateFloatHolder(final float[] tuple,
            FloatArrayList[] medianHolder) {
        int i = 0;
        assert tuple.length == medianHolder.length;
        assert medianHolder.length == pivotsCount;
        while (i < medianHolder.length) {
            medianHolder[i].add(tuple[i]);
            i++;
        }
    }

    /**
     * Creates pivotsCount QuantileBin1D objects that will be used to calculate
     * the medians of the data.
     * @param size
     *            the size of the data to be processed
     * @return an array of QuantileBin1D objects
     */
    protected final QuantileBin1D[] createMedianHolders(final long size) {
        QuantileBin1D[] res = new QuantileBin1D[pivotsCount];
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
    
    protected final FloatArrayList[] createFloatHolders(int size) {
        FloatArrayList[] res = new FloatArrayList[pivotsCount];
        int i = 0;
        while (i < res.length) {
            // TODO: move these parameters to a centralized
            // configuration file.
            res[i] = new FloatArrayList(size);
            i++;
        }
        return res;
    }

    /**
     * Normalizes a value in the given dimension. The value must have been
     * converted into a float [0,1] before using this method
     * @param norm
     *            Normalizes the value in the given dimension
     * @param i
     *            Dimension to use.
     * @return Normalized version of the value.
     */
    protected final float extendedPyramidNormalization(final float norm,
            final int i) {
        return (float) Math.pow(norm, -1.d / (Math.log(mp[i]) / Math.log(2)));
    }

    /**
     * For the given point and the pyramid number we return the height of that
     * point.
     * @param tuple
     *            tuple to be processed
     * @param pyramidNumber
     *            which pyramid number will be processed.
     * @return height of the point
     */
    protected final float heightOfPoint(final float[] tuple,
            final int pyramidNumber) {
        float res = (float) Math.abs(0.5 - tuple[pyramidNumber % pivotsCount]);
        assert res >= 0 && res <= 0.5;
        return res;
    }

    /**
     * Returns the pyramid value for the given tuple.
     * @param Normalized
     *            tuple (first pass)
     * @return The pyramid value for the given tuple.
     */
    public final float pyramidValue(final float[] tuple) {
        int pyramid = pyramidOfPoint(tuple);
        assert pyramid >= 0 && pyramid < pivotsCount * 2 : " Pyramid value:"
                + pyramid;
        return pyramid + heightOfPoint(tuple, pyramid);
    }

    /**
     * Calculates the pyramid # for the given point.
     * @param tuple
     *            Normalized tuple (first pass)
     * @return The pyramid # for the given tuple
     */
    public final int pyramidOfPoint(final float[] tuple) {
        int jmax = pyramidOfPointAux(tuple);
        if (tuple[jmax] < 0.5) {
            return jmax;
        } else {
            return jmax + pivotsCount;
        }
    }

    /**
     * For the given tuple, returns the pyramid # for the tuple.
     * @param tuple
     *            Normalized tuple (first pass)
     * @return pyramid # for the given tuple
     */
    protected final int pyramidOfPointAux(final float[] tuple) {
        int j = 0;
        assert pivotsCount == tuple.length;
        while (j < pivotsCount) {
            int k = 0;
            boolean failed = false;
            while (k < pivotsCount && !failed) {
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
     *            A query rectangle.
     * @param p
     *            The pyramid number
     * @param lowHighResult
     *            Where the lowHighResult will be stored.
     * @return True if rectangle q intersects pyramid p
     */
    protected final boolean intersect(final float[][] q, final int p,
            float[] minArray, float[] lowHighResult) {
        // strategy: as soon as we find something is false, we stop processing
        // otherwise we return true! :)
        int i = p;
        assert i < this.pivotsCount * 2;
        int j = 0;
        if (i < this.pivotsCount) { // the case where i < d
            float minimum = q[i][MIN];
            while (j < q.length) {
                if (j != i) {
                    if (!(minimum <= - minArray[j])) {
                        return false;
                    }
                }
                j++;
            }
            assert j == pivotsCount;
            if (q[i][MAX] > 0) {
                q[i][MAX] = 0;
            }
        } else { // i >= d
            i = i - this.pivotsCount;
            float maximum = q[i][MAX] ;
            while (j < q.length) { // this is the first definition!!!                
                if (j != i) {
                    if (! (maximum >= minArray[j])) {
                        return false;
                    }
                }
                j++;
            }
            assert j == pivotsCount;
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
     * Receives a query and returns a new array generated of calculating min() to 
     * each of the elements
     * @param query
     * @return The array of the min(qj) for each element of the query
     */
    protected final float[] generateMinArray(float[][] query){
        int i  = 0;
        float [] res = new float[query.length];
        while(i < query.length){
            res[i] = min(query[i]);
            i++;
        }
        return res;
    }

    /**
     * @return The total number of boxes served by this index.
     */
    public int totalBoxes() {
        return pivotsCount * 2;
    }

    /**
     * Determines the ranges that have to be searched in the b-tree.
     * @param p
     *            Pyramid #
     * @param q
     *            the query
     * @param lowHighResult
     *            where the high a low will be stored (a two element array)
     */
    private final void determineRanges(int p, float[][] q, float[] lowHighResult) {

        int i = p;
        assert i < pivotsCount;
        lowHighResult[HHIGH] = max(q[i]);         
        if (isEasyCase(q)) { // do not use max2 here
            lowHighResult[HLOW] = 0;
        } else {   
            int j = 0;
            float max = 0;
            while (j < pivotsCount) {
                if (i != j) {
                    float t = qjmin(q, j, i);
                    if (t > max) {
                        max = t;
                    }
                }
                j++;
            }
            lowHighResult[HLOW] = max;
        }
    }
    

    /*
    private final void determineRanges2 (int p, float[][] q, float[] lowHighResult){
        int j = 0;
        int i = p;
        float min = Float.MAX_VALUE;
        while (j < pivotsCount) {
            if (i != j) {
                float t = qbarjmin(q, j, i);
                if (t < min) {
                    min = t;
                }
            }
            j++;
        }
        lowHighResult[HLOW] = min;
    }
        
    private final float qbarjmin(float[][] q, int j, int i){
        if(max(q[j]) >= min(q[i])){
            return Math.max(max(q[i]), min(q[j]));
        }else{
            return min(q[i]);
        }
    }
*/
    /**
     * Finds qjmin for the given j, and the given i pyramid and the ranges of
     * the query max and min. Please see the paper on the pyramid technique.
     * @param q
     *            Query rectangle
     * @param j
     *            j parameter
     * @param i
     *            i parameter
     * @return qjmin for the given q,j,i.
     */
    private final float qjmin(final float[][] q, final int j, final int i) {
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
     *            A query rectangle
     * @return True if this query is an "easy case" query.
     */
    private final boolean isEasyCase(final float[][] q) {
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
     *            2 element array.
     * @return min
     */
    private final float min(final float[] minMax) {
        if (minMax[MIN] <= 0 && 0 <= minMax[MAX]) {
            return 0;
        } else {
            return Math.min(Math.abs(minMax[MAX]), Math.abs(minMax[MIN]));
        }
    }

    /**
     * Calculates max for the given 2 element array.
     * @param minMax
     *            2 element array.
     * @return max
     */
    private final float max(final float[] minMax) {
        return Math.max(Math.abs(minMax[MAX]), Math.abs(minMax[MIN]));
    }

    /**
     * Queries are aligned to the center of the space. Aligns the given query to
     * the center of the space.
     * @param q
     *            query.
     */
    protected final void centerQuery(final float[][] q) {
        int i = 0;
        while (i < q.length) {
            q[i][MIN] = q[i][MIN] - 0.5f;
            q[i][MAX] = q[i][MAX] - 0.5f;
            i++;
        }
    }

    /**
     * Closes database C.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     */
    @Override
    protected final void closeC() throws DatabaseException {
        this.cDB.close();
    }

    /**
     * Copies the contents of query src into dest.
     * @param src
     *            source
     * @param dest
     *            destination
     */
    protected final void copyQuery(final float[][] src, float[][] dest) {
        int i = 0;
        while (i < src.length) {
            dest[i][MIN] = src[i][MIN];
            dest[i][MAX] = src[i][MAX];
            i++;
        }
    }

}
