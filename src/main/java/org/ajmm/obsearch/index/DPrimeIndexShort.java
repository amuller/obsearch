package org.ajmm.obsearch.index;

import hep.aida.bin.StaticBin1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.cache.OBCacheLoaderLong;
import org.ajmm.obsearch.cache.OBCacheLong;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.d.BucketContainer;
import org.ajmm.obsearch.index.d.BucketContainerShort;
import org.ajmm.obsearch.index.d.ObjectBucket;
import org.ajmm.obsearch.index.d.ObjectBucketShort;
import org.ajmm.obsearch.index.utils.IntegerHolder;
import org.ajmm.obsearch.index.utils.ShortUtils;
import org.ajmm.obsearch.index.utils.StatsUtil;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreLong;
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;
import cern.colt.list.ShortArrayList;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public final class DPrimeIndexShort < O extends OBShort >
        extends
        AbstractDPrimeIndex < O, ObjectBucketShort, OBQueryShort < O >, BucketContainerShort < O > >
        implements IndexShort < O > {

    public int hackOne = 5;

   

    /**
     * P parameter that indicates the maximum radius that we will accept.
     */
    private short p;

    /**
     * Median data for each level and pivot.
     */
    private short[] median;

    /**
     * Obtain max distance per pivot... hack. Remove this in the future.
     */
    private short maxDistance;

    /**
     * For each pivot, we have here how many objects fall in distance x. Max:
     * right of the median Min: left of the median
     */
    private int[][] distanceDistribution;

    protected float[][] normalizedProbs;

    /**
     * Cache used to store recently accessed mbr :)
     */
    protected transient OBCacheLong < short[][] > mbrCache;

    /**
     * Storage used to hold MBRs :)
     */
    protected transient OBStoreLong mbrs;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(DPrimeIndexShort.class);

    /**
     * Creates a new DIndex for shorts
     * @param fact
     *                Storage factory to use
     * @param pivotCount
     *                number of pivots to use.
     * @param pivotSelector
     *                Pivot acceptance criteria.
     * @param type
     *                The type of objects to use (needed to create new
     *                instances)
     * @param nextLevelThreshold
     *                threshold used to reduce the number of pivots per level.
     * @param p
     *                P parameter of D-Index.
     * @throws OBStorageException
     *                 If something goes wrong with the storage device.
     * @throws OBException
     *                 If some other exception occurs.
     */
    public DPrimeIndexShort(OBStoreFactory fact, byte pivotCount,
            IncrementalPivotSelector < O > pivotSelector, Class < O > type,
            short p) throws OBStorageException, OBException {
        super(fact, pivotCount, pivotSelector, type);
        this.p = p;
    }

    protected void init() throws OBStorageException, OBException {
        super.init();
        if (this.mbrCache == null) {
            mbrCache = new OBCacheLong < short[][] >(new MBRLoader());
        }

    }

    protected void initStorageDevices() throws OBStorageException {
        super.initStorageDevices();
        // init mbr storage.
        this.mbrs = fact.createOBStoreLong("MBRs", false);
    }

    public void close() throws OBStorageException {
        this.mbrs.close();
        super.close();
    }

    /**
     * Puts the MBR of the container in the DB.
     * @param bc
     */
    protected void putMBR(long id, BucketContainerShort < O > bc)
            throws OBStorageException, IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OutOfRangeException, OBException {
        short[][] mbr = bc.getMBR();
        if (mbr != null) {
            TupleOutput out = new TupleOutput();
            for (short[] m : mbr) {
                for (short d : m) {
                    out.writeShort(d);
                }
            }
            this.mbrCache.remove(id);
            this.mbrs.put(id, out.getBufferBytes());
            
        } else {
            assert bc.size() == 0;
        }
    }

    protected ObjectBucketShort getBucket(O object, short p) throws OBException {

        int i = 0;
        ArrayList < O > piv = super.pivots;
        short[] smapVector = new short[piv.size()];
        long bucketId = 0;
        while (i < piv.size()) {
            short distance = piv.get(i).distance(object);
            smapVector[i] = distance;
            i++;
        }
        ObjectBucketShort res = new ObjectBucketShort(bucketId, 0, smapVector,
                false, -1);
        updateBucket(res);
        return res;
    }

    /**
     * Calculate a new bucket based on the smap vector of the given b Warning,
     * this method destroys the previously available info in the given bucket b.
     * It keeps the smap vector intact.
     * @param b
     *                We will take the smap vector from here.
     * @param level
     *                Level of the hash table
     * @param p
     *                P value to use
     * @throws OBException
     */
    protected void updateBucket(ObjectBucketShort b) throws OBException {
        int i = 0;
        ArrayList < O > piv = super.pivots;
        short[] medians = median;
        short[] smapVector = b.getSmapVector();
        long bucketId = 0;
        while (i < piv.size()) {
            short distance = smapVector[i];
            int r = bps(medians[i], distance);
            if (r == 1) {
                bucketId = bucketId | super.masks[i];
            }
            i++;
        }
        b.setBucket(bucketId);
        b.setExclusionBucket(false);
        b.setLevel(0);

    }

    @Override
    protected ObjectBucketShort getBucket(O object) throws OBException {
        int level = 0;
        ObjectBucketShort res = null;
        while (level < super.pivots.size()) {
            res = getBucket(object, (short) 0);
            if (!res.isExclusionBucket()) {
                break;
            }
            level++;
        }
        assert res != null;
        return res;
    }

    /**
     * Bps function. Returns 0 if d(o,p) <= median - p . Returns 1 if d(o,p) >
     * median +p. Returns 2 otherwise.
     * @param median
     *                Median obtained for the given pivot.
     * @param distance
     *                Distance of the pivot and the object we are processing
     * @return Returns 0 if d(o,p) <= median - p . Returns 1 if d(o,p) > median
     *         +p. Returns 2 otherwise.
     */
    private int bps(short median, short distance) {
        if (distance <= median) {
            return 0;
        } else {
            return 1;
        }
    }

    private int bpsRange(short median, short distance, short range) {
        boolean a = distance - range <= median;
        boolean b = distance + range >= median;
        if (a && b) {
            return 2;
        } else if (a) {
            return 0;
        } else if (b) {
            return 1;
        } else {
            assert false;
            return -1;
        }
    }

    protected void calculateMedians(IntArrayList elementsSource)
            throws OBStorageException, IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OutOfRangeException, OBException {
        int i = 0;
        ArrayList < O > pivots = super.pivots;

        int max;
        if (elementsSource == null) {
            max = (int) A.size();
        } else {
            max = elementsSource.size();
        }

        logger.debug("Calculating medians. max: " + max);
        assert pivots.size() > 0;
        median = new short[pivots.size()];
        while (i < pivots.size()) {
            O p = pivots.get(i);
            int cx = 0;
            ShortArrayList medianData = new ShortArrayList(max);
            // calculate median for pivot p
            while (cx < max) {
                O o = getObjectFreeze(cx, elementsSource);
                short d = p.distance(o);
                if (maxDistance < d) {
                    maxDistance = d;
                }
                medianData.add(d);
                cx++;
            }

            median[i] = median(medianData);
            i++;
        }
        logger.info("max distance: " + maxDistance);
        maxDistance++;
        assert i > 0;
        if (logger.isDebugEnabled()) {
            logger.debug("Found medians: " + Arrays.toString(median));
        }

        assert super.pivots.size() == median.length : "Piv: "
                + super.pivots.size() + " Med: " + median.length;
        this.distanceDistribution = new int[pivots.size()][maxDistance + 1];
    }

    /**
     * Updates probability information.
     * @param b
     */
    protected void updateProbabilities(ObjectBucketShort b) {
        int i = 0;
        while (i < pivots.size()) {

            this.distanceDistribution[i][b.getSmapVector()[i]]++;

            i++;
        }

    }

    protected void normalizeProbs() throws OBStorageException {
        normalizedProbs = new float[pivots.size()][this.maxDistance + 1];
        long total = A.size();
        int i = 0;
        while (i < pivots.size()) {

            // calculate accum distances
            int m = this.median[i];
            // initialize center
            normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total);
            m--;
            // process the left side of the median
            while (m >= 0) {
                normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total)
                        + normalizedProbs[i][m + 1];
                m--;
            }
            // process the right side of the median

            m = this.median[i] + 1; // do not include the median.
            if (m < normalizedProbs[i].length) {
                normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total);
                m++;
            }
            while (m < normalizedProbs[i].length) {
                normalizedProbs[i][m] = ((float) distanceDistribution[i][m] / (float) total)
                        + normalizedProbs[i][m - 1];
                m++;
            }
            i++;
        }
    }

    private short median(ShortArrayList medianData) {
        medianData.sort();
        return medianData.get(medianData.size() / 2);
    }

    protected BucketContainerShort < O > instantiateBucketContainer(byte[] data) {
        return new BucketContainerShort < O >(this, data);
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#intersectingBoxes(org.ajmm.obsearch.ob.OBShort,
     *      short)
     */
    @Override
    public int[] intersectingBoxes(O object, short r)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#intersects(org.ajmm.obsearch.ob.OBShort,
     *      short, long)
     */
    @Override
    public boolean intersects(O object, short r, int box)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        throw new UnsupportedOperationException();

    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#searchOB(org.ajmm.obsearch.ob.OBShort,
     *      short, org.ajmm.obsearch.result.OBPriorityQueueShort, long[])
     */
    @Override
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result,
            int[] boxes) throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        throw new UnsupportedOperationException();

    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#searchOB(org.ajmm.obsearch.ob.OBShort,
     *      short, org.ajmm.obsearch.result.OBPriorityQueueShort)
     */
    @Override
    /*
     * public void searchOB(O object, short r, OBPriorityQueueShort < O >
     * result) throws NotFrozenException, DatabaseException,
     * InstantiationException, IllegalIdException, IllegalAccessException,
     * OutOfRangeException, OBException { OBQueryShort<O> q = new OBQueryShort<O>(object,r,
     * result); int i = 0; ObjectBucketShort b = null; while(i <
     * pivots.size()){// search through all the levels. b = getBucket(object, i,
     * (short)(p + r)); if(! b.isExclusionBucket()){ BucketContainerShort<O> bc =
     * super.bucketContainerCache.get(super.getBucketStorageId(b));
     * bc.search(q,b); return; } if(r <= p){ this.updateBucket(b, i, (short)(p -
     * r)); if(! b.isExclusionBucket()){ BucketContainerShort<O> bc =
     * super.bucketContainerCache.get(super.getBucketStorageId(b)); bc.search(q,
     * b); } }else{ throw new UnsupportedOperationException("Only supporting
     * ranges < p"); } i++; } // finally, search the exclusion bucket :)
     * BucketContainerShort<O> bc =
     * super.bucketContainerCache.get(super.exclusionBucketId); bc.search(q, b); }
     */
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {

        ObjectBucketShort b = getBucket(object);
        OBQueryShort < O > q = new OBQueryShort < O >(object, r, result, b
                .getSmapVector());

        this.queryCount++;

        this.s(b.getBucket(), b, q, false);

        if (hackOne == 5) {
            doIt1(b, q, 0, 0); // keep
        } else if (hackOne == 6) {
            doIt2(b, q, 0, 0); // keep.
        }
        /*
         * while (i < pivots.size()) {// search through all the levels. b =
         * getBucket(object, i, (short) 0); assert !b.isExclusionBucket();
         * BucketContainerShort < O > bc = super.bucketContainerCache
         * .get(super.getBucketStorageId(b)); bc.search(q, b); i++; } //
         * finally, search the exclusion bucket :) BucketContainerShort < O > bc =
         * super.bucketContainerCache .get(super.exclusionBucketId);
         * bc.search(q, b);
         */
    }

    // assuming that this query goes beyond the median.
    private float calculateZero(ObjectBucketShort b, OBQueryShort < O > q,
            int pivotIndex) {
        short base = (short) Math.max(q.getLow()[pivotIndex], 0);
        return this.normalizedProbs[pivotIndex][base];
    }

    // assuming that this query goes beyond the median.
    private float calculateOne(ObjectBucketShort b, OBQueryShort < O > q,
            int pivotIndex) {
        short top = (short) Math.min(q.getHigh()[pivotIndex], maxDistance);
        return this.normalizedProbs[pivotIndex][top];
    }

    /**
     * Does the match for the given index for the given pivot.
     * @param b
     * @param q
     * @param pivotIndex
     */
    private void doIt1(ObjectBucketShort b, OBQueryShort < O > q,
            int pivotIndex, long block) throws NotFrozenException,
            DatabaseException, InstantiationException, IllegalIdException,
            IllegalAccessException, OutOfRangeException, OBException {
        if (pivotIndex < super.pivots.size()) {
            int r = bpsRange(median[pivotIndex], b.getSmapVector()[pivotIndex],
                    q.getDistance());
            if (r == 2) { // if we have to do both
                if (calculateOne(b, q, pivotIndex) > calculateZero(b, q,
                        pivotIndex)) {
                    // do 1 first
                    long newBlock = block | super.masks[pivotIndex];
                    if (super.filter.get(pivotIndex).contains(newBlock)) {
                        doIt1(b, q, pivotIndex + 1, newBlock);
                    }
                    r = bpsRange(median[pivotIndex],
                            b.getSmapVector()[pivotIndex], q.getDistance());
                    if ((r == 2 || r == 0)
                            && super.filter.get(pivotIndex).contains(block)) {
                        doIt1(b, q, pivotIndex + 1, block);
                    }

                } else {
                    // 0 first
                    if (super.filter.get(pivotIndex).contains(block)) {
                        doIt1(b, q, pivotIndex + 1, block);
                    }
                    r = bpsRange(median[pivotIndex],
                            b.getSmapVector()[pivotIndex], q.getDistance());
                    long newBlock = block | super.masks[pivotIndex];
                    if ((r == 2 || r == 1)
                            && super.filter.get(pivotIndex).contains(newBlock)) {

                        doIt1(b, q, pivotIndex + 1, newBlock);
                    }
                }

            } else { // only one of the sides is selected
                if (r == 0 && super.filter.get(pivotIndex).contains(block)) {
                    doIt1(b, q, pivotIndex + 1, block);
                } else {
                    long newBlock = block | super.masks[pivotIndex];
                    if (super.filter.get(pivotIndex).contains(newBlock)) {
                        doIt1(b, q, pivotIndex + 1, newBlock);
                    }
                }
            }

        } else {

            s(block, b, q, true);
        }
    }

    // based on the center of the query!
    private void doIt2(ObjectBucketShort b, OBQueryShort < O > q,
            int pivotIndex, long block) throws NotFrozenException,
            DatabaseException, InstantiationException, IllegalIdException,
            IllegalAccessException, OutOfRangeException, OBException {
        if (pivotIndex < super.pivots.size()) {
            int r = bpsRange(median[pivotIndex], b.getSmapVector()[pivotIndex],
                    q.getDistance());
            if (r == 2) { // if we have to do both
                if (b.getSmapVector()[pivotIndex] > median[pivotIndex]) {
                    // do 1 first
                    long newBlock = block | super.masks[pivotIndex];
                    if (super.filter.get(pivotIndex).contains(newBlock)) {
                        doIt2(b, q, pivotIndex + 1, newBlock);
                    }
                    r = bpsRange(median[pivotIndex],
                            b.getSmapVector()[pivotIndex], q.getDistance());
                    if ((r == 2 || r == 0)
                            && super.filter.get(pivotIndex).contains(block)) {
                        doIt2(b, q, pivotIndex + 1, block);
                    }

                } else {
                    // 0 first
                    if (super.filter.get(pivotIndex).contains(block)) {
                        doIt2(b, q, pivotIndex + 1, block);
                    }
                    r = bpsRange(median[pivotIndex],
                            b.getSmapVector()[pivotIndex], q.getDistance());
                    long newBlock = block | super.masks[pivotIndex];
                    if ((r == 2 || r == 1)
                            && super.filter.get(pivotIndex).contains(newBlock)) {

                        doIt2(b, q, pivotIndex + 1, newBlock);
                    }
                }

            } else { // only one of the sides is selected
                if (r == 0 && super.filter.get(pivotIndex).contains(block)) {
                    doIt2(b, q, pivotIndex + 1, block);
                } else {
                    long newBlock = block | super.masks[pivotIndex];
                    if (super.filter.get(pivotIndex).contains(newBlock)) {
                        doIt2(b, q, pivotIndex + 1, newBlock);
                    }
                }
            }

        } else {

            s(block, b, q, true);
        }
    }

    /**
     * @param block
     *                The block to be processed
     * @param b
     *                The block of the object
     * @param q
     *                The query
     * @param ignoreSameBlocks
     *                if true, if block == b.getBucket() nothing happens.
     * @throws NotFrozenException
     * @throws DatabaseException
     * @throws InstantiationException
     * @throws IllegalIdException
     * @throws IllegalAccessException
     * @throws OutOfRangeException
     * @throws OBException
     */
    private void s(long block, ObjectBucketShort b, OBQueryShort < O > q,
            boolean ignoreSameBlocks) throws NotFrozenException,
            DatabaseException, InstantiationException, IllegalIdException,
            IllegalAccessException, OutOfRangeException, OBException {

        short[][] rect = this.mbrCache.get(block);

        if (rect != null && q.collides(rect)) {

            if (!ignoreSameBlocks || block != b.getBucket()) {
                // we have finished
                BucketContainerShort < O > bc = super.bucketContainerCache
                        .get(block);
           
                    IntegerHolder h = new IntegerHolder(0);
                    super.distanceComputations += bc.searchSorted(q, b, h);
                    smapRecordsCompared += h.getValue();
                
                searchedBoxesTotal++;
            }
        }
    }

    @Override
    public Result exists(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        OBPriorityQueueShort < O > result = new OBPriorityQueueShort < O >(
                (byte) 1);
        searchOB(object, (short) 0, result);
        Result res = new Result();
        res.setStatus(Result.Status.NOT_EXISTS);
        if (result.getSize() == 1) {
            Iterator < OBResultShort < O >> it = result.iterator();
            OBResultShort < O > r = it.next();
            if (r.getObject().equals(object)) {
                res.setId(r.getId());
                res.setStatus(Result.Status.EXISTS);
            }
        }
        return res;
    }

    private class MBRLoader implements OBCacheLoaderLong < short[][] > {

        public int getDBSize() throws OBStorageException {
            return (int) mbrs.size();
        }

        public short[][] loadObject(long id) throws DatabaseException,
                OutOfRangeException, OBException, InstantiationException,
                IllegalAccessException, IllegalIdException {
            byte[] data = mbrs.getValue(id);
            if (data != null) {
                TupleInput in = new TupleInput(data);
                short[][] res = new short[2][pivots.size()];
                for (short[] m : res) {
                    int i = 0;
                    while (i < m.length) {
                        m[i] = in.readShort();
                        i++;
                    }
                }
                return res;
            } else {
                return null;
            }

        }

    }

    public String getStats() {
        StringBuilder res = new StringBuilder();
        res.append("Query count: " + queryCount);
        res.append("\n");
        res.append("Total boxes: " + searchedBoxesTotal);
        res.append("\n");
        res.append("Smap records: " + smapRecordsCompared);
        res.append("\n");
        res.append("Distance computations: " + distanceComputations);
        res.append("\n");
        res.append("Data read: " + dataRead);
        res.append("\n");
        res.append(StatsUtil.mightyIOStats("A", A.getReadStats()));
        res.append("\n");
        res.append(StatsUtil.mightyIOStats("Buckets", Buckets.getReadStats()));
        res.append("\n");
        res.append(StatsUtil.mightyIOStats("MBR", this.mbrs.getReadStats()));
        return res.toString();
    }

    public void resetStats() {
        queryCount = 0;
        searchedBoxesTotal = 0;
        smapRecordsCompared = 0;
        distanceComputations = 0;
        super.dataRead = 0;
        A.setReadStats(new StaticBin1D());
        Buckets.setReadStats(new StaticBin1D());
        mbrs.setReadStats(new StaticBin1D());
    }

}
