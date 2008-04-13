package org.ajmm.obsearch.index;

import hep.aida.bin.StaticBin1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.cache.OBCacheByteArray;
import org.ajmm.obsearch.cache.OBCacheLoaderByteArray;
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
import org.ajmm.obsearch.index.utils.StatsUtil;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.ajmm.obsearch.storage.OBStore;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreLong;
import org.ajmm.obsearch.storage.TupleBytes;
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;
import cern.colt.list.ShortArrayList;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public final class DIndexShort < O extends OBShort >
        extends
        AbstractDIndex < O, ObjectBucketShort, OBQueryShort < O >, BucketContainerShort < O > >
        implements IndexShort < O > {

    /**
     * P parameter that indicates the maximum radius that we will accept.
     */
    private short p;

    /**
     * Median data for each level and pivot.
     */
    private ArrayList < short[] > median;

    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(DIndexShort.class);

    /**
     * Cache used to store recently accessed mbr :)
     */
    protected transient OBCacheByteArray < short[][] > mbrCache;

    /**
     * Storage used to hold MBRs :)
     */
    protected transient OBStore < TupleBytes > mbrs;

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
    public DIndexShort(OBStoreFactory fact, byte pivotCount,
            IncrementalPivotSelector < O > pivotSelector, Class < O > type,
            float nextLevelThreshold, short p, int maxLevel)
            throws OBStorageException, OBException {
        super(fact, pivotCount, pivotSelector, type, nextLevelThreshold,
                maxLevel);
        this.p = p;
    }

    protected void init() throws OBStorageException, OBException {
        super.init();
        if (this.mbrCache == null) {
            mbrCache = new OBCacheByteArray < short[][] >(new MBRLoader());
        }

    }

    protected void initStorageDevices() throws OBStorageException {
        super.initStorageDevices();
        // init mbr storage.
        this.mbrs = fact.createOBStore("MBRs", false);
    }

    private class MBRLoader implements OBCacheLoaderByteArray < short[][] > {

        public int getDBSize() throws OBStorageException {
            return (int) mbrs.size();
        }

        public short[][] loadObject(byte[] id) throws DatabaseException,
                OutOfRangeException, OBException, InstantiationException,
                IllegalAccessException, IllegalIdException {
            int level = (new TupleInput(id)).readInt();
            if(level == -1){
                level = pivots.size() - 1;
            }
            byte[] data = mbrs.getValue(id);
            if (data != null) {
                TupleInput in = new TupleInput(data);
                short[][] res = new short[2][pivots.get(level).size()];
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

    @Override
    protected ObjectBucketShort getBucket(O object, int level)
            throws OBException {
        return getBucket(object, level, p);
    }

    protected ObjectBucketShort getBucket(O object, int level, short p)
            throws OBException {

        int i = 0;
        ArrayList < O > piv = super.pivots.get(level);
        short[] smapVector = new short[piv.size()];
        long bucketId = 0;
        while (i < piv.size()) {
            short distance = piv.get(i).distance(object);
            smapVector[i] = distance;
            i++;
        }
        ObjectBucketShort res = new ObjectBucketShort(bucketId, level,
                smapVector, false, -1);
        updateBucket(res, level, p);
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
    protected void updateBucket(ObjectBucketShort b, int level, short p)
            throws OBException {
        int i = 0;
        ArrayList < O > piv = super.pivots.get(level);
        short[] medians = median.get(level);
        short[] smapVector = b.getSmapVector();
        long bucketId = 0;
        boolean exclusionBucket = false;
        while (i < piv.size()) {
            short distance = smapVector[i];
            int r = bps(medians[i], distance, p);
            if (r == 1) {
                bucketId = bucketId | super.masks[i];
            } else if (r == 2) {
                exclusionBucket = true;
            }
            i++;
        }
        b.setBucket(bucketId);
        b.setExclusionBucket(exclusionBucket);
        b.setLevel(level);

    }

    @Override
    protected ObjectBucketShort getBucket(O object) throws OBException {
        int level = 0;
        ObjectBucketShort res = null;
        while (level < super.pivots.size()) {
            res = getBucket(object, level);
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
    private int bps(short median, short distance, short p) {
        if (distance <= median - p) {
            return 0;
        } else if (distance > median + p) {
            return 1;
        } else {
            return 2;
        }
    }

    protected void calculateMedians(int level, IntArrayList elementsSource)
            throws OBStorageException, IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OutOfRangeException, OBException {
        int i = 0;
        ArrayList < O > pivots = super.pivots.get(level);

        int max;
        if (elementsSource == null) {
            max = (int) A.size();
        } else {
            max = elementsSource.size();
        }
        short[] medians = new short[pivots.size()];
        logger
                .debug("Calculating medians for level: " + level + " max: "
                        + max);
        assert pivots.size() > 0;
        while (i < pivots.size()) {
            O p = pivots.get(i);
            int cx = 0;
            ShortArrayList medianData = new ShortArrayList(max);
            // calculate median for pivot p
            while (cx < max) {
                O o = getObjectFreeze(cx, elementsSource);
                medianData.add(p.distance(o));
                cx++;
            }

            medians[i] = median(medianData);
            i++;
        }
        assert i > 0;
        if (logger.isDebugEnabled()) {
            logger.debug("Found medians: " + Arrays.toString(medians));
        }
        if (median == null) {
            median = new ArrayList < short[] >();
        }
        this.median.add(medians);
        assert super.pivots.size() == median.size() : "Piv: "
                + super.pivots.size() + " Med: " + median.size();
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
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {

        OBQueryShort < O > q = null;
        queryCount++;
        int i = 0;
        ObjectBucketShort b = null;
        while (i < pivots.size()) {// search through all the levels.

            if (q != null) {
                b = getBucket(object, i, (short) (p + q.getDistance()));
                q = new OBQueryShort < O >(object, q.getDistance(), result, b
                        .getSmapVector());
            } else {
                b = getBucket(object, i, (short) (p + r));
                q = new OBQueryShort < O >(object, r, result, b.getSmapVector());
            }
            if (!b.isExclusionBucket()) {

                searchRange(b, q, i, b.getBucket());
                return;
            }
            if (q.getDistance() <= p) {
                this.updateBucket(b, i, (short) (p - q.getDistance()));

                if (!b.isExclusionBucket()) {

                    searchRange(b, q, i, b.getBucket());
                }
            } else {
                // we have to do a depth search
                // one way of doing this is to count the # of - in the string
                // (this count is c)
                // create a number n of c bits, and increment the number one by
                // one.
                // by replacing the ith bit of n in the original bit string, we
                // have all the
                // possibilities! An efficient implementation:
                // Create an array a with the indexes that must be replaced
                // (with -)
                // 1) Take each bit i of n and set the a[i] th bit of the block
                // number
                // 2) n++
                // 3) repeat 2^n - 1 times.
                // PROBLEM: we should go first for the blocks that have greater
                // possibility of having values.
                doIt2(b, q, i, 0, (long) 0);
            }
            i++;
        } // finally, search the exclusion bucket :)

        searchRange(b, q, -1, -1);
    }

    public void close() throws OBStorageException {
        this.mbrs.close();
        super.close();
    }
    
    /**
     * Search an elastic bucket.
     * @param b
     * @param q
     * @param level
     */
    private void searchRange(ObjectBucketShort b, OBQueryShort < O > q,
            int level, long bucket) throws OBStorageException,
            NotFrozenException, DatabaseException, InstantiationException,
            IllegalIdException, IllegalAccessException, OutOfRangeException,
            OBException {
        byte[] id = this.getBucketStorageIdAux(bucket, level);
        short[][] rect = this.mbrCache.get(id);
        if (rect != null && q.collides(rect)) {
            
            BucketContainerShort < O > bc = this.bucketContainerCache.get(id);
            super.dataRead += bc.getBytes().length;
            IntegerHolder smapRecords = new IntegerHolder(0);
            super.distanceComputations += bc.searchSorted(q, b, smapRecords);
            smapRecordsCompared += smapRecords.getValue();
            searchedBoxesTotal++;
            
        }
    }

    private void doIt2(ObjectBucketShort b, OBQueryShort < O > q, int level,
            int pivotIndex, long block) throws NotFrozenException,
            DatabaseException, InstantiationException, IllegalIdException,
            IllegalAccessException, OutOfRangeException, OBException {
        if (pivotIndex < pivots.get(level).size()) {
            short[] medians = median.get(level);
            int r = bps(medians[pivotIndex], b.getSmapVector()[pivotIndex],
                    (short) (q.getDistance() - p));

            if (r == 2) { // if we have to do both
                if (b.getSmapVector()[pivotIndex] > medians[pivotIndex]) {
                    // do 1 first
                    long newBlock = block | super.masks[pivotIndex];

                    doIt2(b, q, level, pivotIndex + 1, newBlock);

                    r = bps(medians[pivotIndex], b.getSmapVector()[pivotIndex],
                            (short) (q.getDistance() - p));

                    if ((r == 2 || r == 0)) {
                        doIt2(b, q, level, pivotIndex + 1, block);
                    }

                } else {
                    // 0 first

                    doIt2(b, q, level, pivotIndex + 1, block);

                    r = bps(medians[pivotIndex], b.getSmapVector()[pivotIndex],
                            (short) (q.getDistance() - p));
                    long newBlock = block | super.masks[pivotIndex];
                    if ((r == 2 || r == 1)) {

                        doIt2(b, q, level, pivotIndex + 1, newBlock);
                    }
                }

            } else { // only one of the sides is selected
                if (r == 0) {
                    doIt2(b, q, level, pivotIndex + 1, block);
                } else {
                    long newBlock = block | super.masks[pivotIndex];

                    doIt2(b, q, level, pivotIndex + 1, newBlock);

                }
            }

        } else {
            // search
            // we have finished
            this.searchRange(b, q, level, block);

        }
    }

    protected byte[] getBucketStorageIdAux(ObjectBucketShort bucket) {
        long block = bucket.getBucket();
        int level = bucket.getLevel();
        return getBucketStorageIdAux(block, level);
    }

    protected byte[] getBucketStorageIdAux(long block, int level) {
        TupleOutput out = new TupleOutput();
        out.writeInt(level);
        out.writeLong(block);
        return out.getBufferBytes();
    }

    protected byte[] getExclusionBucketId(ObjectBucketShort bucket) {
        return getBucketStorageIdAux(-1, -1);
    }

    /*
     * public void searchOB(O object, short r, OBPriorityQueueShort < O >
     * result) throws NotFrozenException, DatabaseException,
     * InstantiationException, IllegalIdException, IllegalAccessException,
     * OutOfRangeException, OBException { if(r > p){ throw new
     * UnsupportedOperationException(); } OBQueryShort < O > q = new
     * OBQueryShort < O >(object, r, result); int i = 0; ObjectBucketShort b =
     * null; super.queryCount++; while (i < pivots.size()) {// search through
     * all the levels. b = getBucket(object, i, (short) 0); assert
     * !b.isExclusionBucket(); BucketContainerShort < O > bc =
     * super.bucketContainerCache .get(super.getBucketStorageId(b));
     * super.distanceComputations += bc.search(q, b);
     * super.smapRecordsCompared+= bc.size(); super.searchedBoxesTotal++; i++; } //
     * finally, search the exclusion bucket :) BucketContainerShort < O > bc =
     * super.bucketContainerCache .get(super.exclusionBucketId);
     * super.distanceComputations += bc.search(q, b);
     * super.smapRecordsCompared+= bc.size(); super.searchedBoxesTotal++; }
     */

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
        return res.toString();
    }

    protected void putMBR(byte[] id, BucketContainerShort < O > bc)
            throws OBStorageException, IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OutOfRangeException, OBException {
       /* if(bc.getLevel() == -1){
            System.out.println("yay");
        }*/
        short[][] mbr = bc.getMBR();
        if (mbr != null) {
            TupleOutput out = new TupleOutput();
            for (short[] m : mbr) {
                for (short d : m) {
                    out.writeShort(d);
                }
            }
            
            this.mbrs.put(id, out.getBufferBytes());
            this.mbrCache.remove(id);

        } else {
            assert bc.size() == 0;
        }
    }

    public void resetStats() {
        queryCount = 0;
        searchedBoxesTotal = 0;
        super.dataRead = 0;
        smapRecordsCompared = 0;
        distanceComputations = 0;
        A.setReadStats(new StaticBin1D());
        Buckets.setReadStats(new StaticBin1D());
    }

}
