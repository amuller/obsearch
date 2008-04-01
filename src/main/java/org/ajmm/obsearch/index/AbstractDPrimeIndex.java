package org.ajmm.obsearch.index;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

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

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.asserts.OBAsserts;
import org.ajmm.obsearch.cache.OBCache;
import org.ajmm.obsearch.cache.OBCacheLoader;
import org.ajmm.obsearch.cache.OBCacheLoaderLong;
import org.ajmm.obsearch.cache.OBCacheLong;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.d.BucketContainer;
import org.ajmm.obsearch.index.d.ObjectBucket;
import org.ajmm.obsearch.index.d.SimpleBloomFilter;
import org.ajmm.obsearch.index.utils.StatsUtil;
import org.ajmm.obsearch.storage.OBStore;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreInt;
import org.ajmm.obsearch.storage.OBStoreLong;
import org.ajmm.obsearch.storage.TupleLong;
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

/**
 * AbstractDIndex contains functionality common to specializations of the
 * D-Index for primitive types.
 * @param <
 *                O > The OB object that will be stored.
 * @param <
 *                B > The Object bucket that will be used.
 * @param < Q >
 *                The query object type.
 * @param <
 *                BC> The Bucket container.
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractDPrimeIndex < O extends OB, B extends ObjectBucket, Q, BC extends BucketContainer < O, B, Q > >
        implements Index < O > {
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractDPrimeIndex.class);

    /**
     * The pivot selector used by the index.
     */
    private IncrementalPivotSelector < O > pivotSelector;

    /**
     * The type used to instantiate objects of type O.
     */
    private Class < O > type;

    /**
     * Tells if this index is frozen or not.
     */
    private boolean frozen = false;

    /**
     * Pivots will be stored here for quick access.
     */
    protected transient ArrayList < O > pivots;

    /**
     * Filter used to avoid unnecessary block accesses.
     */
   //protected ArrayList< SimpleBloomFilter<Long>> filter;
   protected ArrayList< HashSet<Long>> filter;

    /**
     * We store here the pivots when we want to de-serialize/ serialize them.
     */
    private ArrayList < byte[] > pivotBytes;

    /**
     * Factory used by this class and by subclasses to create appropiate storage
     * devices.
     */
    protected transient OBStoreFactory fact;

    // TODO: We have to move to longs for object ids.
    /**
     * Objects are stored by their id's here.
     */
    protected transient OBStoreInt A;

    /**
     * We store the buckets in this storage device.
     */
    protected transient OBStoreLong Buckets;

    /**
     * Required during pre-freeze to make only one copy of an object is
     * inserted. (the index is not built at this stage therefore it is not
     * possible to know if an object is already in the DB.
     */
    protected transient OBStore preFreeze;

    /**
     * # of pivots to be used.
     */
    protected byte pivotsCount;

    /**
     * Cache used for storing recently accessed objects O.
     */
    private transient OBCache < O > aCache;

    /**
     * Cache used for storing recently accessed Buckets.
     */
    protected transient OBCacheLong < BC > bucketContainerCache;

    /**
     * How many pivots will be used on each level of the hash table.
     */
    private float nextLevelThreshold;

    /**
     * Masks used to speedup the generation of hash table codes.
     */
    protected long[] masks;

    /**
     * Max level of buckets to be allowed.
     */
    private int maxLevel = 20;

    /**
     * Accumulated pivots per level (2 ^ pivots per level) so that we can
     * quickly compute the correct storage device
     */
    private long[] accum;
    
    /**
     * All the statistics values are kept here so that we will erase them at
     * some point in the future. This is ugly but it is the only way that we
     * will remove all the code related to stats to squeeze the last ounce of
     * performance out of OBSearch :)
     */
    public long searchedBoxesTotal = 0;

    public long queryCount = 0;    

    public long smapRecordsCompared = 0;

    public long distanceComputations = 0;
    
    // boxes avoided by using the MBR optimization.
    public long avoidedBoxesWithMBR = 0;
    

    /**
     * Initializes this abstract class.
     * @param fact
     *                The factory that will be used to create storage devices.
     * @param pivotCount #
     *                of Pivots that will be used.
     * @param pivotSelector
     *                The pivot selection mechanism to be used. (64 pivots can
     *                create 2^64 different buckets, so we have limited the # of
     *                pivots available. If you need more pivots, then you would
     *                have to use the Smapped P+Tree (PPTree*).
     * @param type
     *                The type that will be stored in this index.
     * @param nextLevelThreshold
     *                How many pivots will be used on each level of the hash
     *                table.
     * @throws OBStorageException
     *                 if a storage device could not be created for storing
     *                 objects.
     */
    protected AbstractDPrimeIndex(OBStoreFactory fact, byte pivotCount,
            IncrementalPivotSelector < O > pivotSelector, Class < O > type)
            throws OBStorageException, OBException {
        this.pivotSelector = pivotSelector;
        this.type = type;
        this.fact = fact;
        init();
        this.pivotsCount = pivotCount;
        OBAsserts.chkAssert(pivotCount > 0, "Pivot count must be > 0");

    }

    protected void init() throws OBStorageException, OBException {
        initStorageDevices();
        initCache();
        // initialize the masks;
        int i = 0;
        long mask = 1;
        this.masks = new long[64];
        while (i < 64) {
            logger.debug(Long.toBinaryString(mask));
            masks[i] = mask;
            mask = mask << 1;
            i++;
        }
   
    }
    
   


    /**
     * Initializes storage devices required by this class.
     * @throws OBStorageException
     *                 If the storage device could not be created.
     */
    protected void initStorageDevices() throws OBStorageException {
        this.A = fact.createOBStoreInt("A", false);
        this.Buckets = fact.createOBStoreLong("Buckets", false);
        if (!this.isFrozen()) {
            this.preFreeze = fact.createOBStore("pre", true);
        }
    }

    /**
     * Subclasses must call this method after they have closed the storage
     * devices they created.
     */
    public void close() throws OBStorageException {
        A.close();
        this.Buckets.close();
        if (this.preFreeze != null) {
            preFreeze.close();
        }
        fact.close();
        
    }

    @Override
    public int databaseSize() throws OBStorageException {
        // TODO: fix this, change the interface so that we support long number
        // of objects.
        return (int) A.size();
    }

    @Override
    public Result delete(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException, NotFrozenException {

        if (this.isFrozen()) {
            B b = getBucket(object);
            long bucketId = getBucketStorageId(b);
            BC bc = this.bucketContainerCache.get(bucketId);
            Result res = bc.delete(b, object);
            if (res.getStatus() == Result.Status.OK) {
                // update the bucket
                // container.
                putBucket(bucketId, bc);
                this.A.delete(res.getId());
            }
            return res;
        } else {
            throw new NotFrozenException();
        }

    }

    @Override
    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException {
        // get n pivots.
        // ask the bucket for each object and insert those who are not excluded.
        // repeat iteratively with the objects that could not be inserted until
        // the remaining
        // objects are small enough.
        
        try {
            
            // initialize bloom filter
            int i = 0;
            //this.filter = new ArrayList<SimpleBloomFilter<Long>>();
            this.filter = new ArrayList<HashSet<Long>>();
            
            while(i < pivotsCount){
                //filter.add(new SimpleBloomFilter<Long>(i * 1000, (int)Math.pow(2, i)));
                filter.add(new HashSet<Long>());
                i++;
            }
            
            // the initial list of object from which we will generate the pivots
            IntArrayList elementsSource = null;
            // After generating the pivots, we put here the objects that fell
            // into the exclusion bucket.
            
            short pivotSize = this.pivotsCount;
            int maxBucketSize = 0;
            pivots = new ArrayList < O >();
            int insertedObjects = 0;
            boolean cont = true;

            // generate pivots for the current souce elements.
            // when elementsSource == null, all the elements in the DB are
            // used.
            int[] pivots = this.pivotSelector.generatePivots(pivotSize,
                    elementsSource, this);
            logger.debug("Pivots: " + Arrays.toString(pivots));
            putPivots(pivots);
            // calculate medians required to be able to use the bps
            // function.
            calculateMedians(elementsSource);
            i = 0;
            int max;
            if (elementsSource == null) {
                max = (int) A.size();
            } else {
                max = elementsSource.size();
            }
            while (i < max) {
                O o = getObjectFreeze(i, elementsSource);
                B b = getBucket(o);
                updateProbabilities(b);
                b.setId(idMap(i, elementsSource));
                insertBucket(b, o);
                insertedObjects++;
                BC bc = this.bucketContainerCache.get(getBucketStorageId(b));
                assert bc.exists(b, o).getStatus() == Result.Status.EXISTS;
                if (maxBucketSize < bc.size()) {
                    maxBucketSize = bc.size();
                }

                i++;
            }
            normalizeProbs();
            logger.debug("Max bucket size: " + maxBucketSize);
            logger.debug("Bucket count: " + A.size());
            // We have inserted all objects, we only have to store
            // the pivots into bytes.
            pivotBytes = new ArrayList < byte[] >();
            for (O pivot : this.pivots) {

                TupleOutput out = new TupleOutput();
                pivot.store(out);
                pivotBytes.add(out.getBufferBytes());
            }
            
            Iterator<TupleLong> it = Buckets.processRange(Long.MIN_VALUE, Long.MAX_VALUE);
            StaticBin1D s = new StaticBin1D();
            while(it.hasNext()){
                TupleLong t = it.next();
                BC bc = this.bucketContainerCache.get(t.getKey());
                s.add(bc.size());
            } // add exlucion
            logger.debug(StatsUtil.mightyIOStats("Bucket distribution", s));
            
            frozen = true;
            // TODO: enable this and debug the deadlock issue.
            // this.preFreeze.deleteAll();
        } catch (PivotsUnavailableException e) {
            throw new OBException(e);
        }
    }
    
    /**
     * 
     * Updates probability information.
     * @param b
     */
    protected abstract void updateProbabilities(B b);
    
    protected abstract void normalizeProbs() throws OBStorageException;

    /**
     * Calculate median values for pivots of level i based on the
     * elementsSource. If elementsSource == null, all the objects in the DB are
     * used.
     * @param level
     *                The level that will be processed.
     * @param elementsSource
     *                The objects that will be processed to generate median data
     *                information.
     * @throws OBStorageException
     *                 if something goes wrong with the storage device.
     */
    protected abstract void calculateMedians(
            IntArrayList elementsSource) throws OBStorageException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException;

    /**
     * Stores the given bucket b into the {@link #Buckets} storage device. The
     * given bucket b should have been returned by {@link #getBucket(OB, int)}
     * @param b
     *                The bucket in which we will insert the object.
     * @param object
     *                The object to insert.
     * @return A result object with the new id of the object if the object was
     *         inserted successfully.
     * @throws OBStorageException
     */
    private Result insertBucket(B b, O object) throws OBStorageException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException {
        // get the bucket id.
        long bucketId = getBucketStorageId(b);
        // if the bucket is the exclusion bucket
        // get the bucket container from the cache.
        BC bc = this.bucketContainerCache.get(bucketId);
        if (bc.getBytes() == null) { // it was just created for the first
            // time.
            bc.setPivots(pivots.size());
            bc.setLevel(b.getLevel());
            updateFilters(bucketId);
        } else {
            assert bc.getPivots() == b.getPivotSize() : " Pivot size: "
                    + bc.getPivots() + " b pivot size: " + b.getPivotSize();
            assert bc.getLevel() == b.getLevel();
        }
        Result res = new Result();
        synchronized (bc) {
            res = bc.exists(b, object);
            if (res.getStatus() != Result.Status.EXISTS) {

                assert bc.getPivots() == b.getPivotSize() : "BC: "
                        + bc.getPivots() + " b: " + b.getPivotSize();
                bc.insert(b);
               putBucket(bucketId, bc);
                res.setStatus(Result.Status.OK);
            }
        }
        return res;
    }
    
    /**
     * 
     * @param bucket
     */
    private void putBucket(long bucketId, BC bc) throws OBStorageException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    DatabaseException, OutOfRangeException, OBException{
        
        Buckets.put(bucketId, bc.getBytes());
        // we have to update the MBRs
        putMBR(bucketId, bc);
    }
    
    /**
     * Puts the MBR of the container in the DB.
     * @param bc
     */
    protected abstract void putMBR(long id, BC bc) throws OBStorageException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    DatabaseException, OutOfRangeException, OBException;
        
    
    
    /** updates the filters! **/
    private void updateFilters(long x){
        String s = Long.toBinaryString(x);
        int max = s.length();
        int i = s.length() - 1;
        int cx = 0;
        while(i >= 0){
            long j  = Long.parseLong(s.substring(i , max), 2);
            //if(! filter.get(cx).contains(j)){
                filter.get(cx).add(j);
            //}            
            i--;
            cx++;
        }
        // add the long to the rest of the layers.
        while(max < pivotsCount){
            filter.get(max).add(x);
            max++;
        }
    }

    private void putPivots(int[] pivotIds) throws IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OutOfRangeException, OBException {
        ArrayList < O > level = new ArrayList < O >(pivotIds.length);
        for (int i : pivotIds) {
            O p = getObject(i);

            pivots.add(p);
        }

    }

    /**
     * If elementSource == null returns id, otherwise it returns
     * elementSource[id]
     * @return
     */
    private int idMap(int id, IntArrayList elementSource) {
        if (elementSource == null) {
            return id;
        } else {
            return elementSource.get(id);
        }
    }

    /**
     * Auxiliary function used in freeze to get objects directly from the DB, or
     * by using an array of object ids.
     */
    protected O getObjectFreeze(int id, IntArrayList elementSource)
            throws IllegalIdException, IllegalAccessException,
            InstantiationException, DatabaseException, OutOfRangeException,
            OBException {

        return getObject(idMap(id, elementSource));

    }

   

    /**
     * Returns the bucket information for the given object.
     * @param object
     *                The object that will be calculated
     * @return The bucket information for the given object.
     */
    protected abstract B getBucket(O object) throws OBException;

    @Override
    public int getBox(O object) throws OBException {
        // Calculate the bucket of the given object.
        return 0;
    }

    @Override
    public O getObject(int id) throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException, OBException {
        // get the object from A, this is easy.
        return aCache.get(id);
    }

    @Override
    public Result insert(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {

        Result res = new Result();
        res.setStatus(Result.Status.OK);
        if (this.isFrozen()) {
            res = exists(object);
            if (res.getStatus() == Result.Status.NOT_EXISTS) {
                TupleOutput out = new TupleOutput();
                object.store(out);
                int id = (int) A.nextId();
                this.A.put(id, out.getBufferBytes());
                B b = getBucket(object);
                b.setId(id);
                res = this.insertBucket(b, object);
                res.setId(id);
            }
        } else { // before freeze
            TupleOutput out = new TupleOutput();
            object.store(out);
            byte[] key = out.getBufferBytes();
            byte[] value = this.preFreeze.getValue(key);
            if (value == null) {
                int id = (int) A.nextId();
                res.setId(id);
                TupleOutput outId = new TupleOutput();
                outId.writeInt(id);
                preFreeze.put(key, outId.getBufferBytes());
            } else {
                res.setStatus(Result.Status.EXISTS);
                TupleInput in = new TupleInput(value);
                res.setId(in.readInt());
            }
            if (res.getStatus() == Result.Status.OK) {
                out = new TupleOutput();
                object.store(out);
                this.A.put(res.getId(), out.getBufferBytes());
            }
        }

        return res;
    }

    /**
     * Returns the bucket's storage id code
     * @param bucket
     *                The bucket that will be processed.
     * @return The exact location in the storage system for the given bucket.
     */
    protected long getBucketStorageId(B bucket) {
        return bucket.getBucket();
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public O readObject(TupleInput in) throws InstantiationException,
            IllegalAccessException, OBException {
        O res = type.newInstance();
        res.load(in);
        return res;
    }

    @Override
    public void relocateInitialize(File dbPath) throws DatabaseException,
            NotFrozenException, IllegalAccessException, InstantiationException,
            OBException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public String toXML() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int totalBoxes() {
        // TODO Auto-generated method stub
        return 0;
    }

    /**
     * Initializes the object cache {@link #cache}.
     * @throws DatabaseException
     *                 If something goes wrong with the DB.
     */
    protected void initCache() throws OBException {
        if (aCache == null) {
            aCache = new OBCache < O >(new ALoader());
        }
        if (this.bucketContainerCache == null) {
            this.bucketContainerCache = new OBCacheLong < BC >(
                    new BucketLoader());
        }
    }

    private class ALoader implements OBCacheLoader < O > {

        public int getDBSize() throws OBStorageException {
            return (int) A.size();
        }

        public O loadObject(int i) throws DatabaseException,
                OutOfRangeException, OBException, InstantiationException,
                IllegalAccessException, IllegalIdException {
            O res = type.newInstance();
            byte[] data = A.getValue(i);
            if (data == null) {
                throw new IllegalIdException(i);
            }
            TupleInput in = new TupleInput(data);
            res.load(in);
            return res;
        }

    }

    /**
     * Get a bucket container fromt he given data.
     * @param data
     *                The data from which the bucket container will be loaded.
     * @return A new bucket container ready to be used.
     */
    protected abstract BC instantiateBucketContainer(byte[] data);

    private class BucketLoader implements OBCacheLoaderLong < BC > {

        public int getDBSize() throws OBStorageException {
            return (int) Buckets.size();
        }

        public BC loadObject(long i) throws DatabaseException,
                OutOfRangeException, OBException, InstantiationException,
                IllegalAccessException {
            byte[] data = Buckets.getValue(i);

            return instantiateBucketContainer(data);

        }

    }

    /**
     * @return the maxLevel
     */
    public int getMaxLevel() {
        return maxLevel;
    }

    /**
     * @param maxLevel
     *                the maxLevel to set
     */
    public void setMaxLevel(int maxLevel) {
        this.maxLevel = maxLevel;
    }

}
