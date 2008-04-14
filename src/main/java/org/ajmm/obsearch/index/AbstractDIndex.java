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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.asserts.OBAsserts;
import org.ajmm.obsearch.cache.OBCache;
import org.ajmm.obsearch.cache.OBCacheByteArray;
import org.ajmm.obsearch.cache.OBCacheLoader;
import org.ajmm.obsearch.cache.OBCacheLoaderByteArray;
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
import org.ajmm.obsearch.index.utils.StatsUtil;
import org.ajmm.obsearch.storage.OBStore;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreInt;
import org.ajmm.obsearch.storage.OBStoreLong;
import org.ajmm.obsearch.storage.TupleBytes;
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
public abstract class AbstractDIndex < O extends OB, B extends ObjectBucket, Q, BC extends BucketContainer < O, B, Q > >
        implements Index < O > {
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractDIndex.class);

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
    protected transient ArrayList < ArrayList < O >> pivots;

    

    /**
     * We store here the pivots when we want to de-serialize/ serialize them.
     */
    private ArrayList < ArrayList < byte[] >> pivotBytes;

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
    protected transient OBStore<TupleBytes> Buckets;

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
    protected transient OBCacheByteArray < BC > bucketContainerCache;

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

    
    
    public long searchedBoxesTotal = 0;

    public long queryCount = 0;    

    public long smapRecordsCompared = 0;

    public long distanceComputations = 0;

    public long dataRead = 0;
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
    protected AbstractDIndex(OBStoreFactory fact, byte pivotCount,
            IncrementalPivotSelector < O > pivotSelector, Class < O > type,
            float nextLevelThreshold, int maxLevel) throws OBStorageException, OBException {
        this.pivotSelector = pivotSelector;
        this.type = type;
        this.fact = fact;
        this.maxLevel = maxLevel;
        init();
        this.pivotsCount = pivotCount;
        OBAsserts.chkAssert(pivotCount > 0, "Pivot count must be > 0");
        OBAsserts.chkAssert(nextLevelThreshold > 0 && nextLevelThreshold <= 1,
                "Threshold must be  > 0 && <= 1");
        this.nextLevelThreshold = nextLevelThreshold;
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
        this.Buckets = fact.createOBStore("Buckets", false);
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
            byte[] bucketId = getBucketStorageId(b);
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
            // the initial list of object from which we will generate the pivots
            IntArrayList elementsSource = null;
            // After generating the pivots, we put here the objects that fell
            // into the exclusion bucket.
            IntArrayList elementsDestination = new IntArrayList(
                    (int) A.size() / 4);
            short pivotSize = this.pivotsCount;
            int level = 0;
            int maxBucketSize = 0;
            pivots = new ArrayList < ArrayList < O >>();
            int insertedObjects = 0;
            boolean cont = true;
            int totalPivots = 0;
            do {
                // generate pivots for the current souce elements.
                // when elementsSource == null, all the elements in the DB are
                // used.
                int[] pivots = this.pivotSelector.generatePivots(pivotSize,
                        elementsSource, this);
                totalPivots += pivots.length;
                logger.debug("Pivots: " + Arrays.toString(pivots));
                logger.debug("Pivots  Length: " + pivots.length);
                
                putPivots(pivots);
                // calculate medians required to be able to use the bps
                // function.
                calculateMedians(level, elementsSource);
                int i = 0;
                int max;
                if (elementsSource == null) {
                    max = (int) A.size();
                } else {
                    max = elementsSource.size();
                }
                while (i < max) {
                    O o = getObjectFreeze(i, elementsSource);
                    B b = getBucket(o, level);
                    if (b.isExclusionBucket()) {
                        elementsDestination.add(idMap(i, elementsSource));
                    } else {
                        b.setId(idMap(i, elementsSource));
                        insertBucket(b, o);
                        insertedObjects++;
                        BC bc = this.bucketContainerCache
                                .get(getBucketStorageId(b));
                        assert bc.exists(b, o).getStatus() == Result.Status.EXISTS;
                        if (maxBucketSize < bc.size()) {
                            maxBucketSize = bc.size();
                        }
                    }
                    i++;
                }
                if (elementsSource != null
                        && elementsSource.size() == elementsDestination.size()) {
                    // we got stuck, break the loop.
                    cont = false;
                }
                logger.debug(  (float)elementsDestination.size() / (float)A.size() + "% pass to next level");
                elementsSource = elementsDestination;
                elementsDestination = new IntArrayList((int) elementsSource
                        .size() );
                pivotSize = (short) (pivotSize * this.nextLevelThreshold);
                if(pivotSize == 0){
                    pivotSize = 3;
                }
                level++;
              
                logger.debug("Max bucket size: " + maxBucketSize
                        + "exclusion: " + elementsSource.size());
            } while (elementsSource.size() > maxBucketSize
                  && level <= maxLevel   );
            logger.debug("Total pivots: " + totalPivots);
            logger.debug("Max Level: " + level);
            // we have to store the accumulated # of pivots per level (2 ^
            // accum_pivots_per_level)
            // so that we can calculate the bucket.

            
            // now we can insert the remaining objects.
            logger.debug("Inserting remaining objects");
            int i = 0;
            List<B> bulk = new LinkedList<B>();
            byte[] bucketId = null;
            while (i < elementsSource.size()) {
                O o = getObjectFreeze(i, elementsSource);
                B b = getBucket(o, level - 1);
                if(bucketId == null){
                    bucketId = this.getBucketStorageId(b);
                }
                b.setId(idMap(i, elementsSource));
                assert (b.isExclusionBucket());
                bulk.add(b);
                insertedObjects++;
                i++;
            }
            if(bucketId != null){
                // get the exclusion bucket.
                BC bc = this.bucketContainerCache.get(bucketId);
                if (bc.getBytes() == null && bulk.size() > 0) { // it was just created for the first
                    // time.
                    bc.setPivots(pivots.get(bulk.get(0).getLevel()).size());
                    bc.setLevel(bulk.get(0).getLevel());
                }
                bc.bulkInsert(bulk);
                this.putBucket(bucketId, bc);
            }
            // insert the data into the bucket.
            
            logger.debug("Bucket count: " + Buckets.size());
            assert A.size() == insertedObjects;
            // We have inserted all objects, we only have to store
            // the pivots into bytes.
            pivotBytes = new ArrayList < ArrayList < byte[] >>();
            for (ArrayList < O > pivotLevel : this.pivots) {
                ArrayList < byte[] > bytesLevel = new ArrayList < byte[] >();
                for (O pivot : pivotLevel) {
                    TupleOutput out = new TupleOutput();
                    pivot.store(out);
                    bytesLevel.add(out.getBufferBytes());
                }
                pivotBytes.add(bytesLevel);
            }
            frozen = true;
            // TODO: enable this and debug the deadlock issue.
            // this.preFreeze.deleteAll();
            Iterator<TupleBytes> it = Buckets.processAll();
            StaticBin1D s = new StaticBin1D();
            while(it.hasNext()){
                TupleBytes t = it.next();
                BC bc = this.bucketContainerCache.get(t.getKey());
                s.add(bc.size());
            } // add exlucion
            
            logger.debug(StatsUtil.mightyIOStats("Bucket distribution", s));
            
        } catch (PivotsUnavailableException e) {
            throw new OBException(e);
        }
    }

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
    protected abstract void calculateMedians(int level,
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
        byte[] bucketId = getBucketStorageId(b);
        // if the bucket is the exclusion bucket
        // get the bucket container from the cache.
        BC bc = this.bucketContainerCache.get(bucketId);
        if (bc.getBytes() == null) { // it was just created for the first
                                        // time.
            bc.setPivots(pivots.get(b.getLevel()).size());
            bc.setLevel(b.getLevel());
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
     * @param bucket
     */
    private void putBucket(byte[]bucketId, BC bc) throws OBStorageException,
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
    protected abstract void putMBR(byte[] id, BC bc) throws OBStorageException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    DatabaseException, OutOfRangeException, OBException;

    private void putPivots(int[] pivotIds) throws IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OutOfRangeException, OBException {
        ArrayList < O > level = new ArrayList < O >(pivotIds.length);
        for (int i : pivotIds) {
            O p = getObject(i);

            level.add(p);
        }
        this.pivots.add(level);
        
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
     * Returns the bucket information for the given object and a certain level.
     * @param object
     *                The object that will be calculated
     * @param level
     *                The level to calculate.
     * @return The bucket information for the given object.
     */
    protected abstract B getBucket(O object, int level) throws OBException;

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
    protected byte[] getBucketStorageId(B bucket) {
        if (bucket.isExclusionBucket()) {
            return getExclusionBucketId(bucket);
        } else {
            return getBucketStorageIdAux(bucket);
        }
    }
    
    /**
     * Returns the key of the given bucket.
     * @param bucket Bucket to process.
     * @return key of the bucket in bytes.
     */
    protected abstract byte[] getBucketStorageIdAux(B bucket) ;
    
    /**
     * Returns the exclusion bucket id.
     * @return Returns the exclusion bucket id.
     */
    protected abstract byte[] getExclusionBucketId(B bucket) ;
    

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
            this.bucketContainerCache = new OBCacheByteArray < BC >(
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

    private class BucketLoader implements OBCacheLoaderByteArray < BC > {

        public int getDBSize() throws OBStorageException {
            return (int) Buckets.size();
        }

        public BC loadObject(byte[] i) throws DatabaseException,
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
