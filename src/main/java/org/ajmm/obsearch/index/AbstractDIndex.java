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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.asserts.OBAsserts;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.PivotsUnavailableException;
import org.ajmm.obsearch.index.d.BucketContainer;
import org.ajmm.obsearch.index.d.ObjectBucket;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreInt;
import org.ajmm.obsearch.storage.OBStoreLong;
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
 * @param < Q > The query object type.
 * @param < BC> The Bucket container.
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractDIndex < O extends OB, B extends ObjectBucket, Q, BC extends BucketContainer<O,B,Q> >
        implements Index < O > {
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
    .getLogger(AbstractDIndex.class);

    /**
     * The pivot selector used by the index.
     */
    private IncrementalPivotSelector pivotSelector;

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
     * This is the id for the exclusion bucket. (to avoid computing 2^m^h all
     * the time)
     */
    protected long exclusionBucketId;

    /**
     * We store here the pivots when we want to de-serialize/ serialize them.
     */
    private ArrayList < ArrayList < byte[] >> pivotBytes;

    /**
     * Factory used by this class and by subclasses to create appropiate storage
     * devices.
     */
    protected transient OBStoreFactory fact;

    // TODO: We have to quickly move to longs.
    /**
     * Objects are stored by their id's here.
     */
    protected transient OBStoreInt A;

    /**
     * We store the buckets in this storage device.
     */
    protected transient OBStoreLong Buckets;

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
    private transient OBCacheLong < BC > bucketContainerCache;

    /**
     * How many pivots will be used on each level of the hash table.
     */
    private float nextLevelThreshold;
    
    /**
     * Masks used to speedup the generation of hash table codes.
     */
    protected long[] masks;

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
            IncrementalPivotSelector<O> pivotSelector, Class < O > type,
            float nextLevelThreshold) throws OBStorageException, OBException {
        this.pivotSelector = pivotSelector;
        this.type = type;
        init();
        this.pivotsCount = pivotCount;
        OBAsserts.chkAssert(pivotCount > 0, "Pivot count must be > 0");
        OBAsserts.chkAssert(nextLevelThreshold > 0 && nextLevelThreshold <= 0,
                "Threshold must be  > 0 && <= 1");
        this.nextLevelThreshold = nextLevelThreshold;
    }

    private void init() throws OBStorageException{
        initStorageDevices();
        // initialize the masks;
        int i = 0;
        int mask = 1;
        this.masks = new long[64];
        while(i < 64){
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
        initSpecializedStorageDevices();
    }

    /**
     * This method will be called by {@link #initSpecializedStorageDevices()} to
     * initialize other storage devices required by the subclasses. If your
     * subclass is not an immediate subclass of
     * {@link #AbstractDIndex(OBStoreFactory, short, PivotSelector, Class)},
     * then you should always call super.
     * {@link #initSpecializedStorageDevices()} after initializing your device.
     * @throws OBStorageException
     *                 If the storage device could not be created.
     */
    protected abstract void initSpecializedStorageDevices()
            throws OBStorageException;

    /**
     * Subclasses must call this method after they have closed the storage
     * devices they created.
     */
    public void close() throws OBStorageException {
        A.close();
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
            if(res.getStatus() == Result.Status.EXISTS){
                this.Buckets.put(bucketId, bc.getBytes()); // update the bucket container.
                this.A.delete(res.getId());
            }
            return res;
        }
        else{        
            throw new NotFrozenException();
        }

    }

    @Override
    public Result exists(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        // get the bucket, and check if the object is there.
        return null;
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
            do {
                // generate pivots for the current souce elements.
                // when elementsSource == null, all the elements in the DB are
                // used.
                int[] pivots = this.pivotSelector.generatePivots(pivotSize,
                        elementsSource, this);
                putPivots(pivots);
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
                        insertBucket(b, o);
                    }
                    i++;
                }
                elementsSource = elementsDestination;
                elementsDestination = new IntArrayList((int) elementsSource
                        .size() / 4);
                pivotSize = (short) (pivotSize * this.nextLevelThreshold);
                level++;
            } while (elementsDestination.size() <= maxBucketSize); // size of
            // excluded
            // elements
            // is small
            // enough.
            // now we just have to store the bucket id for the exclusion bucket,
            // and store those objects in the the exclusion bucket.
            double id = level * (Math.pow(2, pivotSize));
            OBAsserts.chkAssert(id <= Long.MAX_VALUE,
                    "Exceeded bucket id addressing at level: " + (level - 1)
                            + ", aborting");
            this.exclusionBucketId = Math.round(id);
            // now we can insert the remaining objects.
            int i = 0;
            while (i < elementsDestination.size()) {
                O o = getObjectFreeze(i, elementsDestination);
                B b = getBucket(o, level - 1);
                assert (b.isExclusionBucket());
                insertBucket(b, o);
                i++;
            }
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
        } catch (PivotsUnavailableException e) {
            throw new OBException(e);
        }
    }

    /**
     * Stores the given bucket b into the {@link #Buckets} storage device. The
     * given bucket b should have been returned by {@link #getBucket(OB, int)}
     * @param b
     *                The bucket in which we will insert the object.
     * @param object
     *                The object to insert.
     * @return A result object with the new id of the object if the object
     *                was inserted successfully.
     * @throws OBStorageException
     */
    private Result insertBucket(B b, O object) throws OBStorageException, IllegalIdException,
    IllegalAccessException, InstantiationException, DatabaseException,
    OutOfRangeException, OBException {
        // get the bucket id.
        long bucketId = getBucketStorageId(b);
        // if the bucket is the exclusion bucket
        // get the bucket container from the cache.
        BC bc = this.bucketContainerCache.get(bucketId);
        Result res = new Result();        
        synchronized (bc) {
            res = bc.exists(b, object);
            if(res.getStatus() != Result.Status.EXISTS){
                int newId = (int)this.Buckets.nextId();
                b.setId(newId);
                bc.insert(b);
                Buckets.put(bucketId, bc.getBytes());
                res.setStatus(Result.Status.OK);
                res.setId(newId);
            }                            
        }
        return res;
    }

    private void putPivots(int[] pivotIds) throws IllegalIdException,
            IllegalAccessException, InstantiationException, DatabaseException,
            OutOfRangeException, OBException {
        ArrayList < O > level = new ArrayList < O >(pivotIds.length);
        for (int i : pivotIds) {
            level.add(getObject(i));
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
    private O getObjectFreeze(int id, IntArrayList elementSource)
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
    protected abstract B getBucket(O object, int level);
    
    /**
     * Returns the bucket information for the given object.
     * @param object
     *                The object that will be calculated
     * @return The bucket information for the given object.
     */
    protected abstract B getBucket(O object);

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
        // need an object bucket that will always be sorted, and that will find
        // efficiently objects based on the first pivot.
        // TODO: fix this!!!
        Result res = new Result();
        res.setStatus(Result.Status.OK);
        if (this.isFrozen()) {
            B b = getBucket(object);            
            res = this.insertBucket(b, object);
        }
        else{        
            res.setId( (int) A.nextId());
        }
        if(res.getStatus() == Result.Status.OK){
            TupleOutput out = new TupleOutput();
            object.store(out);
            this.A.put(res.getId(), out.getBufferBytes());            
        }
        return res;
    }
    
    /**
     * Returns the bucket's storage id code
     * @param bucket The bucket that will be processed.
     * @return The exact location in the storage system for the given bucket.
     */
    private long getBucketStorageId(B bucket){
        if(bucket.isExclusionBucket()){
            return this.exclusionBucketId;
        }else
        {
            return bucket.getStorageBucket();
        }
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
    }

    private class ALoader implements OBCacheLoader < O > {

        public int getDBSize() throws OBStorageException {
            return (int) A.size();
        }

        public O loadObject(int i) throws DatabaseException,
                OutOfRangeException, OBException, InstantiationException,
                IllegalAccessException {
            O res = type.newInstance();
            TupleInput in = new TupleInput(A.getValue(i));
            res.load(in);
            return res;
        }

    }

}
