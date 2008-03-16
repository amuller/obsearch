package org.ajmm.obsearch.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.d.BucketContainer;
import org.ajmm.obsearch.index.d.BucketContainerShort;
import org.ajmm.obsearch.index.d.ObjectBucket;
import org.ajmm.obsearch.index.d.ObjectBucketShort;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;
import cern.colt.list.ShortArrayList;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public final class DIndexShort < O extends OBShort >
        extends
        AbstractDIndex < O, ObjectBucketShort, OBQueryShort < O >, BucketContainerShort < O > > 
implements IndexShort<O>{

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
     * Creates a new DIndex for shorts
     * @param fact Storage factory to use
     * @param pivotCount number of pivots to use.
     * @param pivotSelector Pivot acceptance criteria.
     * @param type The type of objects to use (needed to create new instances)
     * @param nextLevelThreshold threshold used to reduce the number of pivots per level.
     * @param p P parameter of  D-Index.
     * @throws OBStorageException If something goes wrong with the storage device.
     * @throws OBException If some other exception occurs.
     */
    public DIndexShort(OBStoreFactory fact, byte pivotCount,
            IncrementalPivotSelector < O > pivotSelector, Class < O > type,
            float nextLevelThreshold, short p) throws OBStorageException, OBException {
        super(fact, pivotCount, pivotSelector, type, nextLevelThreshold);
        this.p = p;
    }

    @Override
    protected ObjectBucketShort getBucket(O object, int level) throws OBException{
        return getBucket(object, level, p);
    }
   
    protected ObjectBucketShort getBucket(O object, int level, short p) throws OBException{
        
        int i = 0;
        ArrayList < O > piv = super.pivots.get(level);
        short[] smapVector = new short[piv.size()]; 
        long bucketId = 0;
        while (i < piv.size()) {
            short distance = piv.get(i).distance(object);
            smapVector[i] = distance;          
            i++;
        }
        ObjectBucketShort res =  new ObjectBucketShort(bucketId,level, smapVector, false, -1);
        updateBucket(res, level, p);
        return res;
    }
    
    
    
    /**
     * Calculate a new bucket based on the smap vector of the given b
     * Warning, this method destroys the previously available info in the given bucket b.
     * It keeps the smap vector intact.
     * @param b We will take the smap vector from here.
     * @param level Level of the hash table
     * @param p P value to use
     * @throws OBException
     */
    protected void updateBucket (ObjectBucketShort b, int level, short p) throws OBException{
        int i = 0;
        ArrayList < O > piv = super.pivots.get(level);
        short[] medians = median.get(level);
        short[] smapVector = b.getSmapVector();
        long bucketId = 0;
        boolean exclusionBucket = false;
        while (i < piv.size()) {
            short distance = smapVector[i];
            int r = bps(medians[i], distance, p);
            if(r == 1){
                bucketId = bucketId | super.masks[i];
            }else if(r == 2){
                exclusionBucket = true;
            }
            i++;
        }
        b.setBucket(bucketId);
        b.setExclusionBucket(exclusionBucket);
        b.setLevel(level);
        
    }

    @Override
    protected ObjectBucketShort getBucket(O object) throws OBException{
        int level = 0;
        ObjectBucketShort res = null;
        while(level < super.pivots.size()){
            res = getBucket(object, level);
            if(!res.isExclusionBucket()){         
                break;
            }                                
            level++;
        }
        assert res != null;
        return res;
    }

    /**
     * Bps function. Returns 0 if d(o,p) <= median - p . Returns 1 if d(o,p) > median +p. Returns 2 otherwise. 
     * @param median Median obtained for the given pivot.
     * @param distance Distance of the pivot and the object we are processing
     * @return Returns 0 if d(o,p) <= median - p . Returns 1 if d(o,p) > median +p. Returns 2 otherwise. 
     */
    private int bps(short median, short distance, short p ){
        if(distance <= median - p){
            return 0;
        }else if(distance > median + p){
            return 1;
        }else{
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
        logger.debug("Calculating medians for level: " + level + " max: " + max);
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
        if(logger.isDebugEnabled()){
            logger.debug("Found medians: " + Arrays.toString(medians));
        }
        if(median == null){
            median = new ArrayList<short[]>();
        }
        this.median.add(medians);
        assert super.pivots.size() == median.size(): "Piv: " + super.pivots.size() + " Med: " + median.size();
    }

    private short median(ShortArrayList medianData) {
        medianData.sort();
        return medianData.get(medianData.size() / 2);
    }

    protected BucketContainerShort < O > instantiateBucketContainer(byte []  data){
                   return new BucketContainerShort < O >(this, data);
           }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#intersectingBoxes(org.ajmm.obsearch.ob.OBShort, short)
     */
    @Override
    public int[] intersectingBoxes(O object, short r)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        throw new UnsupportedOperationException();
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#intersects(org.ajmm.obsearch.ob.OBShort, short, long)
     */
    @Override
    public boolean intersects(O object, short r, int box)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        throw new UnsupportedOperationException();
        
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#searchOB(org.ajmm.obsearch.ob.OBShort, short, org.ajmm.obsearch.result.OBPriorityQueueShort, long[])
     */
    @Override
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result,
            int[] boxes) throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        throw new UnsupportedOperationException();        
        
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#searchOB(org.ajmm.obsearch.ob.OBShort, short, org.ajmm.obsearch.result.OBPriorityQueueShort)
     */
    @Override
    public void searchOB(O object, short r, OBPriorityQueueShort < O > result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        OBQueryShort<O> q = new OBQueryShort<O>(object,r, result);
        int i = 0;
        ObjectBucketShort  b = null;
        while(i < pivots.size()){// search through all the levels.
            b =  getBucket(object, i, (short)(p + r));
            if(! b.isExclusionBucket()){
                BucketContainerShort<O> bc = super.bucketContainerCache.get(super.getBucketStorageId(b));
                bc.search(q,b);
                return;
            }
            if(r <= p){
                this.updateBucket(b, i, (short)(p - r));
                if(! b.isExclusionBucket()){
                    BucketContainerShort<O> bc = super.bucketContainerCache.get(super.getBucketStorageId(b));
                    bc.search(q, b);
                }
            }else{
                throw new UnsupportedOperationException("Only supporting ranges < p");
            }
            i++;
        }
        // finally, search the exclusion bucket :)
        BucketContainerShort<O> bc = super.bucketContainerCache.get(super.exclusionBucketId);
        bc.search(q, b);
    }

   
    @Override
    public Result exists(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        OBPriorityQueueShort < O > result = new OBPriorityQueueShort < O >((byte)1);
        searchOB(object, (short)0, result);
        Result res = new Result();
        res.setStatus(Result.Status.NOT_EXISTS);
        if(result.getSize() ==1){
            Iterator<OBResultShort<O>> it = result.iterator();
            OBResultShort<O> r = it.next();
            if(r.getObject().equals(object)){
                res.setId(r.getId());
                res.setStatus(Result.Status.EXISTS);
            }
        }
        return  res;
    }
    

    
}
