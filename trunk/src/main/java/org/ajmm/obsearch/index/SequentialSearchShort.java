package org.ajmm.obsearch.index;

import java.util.Iterator;

import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.TupleInt;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public class SequentialSearchShort<O extends OBShort>
        extends AbstractSequentialSearch<O> implements IndexShort<O> {
    
    protected long[] distanceDistribution = new long[Short.MAX_VALUE + 1];
    
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(SequentialSearchShort.class);
    
    public SequentialSearchShort(OBStoreFactory fact, Class < O > type) throws OBStorageException, OBException {
       super(fact,type);
    }
    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#intersectingBoxes(org.ajmm.obsearch.ob.OBShort, short)
     */
    @Override
    public int[] intersectingBoxes(OBShort object, short r)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#intersects(org.ajmm.obsearch.ob.OBShort, short, int)
     */
    @Override
    public boolean intersects(OBShort object, short r, int box)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#searchOB(org.ajmm.obsearch.ob.OBShort, short, org.ajmm.obsearch.result.OBPriorityQueueShort, int[])
     */
    @Override
    public void searchOB(O object, short r, OBPriorityQueueShort<O> result,
            int[] boxes) throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {
        // TODO Auto-generated method stub
        
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.index.IndexShort#searchOB(org.ajmm.obsearch.ob.OBShort, short, org.ajmm.obsearch.result.OBPriorityQueueShort)
     */
    @Override
    public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
            throws NotFrozenException, DatabaseException,
            InstantiationException, IllegalIdException, IllegalAccessException,
            OutOfRangeException, OBException {

        long top = A.size();
        Iterator<TupleInt > it = A.processAll();
        while(it.hasNext()){
            TupleInt t = it.next();
            int i = t.getKey();
          
            O other = super.aCache.get(i);
            super.distanceComputations++;
            short distance = other.distance(object);
            distanceDistribution[distance]++;
            if(distance <= r){
                result.add(i, other, distance);
            }
            r = result.updateRange(r);
            
        }

            

    }

    public String getStats(){
        StringBuilder res = new StringBuilder(super.getStats());
        res.append("\n");
        res.append("Distance Distribution");
        res.append("\n");
        int i = 0;
        while(i < distanceDistribution.length){
            if(distanceDistribution[i] != 0){
                res.append(i + " " + distanceDistribution[i] );
                res.append("\n");
            }
            i++;
        }
        return res.toString();
    }
    
    public void resetStats(){
        super.resetStats();
        distanceDistribution = new long[Short.MAX_VALUE + 1];
    }

}