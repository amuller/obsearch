package org.ajmm.obsearch.index.d;

import java.util.ArrayList;
import java.util.Iterator;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public final class BucketContainerShort<O extends OBShort> implements
        BucketContainer < O, ObjectBucketShort, OBQueryShort < O >> {
    /**
     * Number of pivots for this bucket.
     */
    private int pivots;

    /**
     * Level of this bucket.
     */
    private int level;

    /**
     * if the bucket is the exclusion bucket.
     */
    private boolean exclusionBucket;

    /**
     * The storage id.
     */
    private long storage;

    /**
     * Bucket id within a level.
     */
    private long bucketId;

    /**
     * The data in this bucket, it is updated every time we insert or delete an
     * object. <int item count><smap vector1><obj id1>,<smap vector2><obj
     * id2>...
     */
    private byte[] data; // all the data of this bucket.

    /**
     * High-level view of our smap vectors. Created lazily.
     */
    private ArrayList < ObjectBucketShort > dataView = null;

    /**
     * We need the index to perform some extra operations.
     */
    private Index < O > index;

    
    @Override
    public Result delete(ObjectBucketShort bucket, O object) throws OBException, DatabaseException, IllegalIdException,
    IllegalAccessException, InstantiationException{
        ArrayList < ObjectBucketShort > v = getSmapVectors();
        Iterator < ObjectBucketShort > it = v.iterator();
        Result res = new Result();
        res.setStatus(Result.Status.NOT_EXISTS);
        while (it.hasNext()) {
            ObjectBucketShort j = it.next();
            if (j.smapEqual(bucket)
                    && index.getObject(j.getId()).distance(object) == 0) {
                it.remove();
                res.setStatus(Result.Status.EXISTS);
                res.setId(j.getId());
            }
        }
        // if an object was removed.
        if(res.getStatus() == Result.Status.EXISTS){
            updateData();
        }
        return res;
    }
    
    /**
     * Read dataView and update data[]
     */
    private void updateData(){
        ArrayList < ObjectBucketShort > v = getSmapVectors();
        TupleOutput out = new TupleOutput();
        out.writeInt(v.size());
        for(ObjectBucketShort b : v){
            for(short j: b.getSmapVector()){
                out.writeShort(j);
            }
            out.writeInt(b.getId());
        }
        this.data = out.getBufferBytes();
    }

    private ArrayList < ObjectBucketShort > getSmapVectors() {
        if (dataView == null) {
            TupleInput in = new TupleInput(data);
            int count = in.readInt();
            ArrayList < ObjectBucketShort > view = new ArrayList < ObjectBucketShort >(
                    count);
            int i = 0;
            while (i < count) {
                int cx = 0;
                short[] tuple = new short[pivots];
                while (cx < this.pivots) {
                    tuple[cx] = in.readShort();
                    cx++;
                }
                int id = in.readInt();
                view.add(new ObjectBucketShort(this.bucketId, this.level,
                        tuple, this.exclusionBucket, id));
                i++;
            }
            dataView = view;
        }
        return dataView;
    }

    
    @Override
    public byte[] getBytes() {
        return data;
    }

    
    @Override
    public Result exists(ObjectBucketShort bucket, O object) throws OBException, DatabaseException, IllegalIdException,
    IllegalAccessException, InstantiationException{
        ArrayList < ObjectBucketShort > v = getSmapVectors();
        Iterator < ObjectBucketShort > it = v.iterator();
        Result res = new Result();
        res.setStatus(Result.Status.NOT_EXISTS);
        while (it.hasNext()) {
            ObjectBucketShort j = it.next();
            if (j.smapEqual(bucket)
                    && index.getObject(j.getId()).distance(object) == 0) {               
                res.setStatus(Result.Status.EXISTS);
                res.setId(j.getId());
                return res;
            }
        }
        return res;
    }
    
    public Result insert(ObjectBucketShort bucket) throws OBException, DatabaseException, IllegalIdException,
    IllegalAccessException, InstantiationException{
        Result res = new Result();
        res.setStatus(Result.Status.OK);
        ArrayList < ObjectBucketShort > v = getSmapVectors();
        v.add(bucket);
        updateData();
        return res;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.d.BucketContainer#search(java.lang.Object,
     *      org.ajmm.obsearch.OB)
     */
    @Override
    public void search(OBQueryShort < O > query, O object) {
        // TODO Auto-generated method stub

    }

}
