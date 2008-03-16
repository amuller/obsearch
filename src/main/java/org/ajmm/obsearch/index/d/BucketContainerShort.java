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

public final class BucketContainerShort < O extends OBShort > implements
        BucketContainer < O, ObjectBucketShort, OBQueryShort < O >> {

    /**
     * The storage id.
     */
    private long storage;

    /**
     * Bucket id within a level.
     */
    private long bucketId;

    /**
     * # of pivots in this Bucket container.
     */
    private int pivots;
    
    

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

    public BucketContainerShort(Index < O > index, byte[] data) {
        assert index != null;
        this.index = index;
        this.data = data;
    }

    @Override
    public Result delete(ObjectBucketShort bucket, O object)
            throws OBException, DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException {
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
        if (res.getStatus() == Result.Status.EXISTS) {
            updateData();
        }
        return res;
    }

    /**
     * Read dataView and update data[]
     */
    private void updateData() {
        ArrayList < ObjectBucketShort > v = getSmapVectors();
        TupleOutput out = new TupleOutput();
        assert pivots != 0;
        out.writeInt(pivots);
        out.writeInt(v.size());
        for (ObjectBucketShort b : v) {
            assert pivots == b.getSmapVector().length : " pivots: " + pivots
                    + " item: " + b.getSmapVector().length;
            for (short j : b.getSmapVector()) {
                out.writeShort(j);
            }
            out.writeInt(b.getId());
        }
        this.data = out.getBufferBytes();
    }

    private ArrayList < ObjectBucketShort > getSmapVectors() {

        if (dataView == null) {
            int count;
            TupleInput in = null;
            if (data != null) {
                in = new TupleInput(data);
                pivots = in.readInt();
                count = in.readInt();
            } else {
                count = 0;
                assert pivots != 0;
            }
            ArrayList < ObjectBucketShort > view = new ArrayList < ObjectBucketShort >(
                    count);
            int i = 0;
            while (i < count) {
                int cx = 0;
                short[] tuple = new short[pivots];
                while (cx < pivots) {
                    tuple[cx] = in.readShort();
                    cx++;
                }
                int id = in.readInt();
                view.add(new ObjectBucketShort(this.bucketId, -1,
                        tuple, false, id));
                i++;
            }
            dataView = view;
            if(data != null){
                //assert in.available() == 0 : "available: " + in.available();
            }
        }
        return dataView;
    }

    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public Result exists(ObjectBucketShort bucket, O object)
            throws OBException, DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException {
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

    public Result insert(ObjectBucketShort bucket) throws OBException,
            DatabaseException, IllegalIdException, IllegalAccessException,
            InstantiationException {
        Result res = new Result();
        //assert this.exclusionBucket == bucket.isExclusionBucket(): "Container: " + this.exclusionBucket + " bucket: " + bucket.isExclusionBucket();
        assert pivots == bucket.getSmapVector().length: " pivots: " + pivots +" obj " + bucket.getSmapVector().length;
        res.setStatus(Result.Status.OK);
        ArrayList < ObjectBucketShort > v = getSmapVectors();
        v.add(bucket);
        updateData();
        return res;
    }

    public int size() {
        return getSmapVectors().size();
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.d.BucketContainer#search(java.lang.Object,
     *      org.ajmm.obsearch.OB)
     */
    @Override
    public void search(OBQueryShort < O > query, ObjectBucketShort b)
            throws IllegalAccessException, DatabaseException, OBException,
            InstantiationException, IllegalIdException {
        if (data == null) {
            return;
        }
        O object = query.getObject();
        boolean res = true;
        TupleInput in = new TupleInput(data);
        pivots = in.readInt();
        assert pivots == b.getSmapVector().length;
        
        assert this.bucketId == b.getBucket();
        
        int items = in.readInt();


        int i = 0;
        short[] smapVector = b.getSmapVector();
        short range = query.getDistance();
        // for every item in this bucket.
        while (i < items) {
            // calculate L-inf
            short max = Short.MIN_VALUE;
            short t;
            int cx = 0;
            while (cx < smapVector.length) {
                t = (short) Math.abs(smapVector[cx] - in.readShort());
                if (t > max) {
                    max = t;
                    // inefficient, we have to read all the bytes in the bucket
                    // :(
                    // need to skip bytes.
                    /*
                     * if (t > range) { break; // finish this loop this slice
                     * won't be // matched }
                     */
                }
                cx++;
            }
            int id = in.readInt(); // read the id
            if (max <= query.getDistance() && query.isCandidate(max)) {                
                O toCompare = index.getObject(id);
                short realDistance = object.distance(toCompare);
                if (realDistance <= range) {
                    query.add(id, toCompare, realDistance);
                }
            }
            i++;
        }
        //assert in.available() == 0 : "Avail: " + in.available();
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.d.BucketContainer#getPivots()
     */
    @Override
    public int getPivots() {
        return this.pivots;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.d.BucketContainer#setPivots()
     */
    @Override
    public void setPivots(int pivots) {
        if (pivots != 0 && pivots != pivots) {
            throw new IllegalArgumentException();
        }
        this.pivots = pivots;
    }

    

    
}
