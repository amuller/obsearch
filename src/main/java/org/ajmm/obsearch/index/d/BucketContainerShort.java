package org.ajmm.obsearch.index.d;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.utils.IntegerHolder;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.query.OBQueryShort;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public final class BucketContainerShort < O extends OBShort > implements
        BucketContainer < O, ObjectBucketShort, OBQueryShort < O >> {

    /**
     * # of pivots in this Bucket container.
     */
    private int pivots;

    /**
     * Level of the bucket (debugging purposes)
     */
    private int level;

    /**
     * The data in this bucket, it is updated every time we insert or delete an
     * object. <int item count><smap vector1><obj id1>,<smap vector2><obj
     * id2>...
     */
    private byte[] data; // all the data of this bucket.

    /**
     * High-level view of our smap vectors. Created lazily.
     */
    private List < ObjectBucketShort > dataView = null;

    /**
     * We need the index to perform some extra operations.
     */
    private Index < O > index;

    private static final int UNIT_SIZE = Short.SIZE / 8;

    private static final int BASE = 4 * 3; // pivots count level

    private int TUPLE_SIZE;

    public BucketContainerShort(Index < O > index, byte[] data) {
        assert index != null;
        this.index = index;
        this.data = data;
        readFromData();
    }

    private int readFromData() {
        if (data != null) {
            return readFromDataAux(new TupleInput(data));
        } else {
            return 0;
        }
    }

    private int readFromDataAux(TupleInput in) {
        pivots = in.readInt();
        TUPLE_SIZE = pivots * UNIT_SIZE + Index.ID_SIZE;
        int count = in.readInt();
        level = in.readInt();
        return count;
    }

    @Override
    public Result delete(ObjectBucketShort bucket, O object)
            throws OBException, DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException {
        List < ObjectBucketShort > v = getVectors();
        Iterator < ObjectBucketShort > it = v.iterator();
        Result res = new Result();
        res.setStatus(Result.Status.NOT_EXISTS);
        while (it.hasNext()) {
            ObjectBucketShort j = it.next();
            if (j.smapEqual(bucket)
                    && index.getObject(j.getId()).distance(object) == 0) {
                it.remove();
                res.setStatus(Result.Status.OK);
                res.setId(j.getId());
            }
        }
        // if an object was removed.
        // if (res.getStatus() == Result.Status.EXISTS) {
        updateData();
        // }
        return res;
    }

    /**
     * Read dataView and update data[]
     */
    private void updateData() {
        List < ObjectBucketShort > v = getVectors();
        Collections.sort(v);
        TupleOutput out = new TupleOutput();
        assert pivots != 0;
        out.writeInt(pivots);
        out.writeInt(v.size());
        out.writeInt(level);
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

    private List < ObjectBucketShort > getVectors() {

        if (dataView == null) {
            int count;
            TupleInput in = null;
            if (data != null) {
                in = new TupleInput(data);

                count = readFromDataAux(in);

            } else {
                count = 0;

            }
            List < ObjectBucketShort > view = new LinkedList < ObjectBucketShort >();
            int i = 0;
            while (i < count) {
                int cx = 0;
                short[] tuple = new short[pivots];
                while (cx < pivots) {
                    tuple[cx] = in.readShort();
                    cx++;
                }
                int id = in.readInt();
                view.add(new ObjectBucketShort(-1, -1, tuple, false, id));
                i++;
            }
            dataView = view;
            if (data != null) {
                // assert in.available() == 0 : "available: " + in.available();
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
        List < ObjectBucketShort > v = getVectors();
        Iterator < ObjectBucketShort > it = v.iterator();
        Result res = new Result();
        res.setStatus(Result.Status.NOT_EXISTS);
        while (it.hasNext()) {
            ObjectBucketShort j = it.next();
            if (j.smapEqual(bucket)) {
                O o2 = index.getObject(j.getId());
                if (o2.distance(object) == 0) {
                    res.setStatus(Result.Status.EXISTS);
                    res.setId(j.getId());
                    return res;
                }
            }
        }
        return res;
    }

    public Result insert(ObjectBucketShort bucket) throws OBException,
            DatabaseException, IllegalIdException, IllegalAccessException,
            InstantiationException {
        Result res = new Result();
        // assert this.exclusionBucket == bucket.isExclusionBucket():
        // "Container: " + this.exclusionBucket + " bucket: " +
        // bucket.isExclusionBucket();
        assert pivots == bucket.getSmapVector().length : " pivots: " + pivots
                + " obj " + bucket.getSmapVector().length;
        res.setStatus(Result.Status.OK);
        List < ObjectBucketShort > v = getVectors();
        v.add(bucket);
        updateData();
        return res;
    }

    public int size() {
        return getVectors().size();
    }

    public short[][] getMBR() {
        short[][] res = null;
        List < ObjectBucketShort > v = getVectors();
        if (v.size() > 0) {
            int pivotSize = v.get(0).getPivotSize();
            res = new short[2][pivotSize];
            int i = 0;
            // initialize result.
            while (i < pivotSize) {
                res[0][i] = Short.MAX_VALUE;
                i++;
            }
            for (ObjectBucketShort o : v) {
                // min = 0, max = 1.
                i = 0;
                short[] smap = o.getSmapVector();
                while (i < smap.length) {
                    if (smap[i] < res[0][i]) {
                        res[0][i] = smap[i];
                    }
                    if (smap[i] > res[1][i]) {
                        res[1][i] = smap[i];
                    }
                    i++;
                }
            }
        }
        return res;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.index.d.BucketContainer#search(java.lang.Object,
     *      org.ajmm.obsearch.OB) returns the # of distance computations.
     */
    @Override
    public long search(OBQueryShort < O > query, ObjectBucketShort b)
            throws IllegalAccessException, DatabaseException, OBException,
            InstantiationException, IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        TupleInput in = new TupleInput(data);
        int count = readFromDataAux(in);
        assert pivots == b.getSmapVector().length;
        assert b.getLevel() == level;

        int i = 0;
        short[] smapVector = b.getSmapVector();
        short range = query.getDistance();
        // for every item in this bucket.
        long res = 0;
        while (i < count) {
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
                res++;
                if (realDistance <= range) {
                    query.add(id, toCompare, realDistance);
                }
            }
            i++;
        }
        // assert in.available() == 0 : "Avail: " + in.available();
        return res;
    }

    /**
     * Gets the ith pivot tuple from the given in. Assumes that
     * @param i
     * @param in
     * @return
     */
    private short getIthPivot0(int i, MyTupleInput in) {
        setIth(i, in);
        return in.readShort();
    }

    private void setIth(int i, MyTupleInput in) {
        in.setOffset(this.BASE + i * TUPLE_SIZE);
    }

    /**
     * Searches the data by using a binary search to reduce SMAP vector
     * computations.
     * @param query
     * @param b
     * @return
     * @throws IllegalAccessException
     * @throws DatabaseException
     * @throws OBException
     * @throws InstantiationException
     * @throws IllegalIdException
     */
    public long searchSorted(OBQueryShort < O > query, ObjectBucketShort b,
            IntegerHolder smapComputations) throws IllegalAccessException,
            DatabaseException, OBException, InstantiationException,
            IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        MyTupleInput in = new MyTupleInput(data);
        int count = readFromDataAux(in);
        assert pivots == b.getSmapVector().length;
        assert b.getLevel() == level;
        // value to search
        short value = query.getLow()[0];
        // now we can start binary searching the bytes.
        
        
        // Adapted from wikipedia
        // http://en.wikipedia.org/wiki/Binary_search
        
        int low = 0;
        int high = count;
        while (low < high) {
            int mid = (low + high)/2;
            if (getIthPivot0(mid,in) < value){
                low = mid + 1; 
            }
            else{
                 //can't be high = mid-1: here A[mid] >= value,
                 //so high can't be < mid if A[mid] == value
                 high = mid;
            }            
        }
        // base will hold the tuple from which we will start processing
        int base;
        short currentP0;
        // high == low, using high or low depends on taste 
        if (low < count){
           base = low;
           currentP0 = getIthPivot0(low,in);
        }
        else{
            return 0; // not found       
        }
        count = count - base;
        setIth(base, in);
        
        int i = 0;
        short[] smapVector = b.getSmapVector();
        short range = query.getDistance();
        short top = query.getHigh()[0];
        // for every item in this bucket.
        long res = 0;
        while (i < count && currentP0 <= top) {
            // calculate L-inf
            short max = Short.MIN_VALUE;
            short t;
            int cx = 0;
            // L-inf
           
            smapComputations.inc();
            // LOOP ***************************
            // this is a loop, split for efficency reasons
            // beginning of the loop.
            short pivotValue = in.readShort();
            currentP0 = pivotValue;
            t = (short) Math.abs(smapVector[cx] - pivotValue);
            if (t > max) {
                max = t;                    
            }
            // read next pivot value                
            cx++;            
            while (cx < smapVector.length) {
                pivotValue = in.readShort();
                t = (short) Math.abs(smapVector[cx] - pivotValue);
                if (t > max) {
                    max = t;                    
                }
                // read next pivot value                
                cx++;
            } 
            // LOOP ***************************
            
            int id = in.readInt(); // read the id
            if (max <= query.getDistance() && query.isCandidate(max)) {
                O toCompare = index.getObject(id);
                short realDistance = object.distance(toCompare);
                res++;
                if (realDistance <= range) {
                    query.add(id, toCompare, realDistance);
                }
            }
            i++;
        }
        // assert in.available() == 0 : "Avail: " + in.available();
        return res;
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

    /**
     * @return the level
     */
    public int getLevel() {
        return level;
    }

    /**
     * @param level
     *                the level to set
     */
    public void setLevel(int level) {
        this.level = level;
    }

}
