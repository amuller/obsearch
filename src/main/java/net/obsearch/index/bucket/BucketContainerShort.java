package net.obsearch.index.bucket;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;

import net.obsearch.Index;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.index.utils.IntegerHolder;


import net.obsearch.ob.OBShort;
import net.obsearch.query.OBQueryShort;
import net.obsearch.utils.bytes.ByteConversion;



public final class BucketContainerShort < O extends OBShort > implements
        BucketContainer < O, ObjectInBucketShort, OBQueryShort < O >> {

    /**
     * # of pivots in this Bucket container.
     */
    private int pivots;

    

    private int size;

    /**
     * The data in this bucket, it is updated every time we insert or delete an
     * object. <int item count><smap vector1><obj id1>,<smap vector2><obj
     * id2>...
     */
    private byte[] data; // all the data of this bucket.

    /**
     * High-level view of our smap vectors. Created lazily.
     */
    private List < ObjectInBucketShort > dataView = null;

    /**
     * We need the index to perform some extra operations.
     */
    private Index < O > index;

    private static final int UNIT_SIZE = Short.SIZE / 8;

    private static final int BASE = 4 * 2; // pivots count level

    private int TUPLE_SIZE;

    short[][] mbr = null;

    public BucketContainerShort(Index < O > index, byte[] data, int pivots) {
        assert index != null;
        this.index = index;
        this.data = data;
        this.pivots = pivots;
        updateTupleSize(pivots);
        readFromData();
    }

    private int readFromData() {
        if (data != null) {
            return readFromDataAux(ByteConversion.createByteBuffer(data));
        } else {
            return 0;
        }
    }

    private int readFromDataAux(ByteBuffer in) {
        pivots = in.getInt();
        updateTupleSize(pivots);
        int count = in.getInt();
        size = count;
        return count;
    }

    private void updateTupleSize(int pivots) {
        TUPLE_SIZE = pivots * UNIT_SIZE + Index.ID_SIZE;
    }

    /**
     * Bulk insert data.
     * @param data
     *                The data that will be inserted.
     */
    public void bulkInsert(List < ObjectInBucketShort > data) {
        Collections.sort(data);
        updateData(data);
    }

    // need to update this thing.
    @Override
    public OperationStatus delete(ObjectInBucketShort bucket, O object)
            throws OBException,  IllegalIdException,
            IllegalAccessException, InstantiationException {
        List < ObjectInBucketShort > v = getVectors();
        Iterator < ObjectInBucketShort > it = v.iterator();
        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
        while (it.hasNext()) {
            ObjectInBucketShort j = it.next();
            if (j.smapEqual(bucket)
                    && index.getObject(j.getId()).distance(object) == 0) {
                it.remove();
                size--;
                res.setStatus(Status.OK);
                res.setId(j.getId());
            }
        }
        mbr = null;
        // if an object was removed.
        // if (res.getStatus() == Result.Status.EXISTS) {
        updateData();

        // }
        return res;
    }

    private void updateData() {
        updateData(getVectors());
    }

    /**
     * Read dataView and update data[]
     */
    private void updateData(List < ObjectInBucketShort > v) {
        if (v.size() > 0) {
            // Collections.sort(v);
            ByteBuffer out = ByteConversion.createByteBuffer(
                    (TUPLE_SIZE * (v.size() + 1)) + BASE);
            size = v.size();
            if(pivots != 0){
                assert pivots == v.get(0).getSmapVector().length;
            }
            out.putInt(pivots);
            out.putInt(v.size());
            for (ObjectInBucketShort b : v) {
                assert pivots == b.getSmapVector().length : " pivots: "
                        + pivots + " item: " + b.getSmapVector().length;
                for (short j : b.getSmapVector()) {
                    out.putShort(j);
                }
                out.putLong(b.getId());
            }
            this.data = out.array();
            readFromData();
        }
    }

    private byte[] initHeader(int size, int pivots) {
        updateTupleSize(pivots);
        ByteBuffer out = ByteConversion.createByteBuffer(BASE);
        out.putInt(pivots);
        out.putInt(size);
        this.size = size;
        this.pivots = pivots;
        return out.array();
    }

    private void updateHeader(int size, int pivots) {

        byte[] source = initHeader(size, pivots);
        System.arraycopy(source, 0, data, 0, BASE);
    }

    private byte[] serializeBucket(ObjectInBucketShort b) {
        ByteBuffer out = ByteConversion.createByteBuffer(TUPLE_SIZE);
        for (short j : b.getSmapVector()) {
            out.putShort(j);
        }
        out.putLong(b.getId());
        return out.array();
    }

    private List < ObjectInBucketShort > getVectors() {

        if (dataView == null) {
            int count;
            ByteBuffer in = null;
            if (data != null) {
                in = ByteConversion.createByteBuffer(data);

                count = readFromDataAux(in);

            } else {
                count = 0;

            }
            List < ObjectInBucketShort > view = new LinkedList < ObjectInBucketShort >();
            int i = 0;
            while (i < count) {
                int cx = 0;
                short[] tuple = new short[pivots];
                while (cx < pivots) {
                    tuple[cx] = in.getShort();
                    cx++;
                }
                long id = in.getLong();
                view.add(new ObjectInBucketShort(-1, tuple, id));
                i++;
            }
            dataView = view;

        }
        return dataView;
    }

    @Override
    public ByteBuffer getBytes() {
        return ByteConversion.createByteBuffer(data);
    }

    @Override
    public OperationStatus exists(ObjectInBucketShort bucket, O object)
            throws OBException, IllegalIdException,
            IllegalAccessException, InstantiationException {

        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
        if (data != null) {
            ByteBuffer in = ByteConversion.createByteBuffer(data);
            int low = binSearch(bucket.getSmapVector()[0], in);

            if (low < size()) {
                // if it was found.

                while (low < size()) {
                    ObjectInBucketShort tuple = this.getIthSMAP(low, in);
                    if (tuple.getSmapVector()[0] != bucket.getSmapVector()[0]) {
                        break;
                    }
                    if (Arrays.equals(tuple.getSmapVector(), bucket
                            .getSmapVector())) {
                        O o2 = index.getObject(tuple.getId());
                        if (o2.distance(object) == 0) {
                            res.setStatus(Status.EXISTS);
                            res.setId(tuple.getId());
                            break;
                        }
                    }
                    low++;
                }
            }
        }
        return res;
    }

    public OperationStatus insert(ObjectInBucketShort bucket) throws OBException,
             IllegalIdException, IllegalAccessException,
            InstantiationException {
        OperationStatus res = new OperationStatus();
        // assert this.exclusionBucket == bucket.isExclusionBucket():
        // "Container: " + this.exclusionBucket + " bucket: " +
        // bucket.isExclusionBucket();
        assert pivots == bucket.getSmapVector().length : " pivots: " + pivots
                + " obj " + bucket.getSmapVector().length;
        res.setStatus(Status.OK);

        if (data == null) {
            size = 0;
            data = initHeader(0, bucket.getPivotSize());
        }

        ByteBuffer in = ByteConversion.createByteBuffer(data);
        int low = binSearch(bucket.getSmapVector()[0], in);
        byte[] ndata = new byte[data.length + TUPLE_SIZE]; // will copy some
                                                            // garbage at the
                                                            // end.

        int tupleArrayIndex = this.getByteArrayIndex(low);
        // copy the data before the tuple that will be inserted.
        System.arraycopy(data, 0, ndata, 0, Math.max(tupleArrayIndex, 0));
        // copy the tuple to be inserted
        System.arraycopy(serializeBucket(bucket), 0, ndata, tupleArrayIndex,
                TUPLE_SIZE);
        // copy the remaining of the data.
        System.arraycopy(data, tupleArrayIndex, ndata,
                getByteArrayIndex(low + 1), data.length - (tupleArrayIndex));

        data = ndata;
        size++;
        updateHeader(size, bucket.getPivotSize());
        

        return res;
    }

    public int size() {
        return size;
    }

    

    
    /*
     * (non-Javadoc)
     * @see net.obsearch.index.bucket.BucketContainer#search(java.lang.Object,
     *      net.obsearch.result.OB) returns the # of distance computations.
     */
    @Override
    public long search(OBQueryShort < O > query, ObjectInBucketShort b)
            throws IllegalAccessException,  OBException,
            InstantiationException, IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        ByteBuffer in = ByteConversion.createByteBuffer(data);
        int count = readFromDataAux(in);
        assert pivots == b.getSmapVector().length;

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
                t = (short) Math.abs(smapVector[cx] - in.getShort());
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
            long id = in.getLong(); // read the id
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
    private short getIthPivot0(int i, ByteBuffer in) {
        setIth(i, in);
        return in.getShort();
    }

    private void setIth(int i, ByteBuffer in) {    	
        in.position(getByteArrayIndex(i));
    }

    /**
     * Returns the byte index for the ith tuple. (to access directly the data
     * byte array).
     * @param i
     * @return
     */
    private int getByteArrayIndex(int i) {
        return this.BASE + (i * TUPLE_SIZE);
    }

    private ObjectInBucketShort getIthSMAP(int i, ByteBuffer in) {
        setIth(i, in);
        int cx = 0;
        short[] res = new short[pivots];
        while (cx < this.pivots) {
            res[cx] = in.getShort();
            cx++;
        }
        long id = in.getLong();
        return new ObjectInBucketShort(res, id);
    }

    private int binSearch(short value, ByteBuffer in) {
        int low = 0;
        int high = size();

        while (low < high) {
            int mid = (low + high) / 2;
            if (getIthPivot0(mid, in) < value) {
                low = mid + 1;
            } else {
                // can't be high = mid-1: here A[mid] >= value,
                // so high can't be < mid if A[mid] == value
                high = mid;
            }
        }
        return low;

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
    public long searchSorted(OBQueryShort < O > query, ObjectInBucketShort b,
            IntegerHolder smapComputations) throws IllegalAccessException,
            OBException, InstantiationException,
            IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        ByteBuffer in = ByteConversion.createByteBuffer(data);
        // readFromDataAux(in);
        assert pivots == b.getSmapVector().length;
        // value to search
        short value = query.getLow()[0];
        // now we can start binary searching the bytes.

        int low = binSearch(value, in);
        // base will hold the tuple from which we will start processing
        int base;
        short currentP0;
        // high == low, using high or low depends on taste
        if (low < size()) {
            base = low;
            currentP0 = getIthPivot0(low, in);
        } else {
            return 0; // not found
        }
        int count = size() - base;
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
            short pivotValue = in.getShort();
            currentP0 = pivotValue;
            t = (short) Math.abs(smapVector[cx] - pivotValue);
            if (t > max) {
                max = t;
            }
            // read next pivot value
            cx++;
            while (cx < smapVector.length) {
                pivotValue = in.getShort();
                t = (short) Math.abs(smapVector[cx] - pivotValue);
                if (t > max) {
                    max = t;
                }
                // read next pivot value
                cx++;
            }
            // LOOP ***************************

            long id = in.getLong(); // read the id
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
     * @see net.obsearch.index.bucket.BucketContainer#getPivots()
     */
    @Override
    public int getPivots() {
        return this.pivots;
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.index.bucket.BucketContainer#setPivots()
     */
    @Override
    public void setPivots(int pivots) {
        if (pivots != 0 && pivots != pivots) {
            throw new IllegalArgumentException();
        }
        this.pivots = pivots;
    }

   

}
