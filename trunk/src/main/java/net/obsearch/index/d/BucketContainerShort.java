package net.obsearch.index.d;

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
import net.obsearch.index.utils.MyTupleInput;

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
    private List < ObjectBucketShort > dataView = null;

    /**
     * We need the index to perform some extra operations.
     */
    private Index < O > index;

    private static final int UNIT_SIZE = Short.SIZE / 8;

    private static final int BASE = 4 * 3; // pivots count level

    private int TUPLE_SIZE;

    short[][] mbr = null;

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
        updateTupleSize(pivots);
        int count = in.readInt();
        size = count;
        level = in.readInt();
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
    public void bulkInsert(List < ObjectBucketShort > data) {
        Collections.sort(data);
        updateData(data);
    }

    // need to update this thing.
    @Override
    public OperationStatus delete(ObjectBucketShort bucket, O object)
            throws OBException, DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException {
        List < ObjectBucketShort > v = getVectors();
        Iterator < ObjectBucketShort > it = v.iterator();
        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
        while (it.hasNext()) {
            ObjectBucketShort j = it.next();
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
    private void updateData(List < ObjectBucketShort > v) {
        if (v.size() > 0) {
            // Collections.sort(v);
            TupleOutput out = new TupleOutput(
                    new byte[(TUPLE_SIZE * (v.size() + 1)) + BASE]);
            size = v.size();
            if(pivots != 0){
                assert pivots == v.get(0).getSmapVector().length;
            }
            out.writeInt(pivots);
            out.writeInt(v.size());
            out.writeInt(level);
            for (ObjectBucketShort b : v) {
                assert pivots == b.getSmapVector().length : " pivots: "
                        + pivots + " item: " + b.getSmapVector().length;
                for (short j : b.getSmapVector()) {
                    out.writeShort(j);
                }
                out.writeInt(b.getId());
            }
            this.data = out.getBufferBytes();
            readFromData();
        }
    }

    private byte[] initHeader(int size, int pivots, int level) {
        updateTupleSize(pivots);
        TupleOutput out = new TupleOutput(new byte[BASE]);
        out.writeInt(pivots);
        out.writeInt(size);
        out.writeInt(level);
        this.size = size;
        this.pivots = pivots;
        this.level = level;
        return out.getBufferBytes();
    }

    private void updateHeader(int size, int pivots, int level) {

        byte[] source = initHeader(size, pivots, level);
        System.arraycopy(source, 0, data, 0, BASE);
    }

    private byte[] serializeBucket(ObjectBucketShort b) {
        TupleOutput out = new TupleOutput(new byte[TUPLE_SIZE]);
        for (short j : b.getSmapVector()) {
            out.writeShort(j);
        }
        out.writeInt(b.getId());
        return out.toByteArray();
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

        }
        return dataView;
    }

    @Override
    public byte[] getBytes() {
        return data;
    }

    @Override
    public OperationStatus exists(ObjectBucketShort bucket, O object)
            throws OBException, DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException {

        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
        if (data != null) {
            MyTupleInput in = new MyTupleInput(data);
            int low = binSearch(bucket.getSmapVector()[0], in);

            if (low < size()) {
                // if it was found.

                while (low < size()) {
                    ObjectBucketShort tuple = this.getIthSMAP(low, in);
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

    public OperationStatus insert(ObjectBucketShort bucket) throws OBException,
            DatabaseException, IllegalIdException, IllegalAccessException,
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
            data = initHeader(0, bucket.getPivotSize(), bucket.getLevel());
        }

        MyTupleInput in = new MyTupleInput(data);
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
        updateHeader(size, bucket.getPivotSize(), bucket.getLevel());

        updateMBR(bucket);

        return res;
    }

    public int size() {
        return size;
    }

    public short[][] getMBR() {

        if (mbr == null) {
            initMBR();
            // leave mbr null if there are no pivots.
        }
        /*
         * int pivotSize = mbr[0].length; short[][] res = new
         * short[2][pivotSize]; int i = 0; while(i < pivotSize){ res[0][i] =
         * mbr[0][i]; res[1][i] = mbr[1][i]; i++; }
         */
        return mbr;
    }

    private void initMBR() {

        MyTupleInput in = new MyTupleInput(data);
        int i = 0;
        while (i < this.size) {
            updateMBR(this.getIthSMAP(i, in));
            i++;
        }
    }

    private void updateMBR(ObjectBucketShort o) {
        updateMBR(o.getSmapVector());
    }

    private void updateMBR(short[] smap) {
        if (mbr == null) {
            int pivotSize = smap.length;
            mbr = new short[2][pivotSize];
            int i = 0;
            // initialize result.
            while (i < pivotSize) {
                mbr[0][i] = Short.MAX_VALUE;
                i++;
            }
            initMBR();
        }

        int i = 0;

        while (i < smap.length) {
            if (smap[i] < mbr[0][i]) {
                mbr[0][i] = smap[i];
            }
            if (smap[i] > mbr[1][i]) {
                mbr[1][i] = smap[i];
            }
            i++;
        }
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.index.d.BucketContainer#search(java.lang.Object,
     *      net.obsearch.result.OB) returns the # of distance computations.
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
        in.setOffset(getByteArrayIndex(i));
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

    private ObjectBucketShort getIthSMAP(int i, MyTupleInput in) {
        setIth(i, in);
        int cx = 0;
        short[] res = new short[pivots];
        while (cx < this.pivots) {
            res[cx] = in.readShort();
            cx++;
        }
        int id = in.readInt();
        return new ObjectBucketShort(res, id);
    }

    private int binSearch(short value, MyTupleInput in) {
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
    public long searchSorted(OBQueryShort < O > query, ObjectBucketShort b,
            IntegerHolder smapComputations) throws IllegalAccessException,
            DatabaseException, OBException, InstantiationException,
            IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        MyTupleInput in = new MyTupleInput(data);
        // readFromDataAux(in);
        assert pivots == b.getSmapVector().length;
        assert b.getLevel() == level;
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
     * @see net.obsearch.index.d.BucketContainer#getPivots()
     */
    @Override
    public int getPivots() {
        return this.pivots;
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.index.d.BucketContainer#setPivots()
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
