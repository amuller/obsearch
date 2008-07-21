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
        BucketContainer < O, BucketObjectShort, OBQueryShort < O >> {

    /**
     * # of pivots in this Bucket container.
     */
    private int pivots;

    /**
     * Number of objects stored in this bucket.
     */
    private int size;

    /**
     * The data in this bucket, it is updated every time we insert or delete an
     * object. <int item count><smap vector1><obj id1>,<smap vector2><obj
     * id2>...
     */
    private ByteBuffer data; // all the data of this bucket.

    /**
     * We need the index to perform some extra operations.
     */
    private Index < O > index;

    private static final int BASE = 4 * 2; // pivots count level

    private int TUPLE_SIZE;

    public BucketContainerShort(Index < O > index, ByteBuffer data, int pivots) {
        assert index != null;
        this.index = index;
        this.data = data;
        this.pivots = pivots;
        updateTupleSize(pivots);
        if(data != null){
            int pivotsX = data.getInt();
            assert pivots == pivotsX;
            size = data.getInt();
        }
    }

    private void updateTupleSize(int pivots) {
        TUPLE_SIZE = (pivots * net.obsearch.constants.ByteConstants.Short
                .getSize())
                + Index.ID_SIZE;
    }

    /**
     * Bulk insert data.
     * @param data
     *                The data that will be inserted.
     */
    public void bulkInsert(List < BucketObjectShort > data) {
        Collections.sort(data);
        updateData(data);
    }
    
    

    
    @Override
    public void setPivots(int pivots) {
       this.pivots = pivots;
        
    }

    // need to update this thing.
    @Override
    public OperationStatus delete(BucketObjectShort bucket, O object)
            throws OBException, IllegalIdException, IllegalAccessException,
            InstantiationException {

        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);

        if (exists(bucket, object).getStatus() == Status.EXISTS) {
            ByteBuffer newByteBuffer = ByteConversion
                    .createByteBuffer(calculateBufferSize(size - 1));
            boolean found = false;
            this.updateHeader(size - 1, pivots, newByteBuffer);
            int i = 0;
            while (i < size) {
                BucketObjectShort j = getIthSMAP(i, this.data);
                if (!found && j.compareTo(bucket) == 0
                        && index.getObject(j.getId()).distance(object) == 0) {
                    // delete this guy
                    res.setStatus(Status.OK);
                    res.setId(j.getId());
                    found = true;
                } else {
                    j.write(newByteBuffer);
                }
                i++;
            }
            data = newByteBuffer;
        }
        size--;
        return res;
    }

    /**
     * Insert the given bucket into the container. We assume the bucket to be inserted does not exist.
     */
    public OperationStatus insert(BucketObjectShort bucket) throws OBException,
            IllegalIdException, IllegalAccessException, InstantiationException {

        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);

        ByteBuffer newByteBuffer = ByteConversion
                .createByteBuffer(calculateBufferSize(size + 1));
        boolean found = false;
        this.updateHeader(size + 1, pivots, newByteBuffer);
        int i = 0;
        int written = 0;
        while (i < size) {
            BucketObjectShort j = getIthSMAP(i, this.data);
            if (!found && bucket.compareTo(j) > 0) {
                // insert my object.
                res.setStatus(Status.OK);
                res.setId(bucket.getId());
                bucket.write(newByteBuffer);
                found = true;
                written++;
            } 
            // write the current object.    
            j.write(newByteBuffer);
            written++;
            
            i++;
            assert j.getId() != bucket.getId();
        }
        assert written == size + 1;
        data = newByteBuffer;
        size++;
        return res;
    }

    /**
     * Calculate buffer size for n items.
     * @param i
     *                Number of items to add.
     * @return the number of bytes required to store n smap vectors.
     */
    private int calculateBufferSize(int i) {
        return (TUPLE_SIZE * i) + BASE;
    }

    /**
     * Read dataView and update data[]
     */
    private void updateData(List < BucketObjectShort > v) {
        if (v.size() > 0) {
            // Collections.sort(v);
            ByteBuffer out = ByteConversion.createByteBuffer((TUPLE_SIZE * v
                    .size())
                    + BASE);
            size = v.size();
            if (pivots != 0) {
                assert pivots == v.get(0).getSmapVector().length;
            }
            this.updateHeader(v.size(), pivots, out);
            for (BucketObjectShort b : v) {
                assert pivots == b.getSmapVector().length : " pivots: "
                        + pivots + " item: " + b.getSmapVector().length;
                b.write(out);
            }
            this.data = out;
        }
    }

    @Override
    public ByteBuffer getBytes() {
        return data;
    }

    @Override
    public OperationStatus exists(BucketObjectShort bucket, O object)
            throws OBException, IllegalIdException, IllegalAccessException,
            InstantiationException {

        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
        if (data != null) {
            ByteBuffer in = data;
            int low = binSearch(bucket, in);

            if (low < size()) {
                // if it was found.

                while (low < size()) {
                    BucketObjectShort tuple = this.getIthSMAP(low, in);
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

    public int size() {
        return size;
    }

    /**
     * Updates a ByteBuffer whose offset is in 0 with size and pivot values
     * @param size
     * @param pivots
     * @param buf
     */
    private void updateHeader(int size, int pivots, ByteBuffer buf) {
        assert 0 == buf.arrayOffset();
        buf.putInt(pivots);
        buf.putInt(size);
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.index.bucket.BucketContainer#search(java.lang.Object,
     *      net.obsearch.result.OB) returns the # of distance computations.
     */
    @Override
    public long search(OBQueryShort < O > query, BucketObjectShort b)
            throws IllegalAccessException, OBException, InstantiationException,
            IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        assert pivots == b.getSmapVector().length;

        int i = 0;
        short range = query.getDistance();
        // for every item in this bucket.
        long res = 0;
        while (i < size) {
            // calculate L-inf
            BucketObjectShort other = this.getIthSMAP(i, data);
            short max = b.lInf(other);

          
            if (max <= query.getDistance() && query.isCandidate(max)) {
                long id = other.getId();
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
        return BucketContainerShort.BASE + (i * TUPLE_SIZE);
    }

    /**
     * Gets the ith BucketObjectShort from this bucket.
     * @param i
     *                I-th vector.
     * @param in
     *                the bucket to modify
     * @return
     */
    private BucketObjectShort getIthSMAP(int i, ByteBuffer in) {
        setIth(i, in);
        int cx = 0;
        short[] res = new short[pivots];
        while (cx < this.pivots) {
            res[cx] = in.getShort();
            cx++;
        }
        long id = in.getLong();
        return new BucketObjectShort(res, id);
    }

    private int binSearch(BucketObjectShort b, ByteBuffer in) {
        int low = 0;
        int high = size();

        while (low < high) {
            int mid = (low + high) / 2;
            if (b.compareTo(getIthSMAP(mid, in)) > 0) {
                low = mid + 1;
            } else {
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
    public long searchSorted(OBQueryShort < O > query, BucketObjectShort b,
            IntegerHolder smapComputations) throws IllegalAccessException,
            OBException, InstantiationException, IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        ByteBuffer in = data;
        assert pivots == b.getSmapVector().length;

        // now we can start binary searching the bytes.

        int low = binSearch(new BucketObjectShort(query.getLow(),-1), in);
     
        int i = low;

        short range = query.getDistance();
        BucketObjectShort top = new BucketObjectShort(query.getHigh(), -1);
        // for every item in this bucket.
        long res = 0;
        while (i < size) {
            BucketObjectShort current = getIthSMAP(i, in);
            if(! (current.compareTo(top) <= 0)){
                break;
            }    
            short max = current.lInf(b);

            if (max <= query.getDistance() && query.isCandidate(max)) {
                long id = current.getId();
                O toCompare = index.getObject(id);
                short realDistance = object.distance(toCompare);
                res++;
                if (realDistance <= range) {
                    query.add(id, toCompare, realDistance);
                }
            }     
            i++;                                       
        }
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

}
