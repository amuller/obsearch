<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="AbstractBucketContainer${Type}.java" />
package net.obsearch.index.bucket.impl;
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
import net.obsearch.index.bucket.BucketContainer;

import net.obsearch.ob.OB${Type};
import net.obsearch.query.OBQuery${Type};
import net.obsearch.utils.bytes.ByteConversion;

/** 
	*  AbstractBucketContainer${Type} Holds the functionality of a
  *  bucket that sorts its smap-vectors lexicographically. Binary
  *  searches are employed inside the vector.
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */
<@gen_warning filename="AbstractBucketContainer.java "/>
public abstract class AbstractBucketContainer${Type} < O extends OB${Type}, B extends BucketObject${Type} > implements
        BucketContainer < O, B, OBQuery${Type} < O >> {

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
		
		/**
     * Header size of the bucket (pivot count and # of elements in the bucket)
		 */
    private static final int BASE = net.obsearch.constants.ByteConstants.Int.getSize() * 2; // pivots count level

    private int TUPLE_SIZE;

    /**
     * Cache used to avoid byte readings.
     */
    private BucketObject${Type}[] cache;

    public AbstractBucketContainer${Type}(Index < O > index, ByteBuffer data, int pivots) {
        assert index != null;
        this.index = index;
        this.data = data;
        this.pivots = pivots;
        updateTupleSize(pivots);
        if (data != null) {
            int pivotsX = data.getInt();
            assert pivots == pivotsX;
            size = data.getInt();
        }else{
            size = 0;
        }
    }

    private void cleanCache() {
        cache = null;
    }

    private void updateTupleSize(int pivots) {
        TUPLE_SIZE = (pivots * net.obsearch.constants.ByteConstants.${Type}
                .getSize())
                + Index.ID_SIZE;
    }

    /**
     * Bulk insert data.
     * @param data
     *                The data that will be inserted.
     */
    public void bulkInsert(List < B > data) {
        Collections.sort(data);
        updateData(data);
    }

    @Override
    public void setPivots(int pivots) {
        this.pivots = pivots;

    }

    // need to update this thing.
    @Override
    public OperationStatus delete(B bucket, O object)
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
                BucketObject${Type} j = getIthSMAP(i, this.data);
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
        cleanCache();
        return res;
    }

    /**
     * Insert the given bucket into the container. We assume the bucket to be
     * inserted does not exist.
     */
    public OperationStatus insert(BucketObject${Type} bucket) throws OBException,
            IllegalIdException, IllegalAccessException, InstantiationException {

        OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);

        ByteBuffer newByteBuffer = ByteConversion
                .createByteBuffer(calculateBufferSize(size + 1));
        boolean found = false;
        this.updateHeader(size + 1, pivots, newByteBuffer);
        int i = 0;
        int written = 0;
        // search the position for the given bucket.
        int low = binSearch(bucket, data);
        while (i < size) {
            BucketObject${Type} j = getIthSMAP(i, this.data);
            if (i == low) {
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
        if(size == 0 || low == size){
        	res.setStatus(Status.OK);
            res.setId(bucket.getId());
            bucket.write(newByteBuffer);
            found = true;
            written++;
        }
        assert written == size + 1;
        data = newByteBuffer;
        size++;
        cleanCache();
        return res;
    }

    /**
     * Calculate buffer size for n items.
     * @param i
     *                Number of items to add.
     * @return the number of bytes required to store n smap vectors.
     */
		<@gen_warning filename="AbstractBucketContainer.java "/>
    private int calculateBufferSize(int i) {
        return (TUPLE_SIZE * i) + BASE;
    }

    /**
     * Read dataView and update data[]
     */
    private void updateData(List < B > v) {
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
            for (BucketObject${Type} b : v) {
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
    public OperationStatus exists(B bucket, O object)
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
                    BucketObject${Type} tuple = this.getIthSMAP(low, in);
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
		<@gen_warning filename="AbstractBucketContainer.java "/>
    @Override
    public long search(OBQuery${Type} < O > query, B b)
            throws IllegalAccessException, OBException, InstantiationException,
            IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        assert pivots == b.getSmapVector().length;

        int i = 0;
        ${type} range = query.getDistance();
        // for every item in this bucket.
        long res = 0;
        while (i < size) {
            // calculate L-inf
            BucketObject${Type} other = this.getIthSMAP(i, data);
            ${type} max = b.lInf(other);

            if (max <= query.getDistance() && query.isCandidate(max)) {
                long id = other.getId();
                O toCompare = index.getObject(id);
                ${type} realDistance = object.distance(toCompare);
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
        return AbstractBucketContainer${Type}.BASE + (i * TUPLE_SIZE);
    }

    /**
     * Gets the ith BucketObject${Type} from this bucket.
     * @param i
     *                I-th vector.
     * @param in
     *                the bucket to modify
     * @return
     */
    private BucketObject${Type} getIthSMAP(int i, ByteBuffer in) {
        if (cache == null) {
            cache = new BucketObject${Type}[size];
        }
        if (cache[i] != null ) {
            return cache[i];
        } else {
            setIth(i, in);
            
            BucketObject${Type} result = instantiateBucketObject();
            result.read(in, this.getPivots());
            cache[i] = result;
            return result;
        }
    }
    
    /**
     * Instantiate an empty bucket ready to be filled with stuff.
     * @return empty BucketObject${Type};
     */
    protected abstract BucketObject${Type} instantiateBucketObject();
    
    protected BucketObject${Type} instantiateBucketObject(${type}[] smap, long id){
    	BucketObject${Type} res = instantiateBucketObject();
    	res.setId(id);
    	res.setSmapVector(smap);
    	return res;
    }

    /**
     * Search the bucket set and leave the low in the
     * first entry greater or equal than b. This means that
     * if you want to insert b in this Bucket Container, 
     * you have to usert it in the position returned by this function.
     * @param b
     * @param in
     * @return the position in which b should be inserted.
     */
    private int binSearch(BucketObject${Type} b, ByteBuffer in) {
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
    public long searchSorted(OBQuery${Type} < O > query, BucketObject${Type} b,
            IntegerHolder smapComputations) throws IllegalAccessException,
            OBException, InstantiationException, IllegalIdException {
        if (data == null) {
            return 0;
        }
        O object = query.getObject();

        ByteBuffer in = data;
        assert pivots == b.getSmapVector().length;

        // now we can start binary searching the bytes.

        int low = binSearch(this.instantiateBucketObject(query.getLow(), -1), in);

        int i = low;

        ${type} range = query.getDistance();
        BucketObject${Type} top = this.instantiateBucketObject(query.getHigh(), -1);
        // for every item in this bucket.
        long res = 0;
        while (i < size) {
            BucketObject${Type} current = getIthSMAP(i, in);
            if (!(current.compareTo(top) <= 0)) {
                break;
            }
            ${type} max = current.lInf(b);
            smapComputations.inc();
            if (max <= query.getDistance() && query.isCandidate(max)) {
                long id = current.getId();
                O toCompare = index.getObject(id);
                ${type} realDistance = object.distance(toCompare);
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
		<@gen_warning filename="AbstractBucketContainer.java "/>
    @Override
    public int getPivots() {
        return this.pivots;
    }

}
</#list>