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
import net.obsearch.filter.Filter;
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
     * When data is employed, updateList is null.
     */
    private ByteBuffer data; // all the data of this bucket.

		/**
		 * List used to cache inserts and deletes.
		 * This makes inserts and deletes *much* cheaper.
		 * When this list is used, data is null.
		 */
		private List<B> updateList;

    /**
     * We need the index to perform some extra operations.
     */
    private Index < O > index;
		
		/**
     * Header size of the bucket (pivot count and # of elements in the bucket)
		 */
    private static final int BASE = net.obsearch.constants.ByteConstants.Int.getSize() * 2; // pivots count level

    private int TUPLE_SIZE;

		

    

    public AbstractBucketContainer${Type}(Index < O > index, ByteBuffer data, int pivots) {
        assert index != null;
        this.index = index;
        this.data = data;
        this.pivots = pivots;
				this.updateList = null;
        updateTupleSize(pivots);
        if (data != null) {
            int pivotsX = data.getInt();
            assert pivots == pivotsX;
            size = data.getInt();
        }else{
            size = 0;
        }
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
        updateData();
    }

    @Override
    public void setPivots(int pivots) {
        this.pivots = pivots;

    }

		/**
		 * Fills updateList with the data from data.
		 * Sets data to null.
		 */
		private void loadUpdateList(){
				if(updateList == null){
						assert updateList == null;
						updateList = new LinkedList<B>();
						int i = 0;
						while(i < size()){
								B tuple = instantiateBucketObject();
								this.getIthSMAP(i, data, tuple);
								updateList.add(tuple);									
								i++;
						}						
						
				}			
				data = null;
				// post-conditions:
				assert data == null;
		}

    // need to update this thing.
    @Override
    public OperationStatus delete(B bucket, O object)
            throws OBException, IllegalIdException, IllegalAccessException,
            InstantiationException {

				loadUpdateList();
				OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
				ListIterator<B> it = updateList.listIterator();
				while(it.hasNext()){
						B cmp = it.next();
						if(bucket.compareTo(cmp) == 0 && index.getObject(cmp.getId()).distance(object) == 0){
								it.remove();
								res.setStatus(Status.OK);
				res.setId(cmp.getId());
								size--;
								break;
						}
				}
				assert size == updateList.size();
        return res;
    }

    /**
     * Insert the given bucket into the container.
		 * @param bucket Bucket to insert.
		 * @param object the bucket has an object.
		 * @return The result of the operation.
     */
    public OperationStatus insert(B bucket, O object) throws OBException,
            IllegalIdException, IllegalAccessException, InstantiationException {
       

        loadUpdateList();
				OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);
				ListIterator<B> it = updateList.listIterator();
				while(it.hasNext()){
						B cmp = it.next();
						int c = cmp.compareTo(bucket);
										 if(c == 0 && index.getObject(cmp.getId()).distance(object) == 0){							
				res.setStatus(Status.EXISTS);
				res.setId(cmp.getId());
								break;
						}else if(c < 0){
				break;
			}
		}
		
		if(res.getStatus() == Status.OK){
								// the cmp object is greater, we do not need
								// to search more.
												 if(it.hasPrevious()){
										it.previous();										
								}
								it.add(bucket); // ordered insert
			assert bucket.getId() != -1;
			res.setId(bucket.getId());
								size++;


				}
  			assert size == updateList.size();
        return res;
    }

		    /**
     * Insert the given bucket into the container.
		 * @param bucket Bucket to insert.
		 * @param object the bucket has an object.
		 * @return The result of the operation.
     */
    public OperationStatus insertBulk(B bucket, O object) throws OBException,
            IllegalIdException, IllegalAccessException, InstantiationException {
       

        loadUpdateList();
				OperationStatus res = new OperationStatus();
		res.setStatus(Status.OK);
		size++;
		updateList.add(bucket);
  			assert size == updateList.size();
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

		private void updateData() {
				if(updateList != null && data == null){
						updateData(updateList);
				}
		}

    /**
     * Read dataView and update data[]
     */
    private void updateData(List < B > v) {
        if (v.size() > 0) {
            Collections.sort(v);
            ByteBuffer out = ByteConversion.createByteBuffer(
																														 calculateBufferSize(v.size()));
                    
            size = v.size();
            if (pivots != 0) {
                assert pivots == v.get(0).getSmapVector().length;
            }
            this.updateHeader(v.size(), pivots, out);
            for (B b : v) {
                assert pivots == b.getSmapVector().length : " pivots: "
                        + pivots + " item: " + b.getSmapVector().length;
                b.write(out);
            }
            this.data = out;
        }
    }

    @Override
    public ByteBuffer getBytes() {
				if( updateList != null && data == null){
						updateData(updateList);						
				}
        return data;
    }

    @Override
    public OperationStatus exists(B bucket, O object)
            throws OBException, IllegalIdException, IllegalAccessException,
            InstantiationException {
				 OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
			
        if(updateList != null){
						assert updateList != null;
							Iterator<B> it = updateList.iterator();
							while(it.hasNext()){
						B cmp = it.next();
						if(bucket.compareTo(cmp) == 0 && index.getObject(cmp.getId()).distance(object) == 0){						
								res.setStatus(Status.EXISTS);							
					res.setId(cmp.getId());
								break;
						}
				}
				}
        else  {
						assert data != null;
            ByteBuffer in = data;
            int low = binSearch(bucket, in);

            if (low < size()) {
                // if it was found.
								B tuple = instantiateBucketObject();
                while (low < size()) {
										this.getIthSMAP(low, in, tuple);
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

    

		protected abstract B[] instantiateArray(int size);


		/**
		 * Lighweight bucket extract implementation.
		 */
		private void getIthSMAP(int i, ByteBuffer in, B b) {
            setIth(i, in);
            b.read(in, this.getPivots());
    }
    
    /**
     * Instantiate an empty bucket ready to be filled with stuff.
     * @return empty BucketObject${Type};
     */
    protected abstract B instantiateBucketObject();
    
    protected B instantiateBucketObject(${type}[] smap, long id){
    	B res = instantiateBucketObject();
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
    private int binSearch(B b, ByteBuffer in) {
        int low = 0;
        int high = size();
				B other =  instantiateBucketObject();
        while (low < high) {
            int mid = (low + high) / 2;
						getIthSMAP(mid, in,other);
            if (b.compareTo(other) > 0) {
                low = mid + 1;
            } else {
                high = mid;
            }
        }
        return low;
    }

		

    


		/**
     * Searches the data by using a binary search to reduce SMAP vector
     * computations. Does not cache objects and creates less
		 * objects during the search than searchSorted(...).
     * @param query
     * @param b
     * @return
     * @throws IllegalAccessException
     * @throws DatabaseException
     * @throws OBException
     * @throws InstantiationException
     * @throws IllegalIdException
     */
    public long search(OBQuery${Type} < O > query, B b,
														 IntegerHolder smapComputations, Filter<O> filter) throws IllegalAccessException,
            OBException, InstantiationException, IllegalIdException {
				updateData();
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
        B top = this.instantiateBucketObject(query.getHigh(), -1);
				B current = instantiateBucketObject();
        // for every item in this bucket.
        long res = 0;
        while (i < size) {
            getIthSMAP(i, in, current);
            if (!(current.compareTo(top) <= 0)) {
                break;
            }
            ${type} max = current.lInf(b);
            smapComputations.inc();
            if (max <= query.getDistance() && query.isCandidate(max)) {
                long id = current.getId();
                O toCompare = index.getObject(id);
								// Process query only if the filter is null.
								if(filter == null || filter.accept(toCompare, object)){
                ${type} realDistance = object.distance(toCompare);
                res++;
                if (realDistance <= range) {
                    query.add(id, toCompare, realDistance);
                }
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

		public boolean isModified(){
				return data == null;
		}

}
</#list>
