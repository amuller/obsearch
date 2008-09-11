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
import net.obsearch.constants.ByteConstants;

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
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStore${Type};
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleBytes;
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
						 * Storage device used to iterate through the bucket.
						 */
						private OBStore<TupleBytes> storage;

						/**
						 * # of pivots in this Bucket container.
						 */
						private int pivots;

						/**
						 * We need the index to perform some extra operations.
						 */
						private Index < O > index;

	

						private int TUPLE_SIZE;

						/**
						 * Additional pivot used to sort objects within the bucket.
						 */
						private int secondaryIndexPivot;
		
						/**
						 * Iterate only through this key.
						 */
						protected byte[] key;
						public AbstractBucketContainer${Type}(Index < O > index, int pivots, OBStore<TupleBytes> storage, byte[] key) {
								//this(index,pivots,storage,key, Math.abs(Arrays.hashCode(key)) % pivots);
								this(index,pivots,storage,key, Math.abs(Arrays.hashCode(key)) % pivots);
						}
		
						public AbstractBucketContainer${Type}(Index < O > index, int pivots, OBStore<TupleBytes> storage, byte[] key, int secondaryIndexPivot) {
								assert index != null;
								updateTupleSize(pivots);
								this.index = index;
								this.pivots = pivots;
								this.storage = storage;
								this.key = key;
								this.secondaryIndexPivot = secondaryIndexPivot;
						}


						/**
						 * Instantiate a new Bucket ready to process each record.
						 */
						protected abstract B instantiateBucketObject();

						@Override
						public void setPivots(int pivots) {
								this.pivots = pivots;

						}

						/**
						 * Appends value to the end of the given key.
						 */
						private byte[] buildKey(byte[] key, ${type} value){
								ByteBuffer temp = ByteConversion.createByteBuffer(key.length +ByteConstants.${Type}.getSize());
								temp.put(key);
								temp.put(storage.getFactory().serialize${Type}(value));
								return temp.array();
						}

		

						// need to update this thing.
						@Override
						public OperationStatus delete(B bucket, O object)
								throws OBException, IllegalIdException, IllegalAccessException,
								InstantiationException {
								
								byte[] key2 = buildKey(key, bucket.getSmapVector()[this.secondaryIndexPivot]);
								CloseIterator<TupleBytes> pr = storage.processRange(key2,key2);
								OperationStatus res = new OperationStatus();
								res.setStatus(Status.NOT_EXISTS);
								try{
										B cmp = instantiateBucketObject();
				
										while(pr.hasNext()){
												TupleBytes t = pr.next();
												cmp.read(t.getValue(), getPivots());
												if(bucket.compareTo(cmp) == 0 && index.getObject(cmp.getId()).distance(object) == 0){
														res.setStatus(Status.OK);
														res.setId(cmp.getId());
														pr.remove();
														break;
												}
										}
								}finally{
										pr.closeCursor();
								}
			

								return res;
						}

						private void updateTupleSize(int pivots) {
								TUPLE_SIZE = (pivots * net.obsearch.constants.ByteConstants.${Type}
				.getSize())
				+ Index.ID_SIZE;
		}



    /**
     * Insert the given bucket into the container.
		 * @param bucket Bucket to insert.
		 * @param object the bucket has an object.
		 * @return The result of the operation.
     */
    public OperationStatus insert(B bucket, O object) throws OBException,
            IllegalIdException, IllegalAccessException, InstantiationException {
               
				OperationStatus res = exists(bucket, object);
				
				if(res.getStatus() == Status.NOT_EXISTS){
						res = insertBulk(bucket, object);
				}
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
				    byte[] key2 = buildKey(key, bucket.getSmapVector()[this.secondaryIndexPivot]);
				    ByteBuffer out = ByteConversion
								.createByteBuffer(calculateBufferSize(getPivots()));
						OperationStatus res = new OperationStatus();
						res.setStatus(Status.OK);
						assert bucket.getId() != -1;
						res.setId(bucket.getId());
						bucket.write(out);
						storage.put(key2, out);
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
        return (TUPLE_SIZE);
    }



    @Override
    public OperationStatus exists(B bucket, O object)
				throws OBException, IllegalIdException, IllegalAccessException,
				InstantiationException {
				OperationStatus res = new OperationStatus();
        res.setStatus(Status.NOT_EXISTS);
				byte[] key2 = buildKey(key, bucket.getSmapVector()[this.secondaryIndexPivot]);
				CloseIterator<TupleBytes> pr = storage.processRange(key2,key2);
				try{
						B cmp = instantiateBucketObject();
						while(pr.hasNext()){
								TupleBytes t = pr.next();
								cmp.read(t.getValue(), getPivots());
								if(bucket.compareTo(cmp) == 0 && index.getObject(cmp.getId()).distance(object) == 0){						
										res.setStatus(Status.EXISTS);							
										res.setId(cmp.getId());
										break;
								}
						}
				}finally{
						pr.closeCursor();
				}
        return res;
    }

    
		public long search(OBQuery${Type} < O > query, B b,
											 IntegerHolder smapComputations,  IntegerHolder dataRead,  Filter<O> filter, ByteBuffer data) throws IllegalAccessException,
				OBException, InstantiationException, IllegalIdException {
				B current = instantiateBucketObject();
				current.read(data, getPivots());
				dataRead.add(data.array().length);
				smapComputations.inc();
				${type} max = current.lInf(b);
				long res = 0;
				if (max <= query.getDistance() && query.isCandidate(max)) {
										long id = current.getId();
										O toCompare = index.getObject(id);
										// Process query only if the filter is null.
										if(filter == null || filter.accept(toCompare, query.getObject())){
												${type} realDistance = query.getObject().distance(toCompare);
												res++;
												if (realDistance <= query.getDistance()) {
														query.add(id, toCompare, realDistance);
												}
										}
								}
				return res;
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
											 IntegerHolder smapComputations,  IntegerHolder dataRead, Filter<O> filter) throws IllegalAccessException,
				OBException, InstantiationException, IllegalIdException {
			  byte[] key1 = buildKey(key, query.getLow()[this.secondaryIndexPivot]);
				byte[] key2 = buildKey(key, query.getHigh()[this.secondaryIndexPivot]);
				CloseIterator<TupleBytes> pr = storage.processRange(key1,key2);
				long res = 0;
				try{
						B current = instantiateBucketObject();
						
						while(pr.hasNext()){
								TupleBytes t = pr.next();
								current.read(t.getValue(), getPivots());
								dataRead.add(t.getValue().array().length);
								${type} max = current.lInf(b);
								smapComputations.inc();
								if (max <= query.getDistance() && query.isCandidate(max)) {
										long id = current.getId();
										O toCompare = index.getObject(id);
										// Process query only if the filter is null.
										if(filter == null || filter.accept(toCompare, query.getObject())){
												${type} realDistance = query.getObject().distance(toCompare);
												res++;
												if (realDistance <= query.getDistance()) {
														query.add(id, toCompare, realDistance);
												}
										}
								}
								
						}
				}finally{
						pr.closeCursor();
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

		/**
		 * Return the # of S-Map vectors in the bucket.
		 */
		public int size() throws OBException{
					CloseIterator<TupleBytes> pr = storage.processRange(key,key);
				int res = 0;
				try{
						while(pr.hasNext()){
								TupleBytes t = pr.next();
								res++;
						}
						}finally{
								pr.closeCursor();
						}
						return res;
		}


}
</#list>
