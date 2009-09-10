<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#include "/@inc/index.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="DistPerm${Type}.java" />
package net.obsearch.index.perm.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.filter.Filter;
import net.obsearch.filter.FilterNonEquals;
import net.obsearch.index.Commons${Type};
import net.obsearch.index.Index${Type};
import net.obsearch.index.bucket.impl.BucketObject${Type};
import net.obsearch.index.bucket.sleek.SleekBucket${Type};
import net.obsearch.index.perm.AbstractDistPerm;
import net.obsearch.index.perm.PermProjection;
import net.obsearch.ob.OB${Type};
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.query.OBQuery${Type};
import net.obsearch.result.OBPriorityQueue${Type};

public class DistPerm${Type}<O extends OB${Type}> extends AbstractDistPerm<O, BucketObject${Type}<O>, OBQuery${Type}<O>, SleekBucket${Type}<O>> implements Index${Type}<O> {

	
	static final transient Logger logger = Logger
	.getLogger(DistPerm${Type}.class.getName());
	
	public DistPerm${Type}(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount,
			int bucketPivotCount) throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount, bucketPivotCount);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected PermProjection calculateDistance(PermProjection query,
			short[] proj) {
		return query.distance(proj);
	}

	@Override
	protected int getCPSize() {
		return ByteConstants.Short.getSize() * getPivotCount();
	}

	@Override
	protected AbstractOBQuery<O> getKQuery(O object, int k) throws OBException,
			InstantiationException, IllegalAccessException {
		
		BucketObject${Type}<O> b = getBucket(object);
		OBPriorityQueue${Type}<O> result = new OBPriorityQueue${Type}<O>(k);
		OBQuery${Type}<O> q = new OBQuery${Type}<O>(object, ${ClassType}.MAX_VALUE, result, b
				.getSmapVector());
		return q;
		
	}

	@Override
	protected Class<short[]> getPInstance() {
		return (Class<short[]>) array.getClass();
	}
	
	private static short[] array = new short[]{};

	@Override
	protected PermProjection getProjection(BucketObject${Type}<O> b)
			throws OBException {
		short i = 0;
		ArrayList<Per> s = new ArrayList<Per>(getPivotCount());
		for(O o : pivots){
			s.add(new Per(o.distance(b.getObject()) , i));
			i++;
		}
		Collections.sort(s);
		short[] order = new short[getPivotCount()];
		i = 0;
		for(Per p : s){
			order[i] = p.getId();
			i++;
		}
		return new PermProjection(order, -1);
	}
	
	private class Per implements Comparable<Per>{
		private ${type} distance;
		private short id;
		public Per(${type} distance, short id) {
			super();
			this.distance = distance;
			this.id = id;
		}
		@Override
		public int compareTo(Per o) {
			if(distance < o.distance){
				return -1;
			}else if(distance > o.distance){
				return 1;
			}else{
				if(id < o.id){
					return -1;
				}else if(id > o.id){
					return 1;
				}else{
					return 0;
				}
			}
		}
		public ${type} getDistance() {
			return distance;
		}
		public void setDistance(${type} distance) {
			this.distance = distance;
		}
		public short getId() {
			return id;
		}
		public void setId(short id) {
			this.id = id;
		}
		
		
		
	}
	
	
	/**
	 * This method returns a list of all the distances of the query against  the DB.
	 * This helps to calculate EP values in a cheaper way. results that are equal to the original object are added
	 * as ${Type}.MAX_VALUE
	 * @param query
	 * @param filterSame if True we do not return objects o such that query.equals(o)
	 * @return
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
	public ${type}[] fullMatchLite(O query, boolean filterSame) throws OBException, IllegalAccessException, InstantiationException{
			return Commons${Type}.fullMatchLite((OB${Type})query, filterSame, this);
	}

	@Override
	protected void maxKEstimationAux(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		
		// we calculate first the list of elements against the DB.
		BucketObject${Type}<O> b = getBucket(object);
		PermProjection ${type}Addr = getProjection(b);
		byte[] addr = ${type}Addr.getAddress();
		
		

		${type} [] sortedList = fullMatchLite( object, true);
		//`calculate the AVG distance between the sample and the DB.
		for(${type} t : sortedList){
				getStats().addExtraStats("GENERAL_DISTANCE", t);
		}
		// we now calculate the buckets and we sort them
		// according to the distance of the query.
		
	
		loadMasks();
	
		long time = System.currentTimeMillis();
		OBAsserts.chkAssert(Buckets.size() <= Integer.MAX_VALUE, "Capacity exceeded");
		List<PermProjection> sortedBuckets = searchBuckets(${type}Addr, (int)Buckets.size());
		
		//List<OBResult${Type}<O>> sortedBuckets2 = fullMatch(object);
		logger.info("Time searching sketches: " + (System.currentTimeMillis() - time));

		// now we have to calculate the EP for each k up to maxK
		// and for each k we calculate how many buckets must be read in order
		// to obtain ep less than
		int i = 0;
		// container
		// used to
		// read
		// data.
		FilterNonEquals<O> fne = new FilterNonEquals<O>();
		while (i < getMaxK().length) {
			double ep = 1;
			int goodK = 0;
			// get the query for the
			AbstractOBQuery<O> queryAbst = getKQuery(object, userK[i]);
			OBQuery${Type}<O> query = (OBQuery${Type}<O>) queryAbst;
			
			for (PermProjection result : sortedBuckets) {
				
				SleekBucket${Type}<O> container = this.bucketCache.get(result.getAddress());
				// search the objects
				assert container != null : "Problem while loading bucket " ;
				container.search(query, b, fne, getStats());
				// calculate the ep of the query and the DB.
				if (query.isFull()) { // only if the query is full of items.
					ep = query.ep(sortedList);
					//double epOld = query.ep((List)sortedBuckets2);
					//OBAsserts.chkAssert(Math.abs(ep - epOld)<= 1f / sortedList.length, "oops: new: " + ep + " old: " + epOld);					
				}
				goodK++;
				if (ep <= this.getExpectedEP()) {
					// add the information to the stats:
					// goodK buckets required to retrieve with k==i.
						logger.info("Found result after reading: " + goodK + " buckets ");
					kEstimators[i].add(goodK);
					// store the distance of the best object and the real-best object
					${type} difference = (${type})Math.abs(sortedList[0] - query.getResult().getSortedElements().get(0).getDistance());

					break; // we are done.
				}
			}
			i++;
		}
		
	}
	
	@Override
	public BucketObject${Type}<O> getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		BucketObject${Type}<O> res = new BucketObject${Type}<O>(null, -1L, object);
		return res;
	}

	@Override
	protected SleekBucket${Type}<O> instantiateBucketContainer(
			byte[] data, byte[] address) throws InstantiationException, IllegalAccessException, OBException {
		if(data == null){
			return new SleekBucket${Type}<O>( type, bucketPivotCount);
		}else{
			try{
				return new SleekBucket${Type}<O>( type, bucketPivotCount, data);
			}catch(IOException e){
				throw new OBException(e);
			}
		
		}
	}

	
	@Override
	protected int primitiveDataTypeSize() {
		return ByteConstants.${Type}.getSize();
	}
	
	
	@Override
	public void searchOB(O object, ${type} r, Filter<O> filter,
			OBPriorityQueue${Type}<O> result) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		BucketObject${Type}<O> b = getBucket(object);

		OBQuery${Type}<O> q = new OBQuery${Type}<O>(object, r, result, b
				.getSmapVector());
		PermProjection query = this.getProjection(b);
		
		// we only use the k estimation once, at the beginning of the search process.
		int kEstimation = estimateK(result.getK());
		stats.addExtraStats("K-estimation", kEstimation);
		long time = System.currentTimeMillis();
		List<PermProjection> sortedBuckets = searchBuckets(query, kEstimation);
		getStats().addExtraStats("Buckets_search_time", System.currentTimeMillis() - time);
		for(PermProjection bucket: sortedBuckets){
				SleekBucket${Type}<O> container = this.bucketCache.get(bucket.getAddress());
			stats.incBucketsRead();
			container.search(q, b, filter, getStats());															
		}
	}
	
	@Override
	public void searchOB(O object, ${type} r, OBPriorityQueue${Type}<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		searchOB(object, r,null, result);
		
				
	}

}
</#list>