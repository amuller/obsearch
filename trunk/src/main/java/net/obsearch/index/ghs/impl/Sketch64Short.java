package net.obsearch.index.ghs.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import net.obsearch.AbstractOBResult;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.filter.Filter;
import net.obsearch.filter.FilterNonEquals;
import net.obsearch.index.IndexShort;
import net.obsearch.index.bucket.impl.BucketObjectShort;
import net.obsearch.index.bucket.sleek.SleekBucketShort;
import net.obsearch.index.ghs.AbstractSketch64;
import net.obsearch.ob.OBShort;
import net.obsearch.pivots.IncrementalPairPivotSelector;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.query.OBQueryShort;
import net.obsearch.result.OBPriorityQueueShort;
import net.obsearch.result.OBResultInvertedByte;
import net.obsearch.result.OBResultInvertedShort;
import net.obsearch.result.OBResultShort;

public class Sketch64Short<O extends OBShort> extends AbstractSketch64<O, BucketObjectShort<O>, OBQueryShort<O>, SleekBucketShort<O>>
implements IndexShort<O> {
	
	private boolean DEBUG = true;
	
	private static final transient Logger logger = Logger
	.getLogger(Sketch64Short.class.getName());
	
	/**
	 * Create a new Sketch64Short with m bytes. 
	 * @param type Type of object that will be stored
	 * @param pivotSelector Pivot selection strategy to be employed.
	 * @param m The number of bits
	 * @param bucketPivotCount Number of pivots per bucket
	 * @throws OBStorageException
	 * @throws OBException
	 * @throws IOException
	 */
	public Sketch64Short(Class<O> type,
			IncrementalPairPivotSelector<O> pivotSelector, int m, int bucketPivotCount
			)												
			throws OBStorageException, OBException, IOException {
		
		super(type, pivotSelector,  m, bucketPivotCount);
		
	}
	
	@Override
	public BucketObjectShort<O> getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		BucketObjectShort<O> res = new BucketObjectShort<O>(null, -1L, object);
		return res;
	}

	
	/**
	 * Compute the sketch for the given object.
	 */
	@Override
	public long getLongAddress(BucketObjectShort<O> bucket) throws OBException {
		int i = 0;
		long res = 0;
		while(i < m){
			if(pivotGrid[i][0].distance(bucket.getObject()) > pivotGrid[i][1].distance(bucket.getObject()) ){
				res = res | (1L << i);
				distortionStats[i][1]++;
			}else{
				distortionStats[i][0]++;
			}
			i++;
		}
		return res;
	}

	
	@Override
	protected SleekBucketShort<O> instantiateBucketContainer(
			byte[] data, byte[] address) throws InstantiationException, IllegalAccessException, OBException {
		if(data == null){
			return new SleekBucketShort<O>( type, bucketPivotCount);
		}else{
			try{
				return new SleekBucketShort<O>( type, bucketPivotCount, data);
			}catch(IOException e){
				throw new OBException(e);
			}
		
		}
	}
	
	@Override
	protected int primitiveDataTypeSize() {
		return ByteConstants.Short.getSize();
	}
	
	
	
	
	//TODO: raise this method one level up and change all the interfaces.
	@Override
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		searchOB(object, r,null, result);
		
				
	}
	
	@Override
	protected AbstractOBQuery<O> getKQuery(O object, int k) throws OBException, InstantiationException, IllegalAccessException {
		BucketObjectShort<O> b = getBucket(object);
		OBPriorityQueueShort<O> result = new OBPriorityQueueShort<O>(k);
		OBQueryShort<O> q = new OBQueryShort<O>(object, Short.MAX_VALUE, result, b
				.getSmapVector());
		return q;
	}

	private long [] searchBuckets(long query, int kEstimation, int m) throws OBException{
		if(sketchSet == null){
			loadMasks();
		}
		return sketchSet.searchBuckets(query, kEstimation, m);
	}
	

	@Override
	public void searchOB(O object, short r, Filter<O> filter,
			OBPriorityQueueShort<O> result) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		BucketObjectShort<O> b = getBucket(object);

		OBQueryShort<O> q = new OBQueryShort<O>(object, r, result, b
				.getSmapVector());
		long query = this.getLongAddress(b);
		byte[] addr = super.convertLongToBytesAddress(query);
		
		// we only use the k estimation once, at the beginning of the search process.
		int kEstimation = estimateK(result.getK());
		stats.addExtraStats("K-estimation", kEstimation);
		long time = System.currentTimeMillis();
		long [] sortedBuckets = searchBuckets(query, kEstimation, m);
		getStats().addExtraStats("Buckets_search_time", System.currentTimeMillis() - time);
		for(long bucket: sortedBuckets){
			SleekBucketShort<O> container = this.bucketCache.get(super.convertLongToBytesAddress(bucket));
			stats.incBucketsRead();
			container.search(q, b, filter, getStats());															
		}
	}
	
	/**
	 * Match the given query against all the DB. (for EP calculation)
	 * removes query from the end result.
	 * @return the results of matching the given query against all the DB.
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
	public List<OBResultShort<O>> fullMatch(O query) throws OBException, IllegalAccessException, InstantiationException{
		long max = databaseSize();
		List<OBResultShort<O>> sortedList = new ArrayList<OBResultShort<O>>((int)(max - 1));
		long id = 0;
		
		while(id < max){
			O o = getObject(id);
			short distance = o.distance(query);
			if(distance == 0 && o.equals(query)){
				id++;
				continue; // do not add self.
			}else{
				sortedList.add(new OBResultShort<O>(o, id, distance));
			}
			id++;
		}
		Collections.sort(sortedList);
		Collections.reverse(sortedList);
		return sortedList;
	}
	
	/**
	 * This method returns a list of all the distances of the query against  the DB.
	 * This helps to calculate EP values in a cheaper way. results that are equal to the original object are added
	 * as Short.MAX_VALUE
	 * @param query
	 * @param filterSame if True we do not return objects o such that query.equals(o)
	 * @return
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
	public short[] fullMatchLite(O query, boolean filterSame) throws OBException, IllegalAccessException, InstantiationException{
		long max = databaseSize();
		OBAsserts.chkAssert(max < Integer.MAX_VALUE, "Cannot exceed 2^32");
		short[] result = new short[(int)max];
		int id = 0;
		O o = type.newInstance();
		while(id < max){
			loadObject(id, o);
			short distance = o.distance(query);
			if(distance == 0 && o.equals(query) && filterSame){
				result[id] = Short.MAX_VALUE;				
			}else{
				result[id] = distance;
			}
			id++;
		}
		
		Arrays.sort(result);
		return result;		
	}
	
	
	/**
	 * Get the kMax closest objects. Count how many different bucket ids are
	 * there for each k and fill in accordingly the tables.
	 * 
	 * @param object
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected void maxKEstimationAux(O object)
			throws OBException, InstantiationException, IllegalAccessException {
		// we calculate first the list of elements against the DB.
		BucketObjectShort<O> b = getBucket(object);
		long longAddr = getLongAddress(b);
		byte[] addr = convertLongToBytesAddress(longAddr);
		
		

		short[] sortedList = fullMatchLite( object, true);
		
		// we now calculate the buckets and we sort them
		// according to the distance of the query.
		
		if(sketchSet == null){
			loadMasks();
		}
		long time = System.currentTimeMillis();
		List<OBResultInvertedByte<Long>> sortedBuckets = sketchSet
				.searchFull(longAddr);
		
		//List<OBResultShort<O>> sortedBuckets2 = fullMatch(object);
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
			OBQueryShort<O> query = (OBQueryShort<O>) queryAbst;
			
			for (OBResultInvertedByte<Long> result : sortedBuckets) {
				
				SleekBucketShort<O> container = this.bucketCache.get(this
						.convertLongToBytesAddress(result.getObject()));
				// search the objects
				assert container != null : "Problem while loading: " + result.getObject();
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
					kEstimators[i].add(goodK);
					break; // we are done.
				}
			}
			i++;
		}

	}
	
	
	
	
}
