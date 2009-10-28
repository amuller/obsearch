package net.obsearch.index.permprefix.impl;

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
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
import net.obsearch.index.CommonsFloat;
import net.obsearch.index.IndexFloat;
import net.obsearch.index.bucket.impl.BucketObjectFloat;
import net.obsearch.index.bucket.sleek.SleekBucketFloat;
import net.obsearch.index.perm.AbstractDistPerm;
import net.obsearch.index.perm.PermProjection;
import net.obsearch.ob.OBFloat;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.result.OBPriorityQueueFloat;
import net.obsearch.index.perm.CompactPerm;
import net.obsearch.index.perm.impl.PerFloat;
import net.obsearch.index.permprefix.AbstractDistPermPrefix;
import net.obsearch.index.permprefix.CompactPermPrefix;
import net.obsearch.index.permprefix.PermPrefixProjection;

public class DistPermPrefixFloat<O extends OBFloat> extends AbstractDistPermPrefix<O, BucketObjectFloat<O>, OBQueryFloat<O>, SleekBucketFloat<O>> implements IndexFloat<O> {

	
	static final transient Logger logger = Logger
	.getLogger(DistPermPrefixFloat.class.getName());
	
	
	
	public DistPermPrefixFloat(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount,
			int bucketPivotCount, int prefixSize) throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount, bucketPivotCount, prefixSize);
	}

	@Override
	protected PermPrefixProjection calculateDistance(PermPrefixProjection query,
			CompactPermPrefix proj) {
		return query.distance(proj);
	}

	

	@Override
	protected AbstractOBQuery<O> getKQuery(O object, int k) throws OBException,
			InstantiationException, IllegalAccessException {
		
		BucketObjectFloat<O> b = getBucket(object);
		OBPriorityQueueFloat<O> result = new OBPriorityQueueFloat<O>(k);
		OBQueryFloat<O> q = new OBQueryFloat<O>(object, Float.MAX_VALUE, result, b
				.getSmapVector());
		return q;
		
	}

	@Override
	protected Class<CompactPermPrefix> getPInstance() {
			return CompactPermPrefix.class;
	}
	


	@Override
	protected PermPrefixProjection getProjection(BucketObjectFloat<O> b)
			throws OBException {
		short i = 0;
		ArrayList<PerFloat> s = new ArrayList<PerFloat>(getPivotCount());
		for(O o : pivots){
				s.add(new PerFloat(o.distance(b.getObject()) , i));
			i++;
		}
		Collections.sort(s);
		int [] cache = new int[getPivotCount()];
		i = 0;
		while(i < getPivotCount()){
			PerFloat p = s.get(i);
			cache[p.getId()] = i;
			i++;
		}
		
		short[] order = new short[getPrefixSize()];
		i = 0;
		while(i < getPrefixSize()){
			order[i] = s.get(i).getId();
			i++;
		}
		Arrays.sort(order);
		return new PermPrefixProjection(new CompactPermPrefix(order), -1, cache);
	}
	
	
	
	
	@Override
	protected double distance(O a2, O b) throws OBException {
		return a2.distance(b);
	}

	/**
	 * This method returns a list of all the distances of the query against  the DB.
	 * This helps to calculate EP values in a cheaper way. results that are equal to the original object are added
	 * as Float.MAX_VALUE
	 * @param query
	 * @param filterSame if True we do not return objects o such that query.equals(o)
	 * @return
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
	public float[] fullMatchLite(O query, boolean filterSame) throws OBException, IllegalAccessException, InstantiationException{
			return CommonsFloat.fullMatchLite((OBFloat)query, filterSame, this);
	}

	@Override
	protected void maxKEstimationAux(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		// TODO Auto-generated method stub
		
		// we calculate first the list of elements against the DB.
		BucketObjectFloat<O> b = getBucket(object);
		PermPrefixProjection floatAddr = getProjection(b);
		byte[] addr = floatAddr.getAddress();
		
		

		float [] sortedList = fullMatchLite( object, true);
		//`calculate the AVG distance between the sample and the DB.
		for(float t : sortedList){
				getStats().addExtraStats("GENERAL_DISTANCE", t);
		}
		// we now calculate the buckets and we sort them
		// according to the distance of the query.
		
	
		loadMasks();
	
		long time = System.currentTimeMillis();
		OBAsserts.chkAssert(Buckets.size() <= Integer.MAX_VALUE, "Capacity exceeded");
		
		List<PermPrefixProjection> sortedBuckets = searchBuckets(floatAddr, this.projections.size());
		StringBuilder bu = new StringBuilder();
		SortedMap<Integer, Integer> m = new TreeMap<Integer, Integer>();
		int cx = 0;
		for(PermPrefixProjection p : sortedBuckets){
			Integer f = m.get(p.getDistance());
			if(f == null){
				f = 0;
			}
			m.put(p.getDistance(), f+1);
			if(cx + 1 < sortedBuckets.size()){
				stats.addExtraStats("LOL", p.distance(sortedBuckets.get(cx + 1).getCompactRepresentation()).getDistance());
			}
			cx++;
		}
		
		for(Map.Entry<Integer,Integer> e : m.entrySet()){
			bu.append(e.getKey());
			bu.append(", ");
			bu.append(e.getValue());
			stats.addExtraStats("LARGO_EXTRA", e.getValue());
			bu.append(" | ");
		}
		//logger.info(bu.toString());
		
		stats.addExtraStats("LARGO", m.size());
		//List<OBResultFloat<O>> sortedBuckets2 = fullMatch(object);
		logger.info("Time searching projections " + sortedBuckets.size() +  " : " + (System.currentTimeMillis() - time));

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
			double compound = 0;
			int goodK = 0;
			// get the query for the
			AbstractOBQuery<O> queryAbst = getKQuery(object, userK[i]);
			OBQueryFloat<O> query = (OBQueryFloat<O>) queryAbst;
			
			for (PermPrefixProjection result : sortedBuckets) {
				
				SleekBucketFloat<O> container = this.bucketCache.get(result.getAddress());
				// search the objects
				assert container != null : "Problem while loading bucket " ;
				container.search(query, b, fne, getStats());
				// calculate the ep of the query and the DB.
				if (query.isFull()) { // only if the query is full of items.
					compound = query.compound(sortedList);
					//double epOld = query.ep((List)sortedBuckets2);
					//OBAsserts.chkAssert(Math.abs(ep - epOld)<= 1f / sortedList.length, "oops: new: " + ep + " old: " + epOld);					
				}
				goodK++;
				if (compound >= this.getExpectedEP()) {
					// add the information to the stats:
					// goodK buckets required to retrieve with k==i.
						logger.info("Found result after reading: " + goodK + " buckets " + " dist: " + result.getDistance());
					stats.addExtraStats("DIFF", result.getMaxDiff());
					kEstimators[i].add(goodK);
					// add dists:
					for(int j : result.getDistances()){
						stats.addExtraStats("FULL_DIFF", j);
					}
					// store the distance of the best object and the real-best object
					//float difference = (float)Math.abs(sortedList[0] - query.getResult().getSortedElements().get(0).getDistance());
					break; // we are done.
				}
			}
			
			i++;
		}
		
		
		
	}
	
	@Override
	public BucketObjectFloat<O> getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		BucketObjectFloat<O> res = new BucketObjectFloat<O>(null, -1L, object);
		return res;
	}

	@Override
	protected SleekBucketFloat<O> instantiateBucketContainer(
			byte[] data, byte[] address) throws InstantiationException, IllegalAccessException, OBException {
		if(data == null){
			return new SleekBucketFloat<O>( type, bucketPivotCount);
		}else{
			try{
				return new SleekBucketFloat<O>( type, bucketPivotCount, data);
			}catch(IOException e){
				throw new OBException(e);
			}
		
		}
	}

	
	@Override
	protected int primitiveDataTypeSize() {
		return ByteConstants.Float.getSize();
	}
	
	
	@Override
	public void searchOB(O object, float r, Filter<O> filter,
			OBPriorityQueueFloat<O> result) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		BucketObjectFloat<O> b = getBucket(object);

		OBQueryFloat<O> q = new OBQueryFloat<O>(object, r, result, b
				.getSmapVector());
		PermPrefixProjection query = this.getProjection(b);
		
		// we only use the k estimation once, at the beginning of the search process.
		int kEstimation = estimateK(result.getK());
		//int kEstimation = 100;
		stats.addExtraStats("K-estimation", kEstimation);
		long time = System.currentTimeMillis();
		List<PermPrefixProjection> sortedBuckets = searchBuckets(query, kEstimation);
		logger.info("Distance: " + sortedBuckets.get(0) + " kest: " + kEstimation);
		getStats().addExtraStats("Buckets_search_time", System.currentTimeMillis() - time);
		for(PermPrefixProjection bucket: sortedBuckets){
				SleekBucketFloat<O> container = this.bucketCache.get(bucket.getAddress());
			stats.incBucketsRead();
			container.search(q, b, filter, getStats());															
		}
	}
	
	@Override
	public void searchOB(O object, float r, OBPriorityQueueFloat<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		searchOB(object, r,null, result);
		
				
	}



}

