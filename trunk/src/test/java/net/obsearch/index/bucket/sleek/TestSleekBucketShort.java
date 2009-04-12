package net.obsearch.index.bucket.sleek;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.example.l1.L1;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.index.bucket.impl.BucketObjectShort;

import static org.junit.Assert.*;

import org.junit.Test;

/**
 * Test the SleekBucketShort
 * 
 * @author amuller
 * 
 */
public class TestSleekBucketShort {

	private static final transient Logger logger = Logger
			.getLogger(TestSleekBucketShort.class.getName());

	private Random r = new Random();

	private final int TOTAL_DIMS = 64;
	private final int MAX_DIM_VALUE = 100;
	// max objects to store in a bucket
	private final int MAX_OBJECTS = 1000;
	// max # of pivots
	private final int MAX_PIVOTS = 100;

	public L1 generateObject() {
		short[] data = new short[TOTAL_DIMS];
		int i = 0;
		while (i < data.length) {
			data[i] = (short) r.nextInt(MAX_DIM_VALUE);
			i++;
		}
		return new L1(data);
	}
	
	@Test
	public void testBucket() throws IllegalIdException, OBException, IllegalAccessException, InstantiationException, IOException{
		// do the test several times with different configurations.
		int i = 0;
		while(i < 1000){
			testBucketAux(r.nextInt(MAX_OBJECTS), r.nextInt(MAX_PIVOTS));
			i++;
		}
	}

	public void testBucketAux(int maxObjects, int maxPivots)
			throws IllegalIdException, OBException, IllegalAccessException,
			InstantiationException, IOException {
		logger.info("Testing with objects" + maxObjects + " max pivots: "
				+ maxPivots);
		// create some random buckets and insert them into the Bucket container
		SleekBucketShort<L1> bucket = new SleekBucketShort<L1>(L1.class,
				maxPivots);
		int i = 0;
		List<L1> objects = new ArrayList<L1>(maxObjects);
		List<BucketObjectShort<L1>> buckets = new ArrayList<BucketObjectShort<L1>>(maxObjects);
		while (i < maxObjects) {
			L1 o = generateObject();
			if(! objects.contains(o)){	
				BucketObjectShort<L1> b = new BucketObjectShort<L1>(null, i, o);
				bucket.insert(b, o);
				objects.add(o);
				buckets.add(b);
				i++;
			}
		}
		// serialize and de-serialize the bucket, objects should be the same.
		byte[] data = bucket.serialize();
		SleekBucketShort<L1> deSerializedBucket = new SleekBucketShort<L1>(
				L1.class, maxPivots, data);
		assertEquals(bucket, deSerializedBucket);
		
		testUpdateOperations(buckets, objects, bucket);
		testUpdateOperations(buckets, objects, deSerializedBucket);
		// end result should be the same.
		assertEquals(bucket, deSerializedBucket);
		
	}
	
	private void testUpdateOperations(List<BucketObjectShort<L1>> buckets, List<L1> objects,  SleekBucketShort<L1> bucket) throws IllegalIdException, OBException, IllegalAccessException, InstantiationException{
		// check size.		
		assertEquals(bucket.size(), objects.size());
		
		// test exist methods.
		for(BucketObjectShort<L1> o : buckets){
			// all objects should be inside.
			assertTrue(bucket.exists(null, o.getObject()).getStatus() == Status.EXISTS);
			assertTrue(bucket.exists(null, o.getObject()).getId() == o.getId());
		}
		// exist should return false if the object is not available
		int i = 0;
		while(i < 100){
			L1 o = generateObject();
			if(! objects.contains(o)){
				assertTrue(bucket.exists(null, o).getStatus() == Status.NOT_EXISTS);
				assertTrue(bucket.exists(null, o).getId() == -1);
				i++;
			}
		}
		
		// try deletes
		for(BucketObjectShort<L1> b : buckets){
			// all objects should be inside.
			OperationStatus res = bucket.delete(b, b.getObject());
			assertTrue(res.getStatus() == Status.OK);
			assertTrue(res.getId() == b.getId());
		}
		assertEquals(bucket.size(), 0);
		
		// deletes should return something empty
		
		// try deletes
		for(L1 o : objects){
			// all objects should be inside.
			assertTrue(bucket.delete(null, o).getStatus() == Status.NOT_EXISTS);
			assertTrue(bucket.delete(null, o).getId() == -1);
		}
		assertEquals(bucket.size(), 0);
		
		// re-insert the objects:
		for(BucketObjectShort<L1> b : buckets){	
			// all objects should be inside.
			OperationStatus res = bucket.insert(b, b.getObject());
			assertTrue(res.getStatus() == Status.OK);
			assertTrue(res.getId() == b.getId());
		}
		assertEquals(bucket.size(), objects.size());
		
		// try to re-insert the objects should fail.
		for(BucketObjectShort<L1> b : buckets){		
			// all objects should be inside.
			OperationStatus res = bucket.insert(b, b.getObject());
			assertTrue(res.getStatus() == Status.EXISTS);
			assertTrue(res.getId() == b.getId());
		}
		
		// check size.		
		assertEquals(bucket.size(), objects.size());
		
	}

}
