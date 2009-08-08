package net.obsearch.index.ghs;


import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.logging.Logger;

import net.obsearch.exception.OBException;


import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.DatabaseEntry;

import static org.junit.Assert.*;

public class TestCompressedBitSet {
	
	private Logger logger = Logger.getLogger(TestCompressedBitSet.class.getName());
	
	private final int TOTAL_QUERIES = 100;
	private final int TOTAL_OBJECTS = 1000000;
	private final int M = 64;
	private final int F = 1000;
	private Random r = new Random();

	@Before
	public void setUp() throws Exception {
	}
	
	@Test
	public void testBitSet() throws OBException, InstantiationException, IllegalAccessException, IOException{		
		int i = 0;
		while(i < TOTAL_QUERIES){
			testQuery(nextLong());
			i++;
		}
	}
	
	private long nextLong(){
		return r.nextLong();
	}
	
	private void testQuery(long q) throws OBException, InstantiationException, IllegalAccessException, IOException{
		//assertTrue(q >= 0);
		long[] data = new long[TOTAL_OBJECTS];
		int i = 0;
		while(i < data.length){
			data[i] = nextLong();
			//assertTrue(data[i] >=0);
			i++;
		}
		Arrays.sort(data);
		//
		CompressedBitSet64 bitSet = new CompressedBitSet64();
		FastPriorityQueueLong base = new FastPriorityQueueLong(M, F);
		for(long o : data){
			int distance = bitSet.bucketDistance(q, o);
			base.add(o, distance);
		}
		
		// now we do the same for the compressedbitset.
		// first we add the data to the bitSet
		for(long o : data){
			bitSet.add(o);
		}
		
		bitSet.commit();
		logger.info("Bitset created: " + bitSet.getBytesSize());
		
		long [] result = bitSet.searchBuckets(q, F, M);
		long [] baseArray = base.get();
		// results arrays must be the same.
		assertTrue(Arrays.equals(result, baseArray));
		
		assertTrue(Arrays.equals(data, bitSet.getAll()));
	}
	
	

}
