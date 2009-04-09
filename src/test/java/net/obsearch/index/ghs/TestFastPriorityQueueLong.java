package net.obsearch.index.ghs;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import org.junit.Test;

import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.je.DatabaseEntry;

import junit.framework.TestCase;

import net.obsearch.result.OBResultInt;
import net.obsearch.result.OBResultInvertedInt;
import static org.junit.Assert.*;

public class TestFastPriorityQueueLong extends TestCase {
	
	private Logger logger = Logger.getLogger(TestFastPriorityQueueLong.class.getName());
	
	private final int TOTAL = 10000;
	private final int MAX_DISTANCE = 64;
	private final int TOP = 1000;
	
	public void testQueue(){
		
		FastPriorityQueueLong queue = new FastPriorityQueueLong(MAX_DISTANCE, TOP);
		
		List<OBResultInvertedInt<Long>> result = new ArrayList<OBResultInvertedInt<Long>>(TOTAL);
		
		int[] counts = new int[MAX_DISTANCE + 1];
		Random r = new Random();
		int i = 0;
		while(i < TOTAL){			
			int distance = r.nextInt(MAX_DISTANCE + 1); 
			queue.add(distance, distance);
			result.add(new OBResultInvertedInt(distance,distance,distance));
			i++;
		}
		Collections.sort(result);
		Iterator<OBResultInvertedInt<Long>> it = result.iterator();
		i = 0;
		long [] data =queue.get();
		for(long l : data){
			if(i >= TOP){
				break;
			}
			OBResultInvertedInt<Long> e = it.next();
			assertTrue( "Found: " + e.getDistance() + " but fast got: " + l, e.getDistance() == l);
			i++;
		}
	}
	
	
	public void testTupleInput(){
		DatabaseEntry entry = new DatabaseEntry();
		LongBinding.longToEntry(3L, entry);		
		logger.info("Size: " + entry.getData().length);
	}

}
