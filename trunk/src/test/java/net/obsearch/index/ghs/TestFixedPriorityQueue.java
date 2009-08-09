package net.obsearch.index.ghs;

import static org.junit.Assert.*;


import it.unimi.dsi.fastutil.PriorityQueue;

import java.util.logging.Logger;


import net.obsearch.result.OBPriorityQueueInt;

import org.junit.Test;

import weka.core.Debug.Random;



public class TestFixedPriorityQueue {
	
	Logger logger = Logger.getLogger(TestFixedPriorityQueue.class.getName());
	@Test
	public void test() throws InstantiationException, IllegalAccessException{
		
		FixedPriorityQueue<Integer> q = new FixedPriorityQueue(4);
		
		
		Integer[] arrayFull = new Integer[]{10,11,12, 8,9,7,8,7,7};
		Integer[] array = new Integer[]{7,7,7,8};
		
		Integer[] arrayFull2 = new Integer[]{-1,6,15,20};
		Integer[] array2 = new Integer[]{-1,6, 7,7};
		
		FixedPriorityQueue<Integer> q2 = new FixedPriorityQueue<Integer>(4);
		
		
		q2.addAll(arrayFull);
		testEquiv(array, q2);
		
		
		q2.addAll(arrayFull2);
		
		testEquiv(array2, q2);
		
		
		Integer[] data = gen(10000000);
		// test
		long start = System.currentTimeMillis();
		FixedPriorityQueue<Integer> f = new FixedPriorityQueue<Integer>(2000);
		for(Integer i : data){
			f.add(i);
		}
		long time = System.currentTimeMillis() - start;
		logger.info("Total fixed2: " + time);
		
		OBPriorityQueueInt<Integer> f2 = new OBPriorityQueueInt<Integer>(2000);
		start = System.currentTimeMillis();
		for(Integer i : data){
			f2.add(i, i, i);
		}
		time = System.currentTimeMillis() - start;
		logger.info("Total Priority: " + time);
		
	
	}
	
	private Integer[] gen(int size){
		Integer[] d = new Integer[size];
		int i = 0;
		Random r = new Random();
		while(i < d.length){
			d[i] = r.nextInt();
			i++;
		}
		return d;
	}
	
	private void testEquiv(Integer[] d, FixedPriorityQueue<Integer> q){
		int i = 0;
		for(Integer in : q){
			assertEquals(d[i], in);
			i++;
		}
	}
	
	

}
