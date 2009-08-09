package net.obsearch.index.ghs;

import static org.junit.Assert.*;

import java.util.ArrayList;

import org.junit.Test;


public class TestSketchPriorityQueue {

	@Test
	public void test1(){
		
		SketchPriorityQueue q = new SketchPriorityQueue(5, 2);
		
		q.add(new SketchProjection(new byte[]{8,7 }, 1), 2);
		q.add(new SketchProjection(new byte[]{10,9 }, 2), 2);
		q.add(new SketchProjection(new byte[]{3,2 }, 3), 2);
		
		
		ArrayList<SketchProjection> r = q.getResults();
		assertEquals(2L, r.get(0).getSketch());
		assertEquals(1L, r.get(1).getSketch());
		
		// add another object.		
		q.add(new SketchProjection(new byte[]{8,7 }, 4), 1);		
		r = q.getResults();
		assertEquals(4L, r.get(0).getSketch());
		assertEquals(2L, r.get(1).getSketch());
		
		
	}
	
}
