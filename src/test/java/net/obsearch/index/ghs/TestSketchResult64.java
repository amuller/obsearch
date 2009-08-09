package net.obsearch.index.ghs;

import static org.junit.Assert.*;

import org.junit.Test;


public class TestSketchResult64 {
	
	@Test
	public void test(){
		SketchProjection s1 = new SketchProjection(new byte[]{9,8,5,1}, 1);
		SketchProjection s2 = new SketchProjection(new byte[]{7,6,5,4}, 2);
		
		assertTrue(s1.compareTo(s2) < 0);	
		
		
	}
	
	@Test
	public void test2(){
		SketchProjection s1 = new SketchProjection(new byte[]{10,4,2,1}, 1);
		SketchProjection s2 = new SketchProjection(new byte[]{5,4,3,2}, 2);
		
		assertTrue(s1.compareTo(s2) < 0);	
		
		
	}
	
	@Test
	public void test3(){
		SketchProjection s1 = new SketchProjection(new byte[]{10,4,2,1}, 1);
		SketchProjection s2 = new SketchProjection(new byte[]{5,4,2,1}, 2);
		
		assertTrue(s1.compareTo(s2) < 0);	
		
		
	}
	
	@Test
	public void test4(){
		SketchProjection s1 = new SketchProjection(new byte[]{10,4,2,1}, 1);
		SketchProjection s2 = new SketchProjection(new byte[]{30,4,2,1}, 2);
		
		assertTrue(s1.compareTo(s2) > 0);	
		
		
	}
	
	@Test
	public void test5(){
		SketchProjection s1 = new SketchProjection(new byte[]{10,4,2,1}, 1);
		SketchProjection s2 = new SketchProjection(new byte[]{7,6,3,1}, 2);
		
		assertTrue(s1.compareTo(s2) == 0);	
		
		
	}
	

}
