package net.obsearch.index.dprime.impl;

import static org.junit.Assert.*;
import net.obsearch.index.dprime.BinaryTrie;

import org.junit.Test;


public class TestBinaryTrie {

	@Test
	public void testTrie(){
		BinaryTrie x = new BinaryTrie();
		
		x.add("01010");
		x.add("01011");
		assertTrue(x.containsPrefix("01010"));
		assertTrue(x.containsPrefix("01011"));
		assertTrue(x.containsPrefix("01"));
		assertTrue(x.containsPrefix("010"));
		assertTrue(! x.containsPrefix("011"));
		assertTrue(x.containsPrefix("0"));
		assertTrue(! x.containsPrefix("0100"));
		assertTrue(! x.containsPrefix("1"));
		assertTrue(! x.containsPrefix("00"));
		
		x.add("10101");
		assertTrue(x.containsPrefix("10101"));
		assertTrue(!x.containsPrefix("101010"));
		assertTrue(!x.containsPrefix("101011"));
		assertTrue(x.containsPrefix("10"));
		assertTrue( x.containsPrefix("101"));
		assertTrue(! x.containsPrefix("1000"));
		assertTrue(! x.containsPrefix("00"));
		
		x.add("101"); // adding a sub-prefix does not change
		              // anything
		assertTrue(x.containsPrefix("10101"));
		assertTrue(!x.containsPrefix("101010"));
		assertTrue(!x.containsPrefix("101011"));
		assertTrue(x.containsPrefix("10"));
		assertTrue( x.containsPrefix("101"));
		assertTrue(! x.containsPrefix("1000"));
		assertTrue(! x.containsPrefix("00"));
		
		System.out.println("Size: " + BinaryTrie.objectCount);
	}
	
}
