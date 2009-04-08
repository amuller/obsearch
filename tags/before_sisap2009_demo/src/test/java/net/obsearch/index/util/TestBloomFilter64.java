package net.obsearch.index.util;

import static org.junit.Assert.*;
import net.obsearch.utils.BloomFilter64bit;

import org.junit.Test;


public class TestBloomFilter64 {
	
	 @Test
	 public void testBloom(){
		 BloomFilter64bit b = new BloomFilter64bit(5, 64);
		 b.add(23L);
		 assertTrue(b.contains(23L));
		 assertTrue(! b.contains(32L));
		 b.add(58L);
		 assertTrue(b.contains(58L));
		 assertTrue(! b.contains(57L));
	 }

}
