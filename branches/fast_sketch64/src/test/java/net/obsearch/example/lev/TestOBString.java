package net.obsearch.example.lev;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;

import org.junit.Test;

import net.obsearch.exception.OBException;


public class TestOBString {
	
	@Test
	public void testDistance() throws OBException, UnsupportedEncodingException{
		OBString a = new OBString("abcde");
		OBString b = new OBString("abxde");
		assertEquals(a.distance(b), (short)1);
		
		OBString a2 = new OBString("abcdejklsel");
		OBString b2 = new OBString("abxdek");
		assertEquals(a2.distance(b2), (short)6);
	}

}
