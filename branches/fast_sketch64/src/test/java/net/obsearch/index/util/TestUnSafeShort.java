package net.obsearch.index.util;

import static org.junit.Assert.*;

import java.lang.reflect.Field;

import net.obsearch.utils.UnsafeArrayHandlerShort;
import org.junit.Before;
import org.junit.Test;

import sun.misc.Unsafe;

public class TestUnSafeShort {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testShort() throws Exception {
        UnsafeArrayHandlerShort us = new UnsafeArrayHandlerShort();
        byte [] data = new byte[us.size * 5];
        us.putshort(data, 0, (short)-1);
        us.putshort(data, 1, (short)-2);
        us.putshort(data, 2, (short)0);
        us.putshort(data, 3, (short)20);
        us.putshort(data, 4, (short)2537);
        
        assertEquals(us.getshort(data, 0), (short)-1);
        assertEquals(us.getshort(data, 1), (short)-2);
        assertEquals(us.getshort(data, 2), (short)0);
        assertEquals(us.getshort(data, 3), (short)20);
        assertEquals(us.getshort(data, 4), (short)2537);
    }

    

}
