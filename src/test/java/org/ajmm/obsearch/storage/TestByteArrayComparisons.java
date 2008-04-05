package org.ajmm.obsearch.storage;

import static org.junit.Assert.*;

import org.ajmm.obsearch.index.utils.ByteArrayComparator;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.TupleOutput;

public class TestByteArrayComparisons {

    @Before
    public void setUp() throws Exception {
    }
    
    @Test
    public void testCase(){
        ByteArrayComparator c = new ByteArrayComparator();
        assertEquals(c.compare(createTuple(10000L,(short)2), createTuple(10000,(short)2)), 0);
        assertEquals(c.compare(createTuple(10000L,(short)2), createTuple(10001,(short)2)), -1);
        assertEquals(c.compare(createTuple(10001L,(short)2), createTuple(-10000,(short)2)), 1);
        assertEquals(c.compare(createTuple(10000L,(short)2), createTuple(10000,(short)3)), -1);        
        assertEquals(c.compare(createTuple(10000L,(short)3), createTuple(10000,(short)2)), 1);
        
        assertEquals(c.compare(createTuple2(10000.1,2.35f), createTuple2(10000.1,2.35f)), 0);
        assertEquals(c.compare(createTuple2(0.35,2.35f), createTuple2(10000.1,2.35f)), -1);
        assertEquals(c.compare(createTuple2(10000.1,2.35f), createTuple2(-0.35,2.35f)), 1);
        assertEquals(c.compare(createTuple2(10000.1,-2.34f), createTuple2(10000.1,2.35f)), -1);
        assertEquals(c.compare(createTuple2(10000.1,213323.56f), createTuple2(10000.1,-2.35f)), 1);
    }
    
    

    public byte[] createTuple(long a, short dist){
        TupleOutput out = new TupleOutput();
        out.writeLong(a);
        out.writeShort(dist);
        
        return out.getBufferBytes();
    }
    
    public byte[] createTuple2(double a, float dist){
        TupleOutput out = new TupleOutput();
        out.writeSortedDouble(a);
        out.writeSortedFloat(dist);
        return out.getBufferBytes();
    }
}
