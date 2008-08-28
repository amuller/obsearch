package org.ajmm.obsearch.index;

import static org.junit.Assert.*;

import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.bind.tuple.TupleOutput;

public class TestMyTupleInput {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testReadShortFast() {
        
        short[] arr = {2,78,Short.MAX_VALUE, Short.MIN_VALUE, 0,-6988, -1000};
        TupleOutput out = new TupleOutput();
        for( short x : arr){
            out.writeShort(x);
        }
        out.writeInt(23);
        MyTupleInput in = new MyTupleInput();
        in.setBuffer(out.getBufferBytes());
        for(short x : arr){
            assertEquals(x, in.readShortFast());
        }
        assertEquals(in.readInt(), 23);
    }

}
