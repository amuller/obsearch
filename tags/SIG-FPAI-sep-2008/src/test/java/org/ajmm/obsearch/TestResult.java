package org.ajmm.obsearch;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

public class TestResult {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testSetId() {
        Result a = new Result(Result.Status.OK);
        a.setId(3);
        Result b = new Result(Result.Status.OK);
        b.setId(4);
        assertEquals(a.getId(),3);
        assertEquals(b.getId(),4);
    }


}
