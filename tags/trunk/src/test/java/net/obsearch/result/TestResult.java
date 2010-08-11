package net.obsearch.result;

import static org.junit.Assert.*;

import net.obsearch.OperationStatus;
import net.obsearch.Status;

import org.junit.Before;
import org.junit.Test;

public class TestResult {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testSetId() {
        OperationStatus a = new OperationStatus(Status.OK);
        a.setId(3);
        OperationStatus b = new OperationStatus(Status.OK);
        b.setId(4);
        assertEquals(a.getId(),3L);
        assertEquals(b.getId(),4L);
    }


}
