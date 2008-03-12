package org.ajmm.obsearch.storage.bdb;

import java.io.File;

import junit.framework.TestCase;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.storage.StorageTester;
import org.junit.Before;
import org.junit.Test;

public class TestBDBStorageShort extends TestCase{

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testAll() throws Exception{
        
        BDBFactory fact = Utils.getFactory();
        StorageTester.ShortStorageValidation(fact.createOBStoreShort("test", false));
    }

}
