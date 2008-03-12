package org.ajmm.obsearch.storage.bdb;

import java.io.File;

import junit.framework.TestCase;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.index.utils.Directory;

public class Utils extends TestCase{

    public static BDBFactory getFactory() throws Exception{
        File dbFolder = new File(TUtils.getTestProperties().getProperty(
                "test.db.path"));
        Directory.deleteDirectory(dbFolder);
        assertTrue(!dbFolder.exists());
        assertTrue(dbFolder.mkdirs());
        BDBFactory fact = new BDBFactory(dbFolder);
        return fact;
    }

}
