package net.obsearch.storage.bdb;

import java.io.File;


import junit.framework.TestCase;

import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.TUtils;

public class Utils extends TestCase{

    public static BDBFactoryJe getFactoryJe() throws Exception{
        File dbFolder = new File(TUtils.getTestProperties().getProperty(
                "test.db.path"));
        Directory.deleteDirectory(dbFolder);
        assertTrue(!dbFolder.exists());
        assertTrue(dbFolder.mkdirs());
        BDBFactoryJe fact = new BDBFactoryJe(dbFolder);
        return fact;
    }
    
    public static BDBFactoryDb getFactoryDb() throws Exception{
        File dbFolder = new File(TUtils.getTestProperties().getProperty(
                "test.db.path"));
        Directory.deleteDirectory(dbFolder);
        assertTrue(!dbFolder.exists());
        assertTrue(dbFolder.mkdirs());
        BDBFactoryDb fact = new BDBFactoryDb(dbFolder);
        return fact;
    }
    
   

}
