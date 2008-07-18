package net.obsearch.storage.bdb;

import java.io.File;

import org.ajmm.obsearch.storage.bdb.BDBFactory;

import junit.framework.TestCase;

import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.TUtils;

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
