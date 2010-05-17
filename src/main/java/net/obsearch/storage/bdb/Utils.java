package net.obsearch.storage.bdb;

import java.io.File;
import java.io.IOException;

import junit.framework.TestCase;

import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.TUtils;

import net.obsearch.storage.tc.TCFactory;

public class Utils extends TestCase {

	public static BDBFactoryJe getFactoryJe() throws Exception {		
		BDBFactoryJe fact = new BDBFactoryJe(init());
		return fact;
	}

	/*public static BDBFactoryDb getFactoryDb() throws Exception {
		
		BDBFactoryDb fact = new BDBFactoryDb(init());
		return fact;
	}*/
	

	public static TCFactory getFactoryTC() throws Exception {
		TCFactory fact = new TCFactory(init());
		return fact;
	}

	

	private static File init() throws IOException {
		File dbFolder = new File(TUtils.getTestProperties().getProperty(
				"test.db.path"));
		Directory.deleteDirectory(dbFolder);
		assertTrue(!dbFolder.exists());
		assertTrue(dbFolder.mkdirs());
		return dbFolder;
	}

}
