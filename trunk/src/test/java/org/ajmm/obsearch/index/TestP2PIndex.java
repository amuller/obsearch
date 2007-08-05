package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.utils.Directory;
import org.apache.log4j.Logger;

import junit.framework.TestCase;

public class TestP2PIndex extends TestCase {

	private static transient final Logger logger = Logger
			.getLogger(TestP2PIndex.class);

	public void testP2P() throws Exception {
		String mainPath = TUtils.getTestProperties()
				.getProperty("test.db.path");

		SynchronizableIndexShort<OBSlice> index1 = generateIndex(mainPath
				+ File.separator + "first");
		File jxta1 = new File(mainPath + File.separator + "first"
				+ File.separator + "jxta");
		prepareFolder(jxta1);
		IndexSmokeTUtil t = new IndexSmokeTUtil();
		t.initIndex(index1);
		// the index must be frozen before we add it to p2p
		P2PIndexShort<OBSlice> p2p1 = new P2PIndexShort<OBSlice>(index1, jxta1, "sync",1);
		logger.debug("Opening p2p #1");
		//p2p1.open();
		
		
		logger.debug("Creating another peer:");
		IndexShort<OBSlice> loadedFromXML = (IndexShort<OBSlice>) IndexFactory
				.createFromXML(index1.toXML());
		
		File std2 = new File(mainPath + File.separator
				+ "second" + File.separator + "std");
		File sync2 = new File(mainPath + File.separator + "second"
				+ File.separator + "sync");
		File jxta2 = new File(mainPath + File.separator + "second"
				+ File.separator + "jxta");

		prepareFolder(std2);
		prepareFolder(sync2);
		prepareFolder(jxta2);
		
		loadedFromXML.relocateInitialize(std2);
		
		SynchronizableIndexShort<OBSlice> index2 = new SynchronizableIndexShort<OBSlice>(
				loadedFromXML, sync2);
		P2PIndexShort<OBSlice> p2p2 = new P2PIndexShort<OBSlice>(index2,jxta2, "join",1);
		logger.debug("Opening p2p #2");
		//p2p2.open();

		// now we create another index... empty.
	}
	
	// prepares a folder
	protected void prepareFolder(File x) throws IOException{
		Directory.deleteDirectory(x);
		assertTrue(!x.exists());
		assertTrue(x.mkdirs());
		assertTrue(x.exists());
	}

	protected SynchronizableIndexShort<OBSlice> generateIndex(String path)
			throws Exception {
		File dbFolder = new File(path + File.separator + "std");
		File dbFolderSync = new File(path + File.separator + "sync");
		Directory.deleteDirectory(dbFolder);
		Directory.deleteDirectory(dbFolderSync);
		prepareFolder(dbFolder);
		prepareFolder(dbFolderSync);

		IndexShort<OBSlice> index = new PPTreeShort<OBSlice>(dbFolder,
				(byte) 30, (byte) 2, (short) 0, (short) 200);
		SynchronizableIndexShort<OBSlice> index2 = new SynchronizableIndexShort<OBSlice>(
				index, dbFolderSync);

		return index2;
	}

}
