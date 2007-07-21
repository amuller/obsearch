package org.ajmm.obsearch.index;

import java.io.File;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
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
		IndexSmokeTUtil t = new IndexSmokeTUtil();
		t.initIndex(index1);
		// the index must be frozen before we add it to p2p
		P2PIndexShort<OBSlice> p2p1 = new P2PIndexShort<OBSlice>(index1);
		logger.debug("Opening p2p #1");
		p2p1.open();
		logger.debug("Creating another peer:");
		IndexShort<OBSlice> loadedFromXML = (IndexShort<OBSlice>) IndexFactory
				.createFromXML(index1.toXML());
		loadedFromXML.relocateInitialize(new File(mainPath + File.separator
				+ "second" + File.separator + "std"));
		SynchronizableIndexShort<OBSlice> index2 = new SynchronizableIndexShort<OBSlice>(
				loadedFromXML, new File(mainPath + File.separator + "second"
						+ File.separator + "sync"));
		P2PIndexShort<OBSlice> p2p2 = new P2PIndexShort<OBSlice>(index2);
		logger.debug("Opening p2p #2");
		p2p2.open();

		// now we create another index... empty.
	}

	protected SynchronizableIndexShort<OBSlice> generateIndex(String path)
			throws Exception {
		File dbFolder = new File(path + File.separator + "std");
		File dbFolderSync = new File(path + File.separator + "sync");
		IndexSmokeTUtil.deleteDB(dbFolder);
		IndexSmokeTUtil.deleteDB(dbFolderSync);
		assertTrue(!dbFolder.exists());
		assertTrue(dbFolder.mkdirs());

		assertTrue(!dbFolderSync.exists());
		assertTrue(dbFolderSync.mkdirs());

		IndexShort<OBSlice> index = new PPTreeShort<OBSlice>(dbFolder,
				(byte) 30, (byte) 2, (short) 0, (short) 200);
		SynchronizableIndexShort<OBSlice> index2 = new SynchronizableIndexShort<OBSlice>(
				index, dbFolderSync);

		return index2;
	}

}
