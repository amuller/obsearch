/**
 *
 */
package org.ajmm.obsearch.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.AbstractOBResult;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.pivotselection.FixedPivotSelector;
import org.ajmm.obsearch.index.pivotselection.RandomPivotSelector;
import org.ajmm.obsearch.index.pivotselection.TentaclePivotSelectorShort;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.Before;

import junit.framework.TestCase;

/*
 * OBSearch: a distributed similarity search engine This project is to
 * similarity search what 'bit-torrent' is to downloads. Copyright (C) 2007
 * Arnoldo Jose Muller Molina
 *
 * This program is free software; you can redistribute it and/or modify it under
 * the terms of the GNU General Public License as published by the Free Software
 * Foundation; either version 2 of the License, or (at your option) any later
 * version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
 * details.
 *
 * You should have received a copy of the GNU General Public License along with
 * this program; if not, write to the Free Software Foundation, Inc., 51
 * Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
 */
/**
 * Perform different tests on the pyramid technique
 *
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class TestExtentedPyramidIndex extends TestCase {


    private static transient final Logger logger = Logger
            .getLogger(TestExtentedPyramidIndex.class);


    
    public void testPyramid() throws Exception{
    	// TODO: enable this test:
    	File dbFolder = new File(TUtils.getTestProperties().getProperty("test.db.path") );
    	Directory.deleteDirectory(dbFolder);
   	 	assertTrue(! dbFolder.exists());
       assertTrue(dbFolder.mkdirs());
    	IndexShort<OBSlice> index = new ExtendedPyramidIndexShort<OBSlice>(
                 dbFolder, (byte) 15, (short)0, (short) 200);
    	IndexSmokeTUtil t = new IndexSmokeTUtil();
    	t.tIndex(index);
    }


}
