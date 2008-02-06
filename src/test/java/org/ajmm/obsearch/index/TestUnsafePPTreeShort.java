package org.ajmm.obsearch.index;

/*
OBSearch: a distributed similarity search engine
This project is to similarity search what 'bit-torrent' is to downloads.
Copyright (C)  2007 Arnoldo Jose Muller Molina

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

/**
* Tests on the Unsafe P+Tree.
* @author Arnoldo Jose Muller Molina
* @since 0.7
*/
import java.io.File;

import junit.framework.TestCase;

import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.pivotselection.DummyPivotSelector;
import org.ajmm.obsearch.index.utils.Directory;
import org.apache.log4j.Logger;
import org.junit.Before;

public class TestUnsafePPTreeShort  extends TestCase{

    @Before
    public void setUp() throws Exception {
    }
    
    /**
    * Logger.
    */
   private static transient final Logger logger = Logger
           .getLogger(TestPPTree.class);

   /**
    * Tests on the P+Tree.
    * @throws Exception If something goes really bad.
    */
   public void testPPTree() throws Exception {
       File dbFolder = new File(TUtils.getTestProperties().getProperty(
               "test.db.path"));
       Directory.deleteDirectory(dbFolder);
       assertTrue(!dbFolder.exists());
       assertTrue(dbFolder.mkdirs());
       DummyPivotSelector ps = new DummyPivotSelector();
       IndexShort < OBSlice > index = new UnsafePPTreeShort < OBSlice >(dbFolder,
               (byte) 30, (byte) 8, (short) 0, (short) 200, ps, OBSlice.class);

       IndexSmokeTUtil t = new IndexSmokeTUtil();
       t.tIndex(index);
   }

}
