/**
 *
 */
package org.ajmm.obsearch.index;

import java.io.File;
import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.utils.Directory;
import org.apache.log4j.Logger;
import junit.framework.TestCase;

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
 * Tests on the pyramid technique.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class TestExtentedPyramidIndex
        extends TestCase {

    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(TestExtentedPyramidIndex.class);

    /**
     * Test the pyramid technique.
     * @throws Exception If something goes really bad.
     */
    public void testPyramid() throws Exception {
        File dbFolder = new File(TUtils.getTestProperties().getProperty(
                "test.db.path"));
        Directory.deleteDirectory(dbFolder);
        assertTrue(!dbFolder.exists());
        assertTrue(dbFolder.mkdirs());
        IndexShort < OBSlice > index = new ExtendedPyramidIndexShort < OBSlice >(
                dbFolder, (byte) 15, (short) 0, (short) 200);
        IndexSmokeTUtil t = new IndexSmokeTUtil();
        t.tIndex(index);
    }

}
