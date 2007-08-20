package org.ajmm.obsearch.index;

import junit.framework.TestCase;

import org.junit.Test;

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
 * The Parallel Index test is currently disabled.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class TestParallelIndex
        extends TestCase {

    /**
     * The Parallel Index test is currently disabled.
     * @throws Exception
     *             If something goes really bad.
     */
    @Test
    public void testParallelIndexPPTree() throws Exception {
        // TODO: enable parallel index in the future.
        /*
         * File dbFolder = new
         * File(TUtils.getTestProperties().getProperty("test.db.path"));
         * IndexSmokeTUtil.deleteDB(dbFolder); assertTrue(! dbFolder.exists());
         * assertTrue(dbFolder.mkdirs()); IndexShort<OBSlice> index = new
         * PPTreeShort<OBSlice>( dbFolder, (byte) 30, (byte) 2, (short)0,
         * (short) 200); ParallelIndexShort<OBSlice> pindex = new
         * ParallelIndexShort<OBSlice>(index,2,3000); IndexSmokeTUtil t = new
         * IndexSmokeTUtil(); t.tIndex(pindex);
         */
    }

}
