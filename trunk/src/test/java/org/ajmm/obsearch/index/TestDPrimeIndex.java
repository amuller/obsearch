package org.ajmm.obsearch.index;

import java.io.File;

import junit.framework.TestCase;

import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.dummy.IncrementalDummyPivotSelector;
import net.obsearch.pivots.kmeans.impl.IncrementalKMeansPPPivotSelectorShort;

import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.example.OBSliceFactory;
import org.ajmm.obsearch.example.ted.OBTed;
import org.ajmm.obsearch.example.ted.OBTedFactory;
import org.ajmm.obsearch.index.pivotselection.DummyPivotSelector;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.index.utils.IndexSmokeTUtil;
import org.ajmm.obsearch.index.utils.TUtils;
import org.ajmm.obsearch.storage.bdb.BDBFactory;
import org.ajmm.obsearch.storage.bdb.Utils;
import org.apache.log4j.Logger;

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
 * Tests on the P+Tree.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public class TestDPrimeIndex
        extends TestCase {

    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(TestDIndex.class);

    /**
     * Tests on the P+Tree.
     * @throws Exception If something goes really bad.
     */
    public void testPrimeTree() throws Exception {
        
      //IncrementalKMeansPPPivotSelectorShort<OBSlice> sel = new IncrementalKMeansPPPivotSelectorShort<OBSlice>(new AcceptAll());
       //IncrementalDummyPivotSelector<OBSlice> sel = new IncrementalDummyPivotSelector<OBSlice> ();
        IncrementalBustosNavarroChavezShort<OBSlice> sel = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
                30, 30);
        BDBFactory fact = Utils.getFactory();
        DPrimeIndexShort<OBSlice> d = new DPrimeIndexShort<OBSlice>(fact, (byte)14,
            sel, OBSlice.class);

        IndexSmokeTUtil<OBSlice> t = new IndexSmokeTUtil<OBSlice>(new OBSliceFactory());
        t.tIndex(d);
        //logger.info("Boxes per search: " + d.totalBoxAccess/ d.queryCount);
        logger.info("Query count: " + d.queryCount);
        logger.info("Total boxes: " +d.searchedBoxesTotal);        
        logger.info("Smap records: " + d.smapRecordsCompared);
        logger.info("Distance computations: " + d.distanceComputations);
    }

}
