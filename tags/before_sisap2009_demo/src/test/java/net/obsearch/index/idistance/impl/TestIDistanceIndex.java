


package net.obsearch.index.idistance.impl;

import java.io.File;

import junit.framework.TestCase;

import net.obsearch.example.OBSlice;
import net.obsearch.example.OBSliceFactory;
import net.obsearch.example.ted.OBTed;
import net.obsearch.example.ted.OBTedFactory;
import net.obsearch.index.idistance.impl.IDistanceIndexShort;

import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.IndexSmokeTUtil;
import net.obsearch.index.utils.TUtils;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.dummy.IncrementalFixedPivotSelector;
import net.obsearch.pivots.kmeans.impl.IncrementalKMeansPPPivotSelectorShort;


import net.obsearch.storage.bdb.BDBFactoryDb;
import net.obsearch.storage.bdb.BDBFactoryJe;
import net.obsearch.storage.bdb.Utils;
import net.obsearch.storage.l.OBLFactory;

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
public class TestIDistanceIndex
        extends TestCase {

    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(TestIDistanceIndex.class);

    /**
     * Tests on the P+Tree.
     * @throws Exception If something goes really bad.
     */
    public void testIDistanceTree() throws Exception {
        

       //IncrementalBustosNavarroChavezShort<OBSlice> sel = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
        //        30, 30);
    	//IncrementalKMeansPPPivotSelectorShort<OBSlice> sel = new IncrementalKMeansPPPivotSelectorShort<OBSlice>(new AcceptAll());
    	//IncrementalMullerShort<OBSlice> sel = new IncrementalMullerShort<OBSlice>(new AcceptAll(), 100, 100, (short)7);
    	//IncrementalFixedPivotSelector sel = new IncrementalFixedPivotSelector();
    	
    	 IncrementalBustosNavarroChavezShort<OBSlice> sel = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
                 100, 100);
    	//OBLFactory fact = Utils.getFactoryL();
    	BDBFactoryDb fact = Utils.getFactoryDb();
        IDistanceIndexShort<OBSlice> i = new IDistanceIndexShort<OBSlice>(OBSlice.class, sel, 20);
        i.init(fact);
        IndexSmokeTUtil<OBSlice> t = new IndexSmokeTUtil<OBSlice>(new OBSliceFactory());
        t.tIndex(i);

    }

}
