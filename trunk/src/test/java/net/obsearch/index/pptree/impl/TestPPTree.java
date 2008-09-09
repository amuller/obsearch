package net.obsearch.index.pptree.impl;

import java.io.File;

import junit.framework.TestCase;

import net.obsearch.example.OBSlice;
import net.obsearch.example.OBSliceFactory;
import net.obsearch.index.pptree.impl.PPTreeShort;
import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.IndexSmokeTUtil;
import net.obsearch.index.utils.TUtils;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.storage.bdb.Utils;

import net.obsearch.storage.bdb.BDBFactoryJe;
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
public class TestPPTree
        extends TestCase {

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
       
        
        
        IncrementalBustosNavarroChavezShort<OBSlice> sel = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
                30, 30);
        BDBFactoryJe fact = Utils.getFactoryJe();
        
        PPTreeShort<OBSlice> d = new PPTreeShort<OBSlice>(OBSlice.class, sel, 20, 6, (short) 0, (short) (OBSliceFactory.maxSliceSize * 2));
        d.init(fact);
        IndexSmokeTUtil<OBSlice> t = new IndexSmokeTUtil<OBSlice>(new OBSliceFactory());
        t.tIndex(d);
    }

}
