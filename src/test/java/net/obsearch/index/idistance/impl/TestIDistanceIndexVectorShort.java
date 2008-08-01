


package net.obsearch.index.idistance.impl;

import java.io.File;

import junit.framework.TestCase;

import net.obsearch.example.OBSlice;
import net.obsearch.example.OBSliceFactory;
import net.obsearch.example.ted.OBTed;
import net.obsearch.example.ted.OBTedFactory;
import net.obsearch.index.OBVectorShort;
import net.obsearch.index.VectorTestFrameworkShort;
import net.obsearch.index.dprime.impl.DPrimeIndexShort;
import net.obsearch.index.dprime.impl.TestDPrimeIndex;
import net.obsearch.index.idistance.impl.IDistanceIndexShort;
import net.obsearch.index.rosa.RosaFilterShort;
import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.IndexSmokeTUtil;
import net.obsearch.index.utils.TUtils;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.dummy.IncrementalFixedPivotSelector;
import net.obsearch.pivots.kmeans.impl.IncrementalKMeansPPPivotSelectorShort;
import net.obsearch.pivots.muller2.impl.IncrementalMullerShort;
import net.obsearch.storage.bdb.BDBFactory;
import net.obsearch.storage.bdb.Utils;
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
public class TestIDistanceIndexVectorShort
        extends TestCase {

    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(TestIDistanceIndexVectorShort.class);

    /**
     * Tests on the P+Tree.
     * @throws Exception If something goes really bad.
     */
    public void testIDistanceTree() throws Exception {
        

       IncrementalBustosNavarroChavezShort<OBVectorShort> sel = new IncrementalBustosNavarroChavezShort<OBVectorShort>(new AcceptAll(),
                100, 100);    	
    	
    	BDBFactory fact = Utils.getFactory();
        IDistanceIndexShort<OBVectorShort> i = new IDistanceIndexShort<OBVectorShort>(OBVectorShort.class, sel, 20);
        i.init(fact);
        VectorTestFrameworkShort t = new VectorTestFrameworkShort(200, 10000, 1000,
    			 i);
        t.test();

    }

}
