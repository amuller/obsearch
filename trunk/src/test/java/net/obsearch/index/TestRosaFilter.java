


package net.obsearch.index;

import java.io.File;

import junit.framework.TestCase;

import net.obsearch.example.OBSlice;
import net.obsearch.example.OBSliceFactory;
import net.obsearch.example.ted.OBTed;
import net.obsearch.example.ted.OBTedFactory;
import net.obsearch.index.dprime.impl.DPrimeIndexShort;
import net.obsearch.index.dprime.impl.TestDPrimeIndex;
import net.obsearch.index.rosa.RosaFilterShort;
import net.obsearch.index.utils.Directory;
import net.obsearch.index.utils.IndexSmokeTUtil;
import net.obsearch.index.utils.IndexSmokeTUtilApprox;
import net.obsearch.index.utils.TUtils;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
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
public class TestRosaFilter
        extends TestCase {

    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(TestRosaFilter.class);

    /**
     * Tests on the P+Tree.
     * @throws Exception If something goes really bad.
     */
    public void testRosaFilter() throws Exception {
        
      //IncrementalKMeansPPPivotSelectorShort<OBSlice> sel = new IncrementalKMeansPPPivotSelectorShort<OBSlice>(new AcceptAll());
       //IncrementalDummyPivotSelector<OBSlice> sel = new IncrementalDummyPivotSelector<OBSlice> ();
        
        
        IncrementalMullerShort<OBSlice> sel = new IncrementalMullerShort<OBSlice>(new AcceptAll(), 100, 100, (short)0);
        BDBFactory fact = Utils.getFactory();
        
        IncrementalBustosNavarroChavezShort<OBSlice> sel2 = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
                100, 100);   
        
        RosaFilterShort<OBSlice> index = new  RosaFilterShort<OBSlice>(OBSlice.class,
    			  sel2, 64,
    			 (short)2, sel, 64) ;
        
        index.init(fact);
        IndexSmokeTUtilApprox<OBSlice> t = new IndexSmokeTUtilApprox<OBSlice>(new OBSliceFactory());
        t.tIndex(index);
        logger.info(index.stats.toString());
       
    }
    
    public void testRosaFilterVectors() throws Exception{
    	
    	  IncrementalMullerShort<OBVectorShort> sel = new IncrementalMullerShort<OBVectorShort>(new AcceptAll(), 100, 100, (short)0);
    	  IncrementalBustosNavarroChavezShort<OBVectorShort> sel2 = new IncrementalBustosNavarroChavezShort<OBVectorShort>(new AcceptAll(),
                  100, 100);   
    	  IndexShort<OBVectorShort> index = new  RosaFilterShort<OBVectorShort>(OBVectorShort.class,
      			  sel2, 64,
      			 (short)2, sel, 64) ;
    	
    	BDBFactory fact = Utils.getFactory();
        
        index.init(fact);
        VectorTestFrameworkApproxShort t = new VectorTestFrameworkApproxShort(200
, 10000, 1000, index);
        t.test();
    }

}
