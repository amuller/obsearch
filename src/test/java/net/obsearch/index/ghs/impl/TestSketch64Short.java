package net.obsearch.index.ghs.impl;

import org.apache.log4j.Logger;
import org.junit.Test;

import net.obsearch.example.OBSlice;
import net.obsearch.example.OBSliceFactory;
import net.obsearch.index.utils.IndexSmokeTUtil;
import net.obsearch.index.utils.IndexSmokeTUtilApprox;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.muller2.IncrementalMullerRosaShort;
import net.obsearch.storage.bdb.BDBFactoryJe;
import net.obsearch.storage.bdb.Utils;


public class TestSketch64Short {

	/**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(TestSketch64Short.class);

    /**
     * Tests on the P+Tree.
     * @throws Exception If something goes really bad.
     */
    @Test
    public void testIDistanceTree() throws Exception {
        
      
    	 IncrementalMullerRosaShort<OBSlice> sel = new IncrementalMullerRosaShort<OBSlice>(
 				new AcceptAll<OBSlice>(), 100, 100, (short) 5000);
    	BDBFactoryJe fact = Utils.getFactoryJe();
    	Sketch64Short<OBSlice> index = new Sketch64Short<OBSlice>(OBSlice.class, sel, 64, 1 );
        index.init(fact);
        index.setKAlpha(2f);
        IndexSmokeTUtilApprox<OBSlice> t = new IndexSmokeTUtilApprox<OBSlice>(new OBSliceFactory());
        t.tIndex(index);

    }
	
}
