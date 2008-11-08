package net.obsearch.index.knngraph;

import net.obsearch.example.OBSlice;
import net.obsearch.example.OBSliceFactory;
import net.obsearch.index.knngraph.impl.KnnGraphShort;
import net.obsearch.index.utils.IndexSmokeTUtil;
import net.obsearch.index.utils.IndexSmokeTUtilApprox;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.bustos.impl.IncrementalBustosNavarroChavezShort;
import net.obsearch.pivots.dummy.IncrementalDummyPivotSelector;
import net.obsearch.storage.bdb.BDBFactoryDb;
import net.obsearch.storage.bdb.BDBFactoryJe;
import net.obsearch.storage.bdb.Utils;

import org.junit.Test;


public class TestKnnGraph {
	
	@Test
	public void testPrimeTree() throws Exception {
        
	      //IncrementalKMeansPPPivotSelectorShort<OBSlice> sel = new IncrementalKMeansPPPivotSelectorShort<OBSlice>(new AcceptAll());
	       IncrementalDummyPivotSelector<OBSlice> sel = new IncrementalDummyPivotSelector<OBSlice> ();
	       //
	       
	       //IncrementalBustosNavarroChavezShort<OBSlice> sel = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
	        //        100, 100);
	        //IncrementalFixedPivotSelector sel = new IncrementalFixedPivotSelector();
	       BDBFactoryJe fact = Utils.getFactoryJe();
	        //BDBFactoryDb fact = Utils.getFactoryDb();
	        //OBLFactory fact = Utils.getFactoryL();

	        KnnGraphShort<OBSlice> knn = new KnnGraphShort<OBSlice>(OBSlice.class, sel, 32, 40, (short)500, 40);
	        // k = 25, 10 seeds. Increasing k improves things a lot!
	        // 2.4 for range 3 (1 / 1000)
	        // 4 for  range 20
	        knn.setT(3.0f);
	        knn.init(fact);
	        knn.setSeeds(30);
	        IndexSmokeTUtilApprox<OBSlice> t = new IndexSmokeTUtilApprox<OBSlice>(new OBSliceFactory());
	        t.tIndex(knn);

	        //logger.info("Boxes per search: " + d.totalBoxAccess/ d.queryCount);
	        //logger.info("Query count: " + d.queryCount);
	        //logger.info("Total boxes: " +d.searchedBoxesTotal);        
	        //logger.info("Smap records: " + d.smapRecordsCompared);
	        //logger.info("Distance computations: " + d.distanceComputations);
	        
	        // best 15 1.4 20 seeds
	    }

}
