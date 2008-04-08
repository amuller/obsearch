package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.PPTreeShort;
import org.ajmm.obsearch.index.pivotselection.AcceptAll;
import org.ajmm.obsearch.index.pivotselection.FixedPivotSelector;
import org.ajmm.obsearch.index.pivotselection.KMeansPPPivotSelector;
import org.ajmm.obsearch.index.utils.Directory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class BenchPPTree {
    
    private static final Logger logger = Logger.getLogger("P+Tree");
    
    public static void main(String args[]){
        try {
            PropertyConfigurator.configure("obexample.log4j");
        } catch (final Exception e) {
            System.err.print("Make sure log4j is configured properly"
                    + e.getMessage());
            e.printStackTrace();
            System.exit(48);
        }
        try{
        File dbFolder = new File(args[0]);
        String query = args[1];
        String dbData = args[2]; 
        Directory.deleteDirectory(dbFolder);
        dbFolder.mkdirs();
       /* KMeansPPPivotSelector ps = new KMeansPPPivotSelector < OBSlice >(
                new AcceptAll < OBSlice >());
          
                */
        
        byte pivots = Byte.parseByte(args[3]);
        logger.debug("Doing pivots: " + pivots); 
        
        FixedPivotSelector ps = new FixedPivotSelector();
        
                
        
        PPTreeShort < OBSlice > index = new PPTreeShort < OBSlice >(dbFolder,
                pivots, (byte) 12, (short) 0, (short) (Benchmark.maxSliceSize * 2), ps, OBSlice.class);

        Benchmark.bench(index, query, dbData);
        
        logger.info("Query count: " + index.queryCount);
        logger.info("Initial hyper rect: " + index.initialHyperRectangleTotal);
        logger.info("Final hyper rect: " + index.finalHyperRectangleTotal);
        logger.info("Final pyramid total: " + index.finalPyramidTotal);
        logger.info("Smap records: " + index.smapRecordsCompared );
        logger.info("Distance computations " + index.distanceComputations);
        
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
