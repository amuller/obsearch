package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.index.DIndexShort;
import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.PPTreeShort;
import org.ajmm.obsearch.index.pivotselection.AcceptAll;
import org.ajmm.obsearch.index.pivotselection.IncrementalBustosNavarroChavezShort;
import org.ajmm.obsearch.index.pivotselection.IncrementalDummyPivotSelector;
import org.ajmm.obsearch.index.pivotselection.IncrementalFixedPivotSelector;
import org.ajmm.obsearch.index.pivotselection.IncrementalKMeansPPPivotSelectorShort;
import org.ajmm.obsearch.index.pivotselection.KMeansPPPivotSelector;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.storage.bdb.BDBFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class BenchDTree {
    
    private static final Logger logger = Logger.getLogger("BenchDTree");

    
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
        
        byte pivots = Byte.parseByte(args[3]);
        logger.debug("Doing pivots: " + pivots);
        
        short p = Short.parseShort(args[4]);
        logger.debug("P: " + p);
        float prob = Float.parseFloat(args[5]);
        logger.debug("Prob: "  + prob);
        
        int maxLevel = Integer.parseInt(args[6]);
        logger.debug("MAX: "  + maxLevel);
      //IncrementalKMeansPPPivotSelectorShort<OBSlice> ps = new IncrementalKMeansPPPivotSelectorShort<OBSlice>(new AcceptAll());
      IncrementalBustosNavarroChavezShort<OBSlice> ps = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
              30, 30);
      
     // IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector();
      
        
        BDBFactory fact = new BDBFactory(dbFolder);
        DIndexShort<OBSlice> index = new DIndexShort<OBSlice>(fact, pivots,
            ps, OBSlice.class,
            prob, p, maxLevel);

        Benchmark.bench(index, query, dbData);
        
        logger.info(index.getStats());

        
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
