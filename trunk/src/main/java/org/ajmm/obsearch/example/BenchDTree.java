package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.cache.OBStringFactory;
import org.ajmm.obsearch.example.ted.OBTed;
import org.ajmm.obsearch.example.ted.OBTedFactory;
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
        
        
        byte pivots = Byte.parseByte(args[3]);
        logger.debug("Doing pivots: " + pivots);
        
        short p = Short.parseShort(args[4]);
        logger.debug("P: " + p);
        float prob = Float.parseFloat(args[5]);
        logger.debug("Prob: "  + prob);
        
        int maxLevel = Integer.parseInt(args[6]);
        logger.debug("MAX: "  + maxLevel);
      //IncrementalKMeansPPPivotSelectorShort<OBSlice> ps = new IncrementalKMeansPPPivotSelectorShort<OBSlice>(new AcceptAll());
     
      String mode = args[7];
      logger.debug("Mode: " + mode);
      
      dbFolder = new File(dbFolder, mode);
      Directory.deleteDirectory(dbFolder);
      dbFolder.mkdirs();
      
      BDBFactory fact = new BDBFactory(dbFolder);
     // IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector();
      if(mode.equals("ted")){
          
          IncrementalBustosNavarroChavezShort<OBTed> ps = new IncrementalBustosNavarroChavezShort<OBTed>(new AcceptAll(),
                  317, 317);
          //IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector(IncrementalFixedPivotSelector.tedLevel);
          DIndexShort<OBTed> index = new DIndexShort<OBTed>(fact, pivots,
                  ps, OBTed.class,
                  prob, p, maxLevel);
          OBTedFactory.maxSliceSize = 20;
          Benchmark.totalTimes = 1;
              Benchmark < OBTed > b = new Benchmark < OBTed >(
                      new OBTedFactory());
              b.benchTed(index, query, dbData);
          
      }else if(mode.equals("lev")){
          //IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector(IncrementalFixedPivotSelector.levLevel);
          IncrementalBustosNavarroChavezShort<OBString> ps = new IncrementalBustosNavarroChavezShort<OBString>(new AcceptAll(),
                  1000, 1000);
          
          DIndexShort<OBString> index = new DIndexShort<OBString>(fact, pivots,
                  ps, OBString.class,
                  prob, p, maxLevel);
              Benchmark < OBString > b = new Benchmark < OBString >(
                      new OBStringFactory());
              b.benchLev(index, query, dbData);
      }else{
          IncrementalBustosNavarroChavezShort<OBSlice> ps = new IncrementalBustosNavarroChavezShort<OBSlice>(new AcceptAll(),
                  1000, 1000);
          //IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector(IncrementalFixedPivotSelector.mtdLevel);
        DIndexShort<OBSlice> index = new DIndexShort<OBSlice>(fact, pivots,
            ps, OBSlice.class,
            prob, p, maxLevel);

        Benchmark < OBSlice > b = new Benchmark < OBSlice >(
                new OBSliceFactory());
        b.benchMtd(index, query, dbData);
        
       
      }
        
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
