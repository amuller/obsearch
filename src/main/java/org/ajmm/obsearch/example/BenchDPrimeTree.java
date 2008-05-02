package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.cache.OBStringFactory;
import org.ajmm.obsearch.example.ted.OBTed;
import org.ajmm.obsearch.example.ted.OBTedFactory;
import org.ajmm.obsearch.index.DIndexShort;
import org.ajmm.obsearch.index.DPrimeIndexShort;
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

public class BenchDPrimeTree {

    private static final Logger logger = Logger.getLogger("BenchDPrimeTree");

    public static void main(String args[]) {
        try {
            PropertyConfigurator.configure("obexample.log4j");
        } catch (final Exception e) {
            System.err.print("Make sure log4j is configured properly"
                    + e.getMessage());
            e.printStackTrace();
            System.exit(48);
        }
        try {
            File dbFolder = new File(args[0]);
            String query = args[1];
            String dbData = args[2];
            
            byte pivots = Byte.parseByte(args[3]);
            logger.debug("Doing pivots: " + pivots);
            int hackOne = Integer.parseInt(args[4]);
            logger.debug("Hack: " + hackOne);

            String mode = args[5];
            logger.debug("Mode: " + mode);
            
            
            dbFolder = new File(dbFolder, mode);
            Directory.deleteDirectory(dbFolder);
            dbFolder.mkdirs();
            // IncrementalKMeansPPPivotSelectorShort<OBSlice> ps = new
            // IncrementalKMeansPPPivotSelectorShort<OBSlice>(new AcceptAll());
            // IncrementalBustosNavarroChavezShort<OBSlice> ps = new
            // 
            //
            BDBFactory fact = new BDBFactory(dbFolder);
            if (mode.equals("ted")) {
                //IncrementalBustosNavarroChavezShort<OBTed> ps = new
                //IncrementalBustosNavarroChavezShort<OBTed>(new AcceptAll(),
                 //        317, 317);
                IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector(IncrementalFixedPivotSelector.ted);
                DPrimeIndexShort < OBTed > index = new DPrimeIndexShort < OBTed >(
                        fact, pivots, ps, OBTed.class);
                index.hackOne = hackOne;
                OBTedFactory.maxSliceSize = 20;
                Benchmark.totalTimes = 1;
                Benchmark < OBTed > b = new Benchmark < OBTed >(
                        new OBTedFactory());
                b.benchTed(index, query, dbData);

            }else if (mode.equals("lev")) {
               // IncrementalBustosNavarroChavezShort<OBString> ps = new
                //IncrementalBustosNavarroChavezShort<OBString>(new AcceptAll(),
                 //        1000, 1000);
                
                IncrementalKMeansPPPivotSelectorShort<OBString> ps = new
                 IncrementalKMeansPPPivotSelectorShort<OBString>(new AcceptAll());
                //IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector(IncrementalFixedPivotSelector.lev);               
                DPrimeIndexShort < OBString > index = new DPrimeIndexShort < OBString >(
                        fact, pivots, ps, OBString.class);
                index.hackOne = hackOne;       
                
                Benchmark < OBString > b = new Benchmark < OBString >(
                        new OBStringFactory());
                b.MAX_DATA = 300000;
                b.benchLev(index, query, dbData);

            }
            
            else { // default mode OBSlice
                IncrementalFixedPivotSelector ps = new IncrementalFixedPivotSelector(IncrementalFixedPivotSelector.mtd);               
                DPrimeIndexShort < OBSlice > index = new DPrimeIndexShort < OBSlice >(
                        fact, pivots, ps, OBSlice.class);
                index.hackOne = hackOne;
                Benchmark < OBSlice > b = new Benchmark < OBSlice >(
                        new OBSliceFactory());
                b.benchMtd(index, query, dbData);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
