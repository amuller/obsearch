package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.cache.OBStringFactory;
import org.ajmm.obsearch.example.ted.OBTed;
import org.ajmm.obsearch.example.ted.OBTedFactory;
import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.PPTreeShort;
import org.ajmm.obsearch.index.PivotSelector;
import org.ajmm.obsearch.index.pivotselection.AcceptAll;
import org.ajmm.obsearch.index.pivotselection.FixedPivotSelector;
import org.ajmm.obsearch.index.pivotselection.IncrementalBustosNavarroChavezShort;
import org.ajmm.obsearch.index.pivotselection.IncrementalFixedPivotSelector;
import org.ajmm.obsearch.index.pivotselection.KMeansPPPivotSelector;
import org.ajmm.obsearch.index.utils.Directory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class BenchPPTree {

    private static final Logger logger = Logger.getLogger("P+Tree");

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
            
            /*
             * KMeansPPPivotSelector ps = new KMeansPPPivotSelector < OBSlice >(
             * new AcceptAll < OBSlice >());
             */

            byte pivots = Byte.parseByte(args[3]);
            logger.debug("Doing pivots: " + pivots);

            

            String mode = args[4];
            logger.debug("Mode: " + mode);
            
            dbFolder = new File(dbFolder, mode);
            Directory.deleteDirectory(dbFolder);
            dbFolder.mkdirs();
            
            if (mode.equals("ted")) {
                Benchmark.totalTimes = 1;
                OBTedFactory.maxSliceSize = 20;
                FixedPivotSelector ps = new FixedPivotSelector(IncrementalFixedPivotSelector.ted);
                PPTreeShort < OBTed > index = new PPTreeShort < OBTed >(
                        dbFolder, pivots, (byte) 12, (short) 0,
                        (short) (OBTedFactory.maxSliceSize * 2), ps, OBTed.class);
               
                Benchmark < OBTed > b = new Benchmark < OBTed >(
                        new OBTedFactory());
                b.benchTed(index, query, dbData);
                
                
            } else if(mode.equals("lev")){
                FixedPivotSelector ps = new FixedPivotSelector(IncrementalFixedPivotSelector.lev);
                PPTreeShort < OBString > index = new PPTreeShort < OBString >(
                        dbFolder, pivots, (byte) 12, (short) 0,
                        (short) 10000, ps, OBString.class);
               
                Benchmark < OBString > b = new Benchmark < OBString >(
                        new OBStringFactory());
                b.MAX_DATA = 300000;
                b.benchLev(index, query, dbData);
                
            }else {
                FixedPivotSelector ps = new FixedPivotSelector(IncrementalFixedPivotSelector.mtd);
                PPTreeShort < OBSlice > index = new PPTreeShort < OBSlice >(
                        dbFolder, pivots, (byte) 12, (short) 0,
                        (short) (Benchmark.maxSliceSize * 2), ps, OBSlice.class);

                Benchmark < OBSlice > b = new Benchmark < OBSlice >(
                        new OBSliceFactory());
                b.benchMtd(index, query, dbData);
            }

           

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
