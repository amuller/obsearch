package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.example.ted.OBTed;
import org.ajmm.obsearch.example.ted.OBTedFactory;
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
            Directory.deleteDirectory(dbFolder);
            dbFolder.mkdirs();
            /*
             * KMeansPPPivotSelector ps = new KMeansPPPivotSelector < OBSlice >(
             * new AcceptAll < OBSlice >());
             */

            byte pivots = Byte.parseByte(args[3]);
            logger.debug("Doing pivots: " + pivots);

            FixedPivotSelector ps = new FixedPivotSelector();

            String mode = args[4];
            logger.debug("Mode: " + mode);
            if (mode.equals("ted")) {
                
                PPTreeShort < OBTed > index = new PPTreeShort < OBTed >(
                        dbFolder, pivots, (byte) 12, (short) 0,
                        (short) (Benchmark.maxSliceSize * 2), ps, OBTed.class);
                OBTedFactory.maxSliceSize = 20;
                Benchmark < OBTed > b = new Benchmark < OBTed >(
                        new OBTedFactory());
                b.bench(index, query, dbData);
                
                
            } else {

                PPTreeShort < OBSlice > index = new PPTreeShort < OBSlice >(
                        dbFolder, pivots, (byte) 12, (short) 0,
                        (short) (Benchmark.maxSliceSize * 2), ps, OBSlice.class);

                Benchmark < OBSlice > b = new Benchmark < OBSlice >(
                        new OBSliceFactory());
                b.bench(index, query, dbData);
            }

           

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
