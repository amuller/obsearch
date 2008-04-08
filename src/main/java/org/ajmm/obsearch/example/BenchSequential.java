package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.example.ted.OBTed;
import org.ajmm.obsearch.example.ted.OBTedFactory;
import org.ajmm.obsearch.index.SequentialSearchShort;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.storage.bdb.BDBFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class BenchSequential {

    private static final Logger logger = Logger.getLogger("BenchSequential");

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
            BDBFactory fact = new BDBFactory(dbFolder);

            String mode = args[3];
            logger.debug("Mode: " + mode);
            Benchmark.totalTimes = 1;
            if (mode.equals("ted")) {
                
                SequentialSearchShort < OBTed > index = new SequentialSearchShort < OBTed >(
                        fact, OBTed.class);
                OBTedFactory.maxSliceSize = 30;
                Benchmark < OBTed > b = new Benchmark < OBTed >(
                        new OBTedFactory());
                b.bench(index, query, dbData);
                logger.info(index.getStats());
                

            } else {
                SequentialSearchShort < OBSlice > index = new SequentialSearchShort < OBSlice >(
                        fact, OBSlice.class);
               
                Benchmark < OBSlice > b = new Benchmark < OBSlice >(
                        new OBSliceFactory());
                b.bench(index, query, dbData);
                logger.info(index.getStats());
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
