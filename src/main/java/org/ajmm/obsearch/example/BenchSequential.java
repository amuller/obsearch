package org.ajmm.obsearch.example;

import java.io.File;

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
            SequentialSearchShort < OBSlice > index = new SequentialSearchShort < OBSlice >(
                    fact, OBSlice.class);
            Benchmark.totalTimes=1;
            Benchmark.bench(index, query, dbData);
            logger.info(index.getStats());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
