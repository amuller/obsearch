package org.ajmm.obsearch.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.utils.IndexSmokeTUtil;
import org.ajmm.obsearch.index.utils.OBFactory;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.apache.log4j.Logger;

public class Benchmark<O extends OBShort> {
    
    private OBFactory<O> factory;
    
    protected int MAX_DATA = Integer.MAX_VALUE;
    
    // only process 1000 randomly selected queries.
    public int MAX_QUERIES = 1000;

    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(Benchmark.class);
    
    public Benchmark(OBFactory <O> factory){
        this.factory = factory;
    }

    protected  void initIndex(IndexShort < O > index, String query,
            String db) throws Exception {

        logger.debug("query file: " + query);
        logger.debug("db file: " + db);

        logger.info("Adding data");
        BufferedReader r = new BufferedReader(new FileReader(db));
        String re = r.readLine();
        int realIndex = 0;
        while (re != null && realIndex <= MAX_DATA) {
            String line = re;
            if (line != null) {
                O s = factory.create(line);
                if (factory.shouldProcess(s)) {
                    Result res = index.insert(s);
                    if (res.getStatus() != Result.Status.OK) {
                        throw new Exception("Could not insert status: " + res.getStatus().toString()  + " line: <" + line + ">");
                    }
                    realIndex++;
                    
                }
            }
            re = r.readLine();
            
        }
        r.close();

        // "learn the data".
        logger.info("freezing");
        index.freeze();
        logger.debug("Distance count creation: " + OBSlice.count);
        OBSlice.count = 0;

    }
    
    

    public static int totalTimes = 2;

    protected  void benchAux(IndexShort < O > index, String query,
            short range, byte k) throws Exception {
        OBSlice.count = 0;
        index.resetStats();
        try {
            int times = 0;
            long time = System.currentTimeMillis();
            while (times < totalTimes) { // repeat 10 times
                BufferedReader r = new BufferedReader(new FileReader(query));
                String re = r.readLine();
                int i = 0;
                HashMap < String, Integer > queries = new HashMap < String, Integer >();
                while (re != null) {
                    String line = re;
                    if (line != null && !queries.containsKey(line)) {
                        queries.put(line, Integer.MAX_VALUE);
                        OBPriorityQueueShort < O > x = new OBPriorityQueueShort < O >(
                                k);
                       

                        O s = factory.create(line);
                        if (factory.shouldProcess(s)) {
                            index.searchOB(s, range, x);
                            if (i % 100 == 0) {
                                logger.info("Matching " + i);
                            }
                            i++;
                            
                        }
                        if (i == this.MAX_QUERIES) {
                            logger.warn("Finishing test at i : " + i);
                            break;
                        }
                    }

                    re = r.readLine();
                }
                times++;
            }
            logger
                    .info("r "
                            + range
                            + " k "
                            + k
                            + " time: "
                            + ((System.currentTimeMillis() - time) / 1000));
            printDistanceCount();
            logger.info("Stats: + " + index.getStats());
        } catch (UnsupportedOperationException e) {
            logger.info("Skipping method...");
        }
    }

    public  void bench(IndexShort < O > index, String query,
            String db) throws Exception {
        initIndex(index, query, db);
        
        search(index, query);
    }
    
    protected void search(IndexShort < O > index, String query) throws Exception{
        logger.info("Current stats: " + index.getStats());
        index.resetStats();
        logger.info("Real distance count after DB creation");
        printDistanceCount();
        logger.debug("Searching ");
        
        benchAux(index, query, (short) 0, (byte) 1);

        benchAux(index, query, (short) 2, (byte) 1);

        benchAux(index, query, (short) 2, (byte) 3);

        benchAux(index, query, (short) 3, (byte) 1);

        benchAux(index, query, (short) 3, (byte) 3);

        benchAux(index, query, (short) 7, (byte) 1);

        benchAux(index, query, (short) 7, (byte) 3);

        benchAux(index, query, (short) 10, (byte) 1);

        benchAux(index, query, (short) 10, (byte) 3);
    }

    public static void printDistanceCount() {
        logger.debug("Real Distance count search: " + OBSlice.count);
        OBSlice.count = 0;
    }

    public static boolean shouldProcessSlice(OBSlice x) throws Exception {
        return x.size() <= maxSliceSize;
    }

    public static int maxSliceSize = 500;
}
