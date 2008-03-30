package org.ajmm.obsearch.example;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashMap;

import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.index.IndexShort;
import org.ajmm.obsearch.index.IndexSmokeTUtil;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.apache.log4j.Logger;



public class Benchmark {
    
    /**
     * Logger.
     */
    private static transient final Logger logger = Logger
            .getLogger(Benchmark.class);
    
    public static void initIndex(IndexShort < OBSlice > index, String query, String db) throws Exception {
        
        logger.debug("query file: " + query);
        logger.debug("db file: " + db);
        
        logger.info("Adding data");
        BufferedReader r = new BufferedReader(new FileReader(db));
        String re = r.readLine();
        int realIndex = 0;
        while (re != null) {
            String line = IndexSmokeTUtil.parseLine(re);
            if (line != null) {
                OBSlice s = new OBSlice(line);
                if (IndexSmokeTUtil.shouldProcessSlice(s)) {
                    Result res = index.insert(s);
                    if(res.getStatus() != Result.Status.OK){
                        throw new Exception("Could not insert");
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
        logger.debug("Distance count creation: "+ OBSlice.count);
        OBSlice.count = 0;
        
    }
    
    public static void benchAux(IndexShort < OBSlice > index, String query, short range, byte k) throws Exception{
        try{
        int times = 0;
        long time = System.currentTimeMillis();
        while(times < 10){ // repeat 10 times
        BufferedReader r = new BufferedReader(new FileReader(query));
        String re = r.readLine();
        int i = 0;
        HashMap<String, Integer> queries = new HashMap<String,Integer>();
        while (re != null) {
            String line = IndexSmokeTUtil.parseLine(re);
            if (line != null && ! queries.containsKey(line)) {
                queries.put(line, Integer.MAX_VALUE);
                OBPriorityQueueShort < OBSlice > x = new OBPriorityQueueShort < OBSlice >(
                        k);
                if (i % 100 == 0) {
                    logger.info("Matching " + i);
                }

                OBSlice s = new OBSlice(line);
                if (IndexSmokeTUtil.shouldProcessSlice(s)) {
                    index.searchOB(s, range, x);
                    i++;
                }
                if (i == 5000) {
                    logger.warn("Finishing test at i : " + i);
                    break;
                }
            }
            
            re = r.readLine();
        }
        times++;
        }
       logger.info("r: "  +  range + " k " + k + " time: " + ((System.currentTimeMillis() - time) / 1000));
        }catch(UnsupportedOperationException e){
            logger.info("Skipping method");
        }
    }
    
    public static void bench(IndexShort < OBSlice > index, String query, String db) throws Exception{
        initIndex(index, query, db);
        index.resetStats();
        benchAux(index, query, (short)2, (byte)1 );
        benchAux(index, query, (short)2, (byte)1 );
        benchAux(index, query, (short)3, (byte)1 );
        benchAux(index, query, (short)3, (byte)3 );
        
        benchAux(index, query, (short)7, (byte)1 );
        benchAux(index, query, (short)7, (byte)3 );
        benchAux(index, query, (short)10, (byte)1 );
        benchAux(index, query, (short)10, (byte)3 );
        logger.debug("Real Distance count search: "+ OBSlice.count);
    }
}
