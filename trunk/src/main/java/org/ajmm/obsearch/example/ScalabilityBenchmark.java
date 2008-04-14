package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.cache.OBStringFactory;
import org.ajmm.obsearch.index.DPrimeIndexShort;
import org.ajmm.obsearch.index.pivotselection.AcceptAll;
import org.ajmm.obsearch.index.pivotselection.IncrementalBustosNavarroChavezShort;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.storage.bdb.BDBFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class ScalabilityBenchmark
        extends Benchmark<OBString> {
    
    private static final Logger logger = Logger.getLogger("Scala");

    
    private File dbFolder;
    private String query;
    private String db;
    public ScalabilityBenchmark(File dbFolder, String query, String db){
        super(new OBStringFactory());
        this.dbFolder = dbFolder;
        this.query = query;
        this.db = db;
    }
    
    public void doIt(byte pivots) throws Exception{
        
        doIt(100000, pivots);
        doIt(200000, pivots);
        doIt(300000, pivots);
        doIt(400000, pivots);
        doIt(500000, pivots);
        doIt(600000, pivots);
        doIt(700000, pivots);
        doIt(800000, pivots);
        doIt(900000, pivots);
        doIt(1000000, pivots);
    }
    
    private void doIt(int maxElements, byte pivots) throws Exception{
        logger.info("Doing" + maxElements + " pivots: " + pivots);
        Directory.deleteDirectory(dbFolder);
        dbFolder.mkdirs();
        super.MAX_DATA = maxElements;
        
        BDBFactory fact = new BDBFactory(dbFolder);
        IncrementalBustosNavarroChavezShort<OBString> ps = new
        IncrementalBustosNavarroChavezShort<OBString>(new AcceptAll<OBString>(),
                 1000, 1000);
        DPrimeIndexShort < OBString > index = new DPrimeIndexShort < OBString >(
                fact, (byte)pivots, ps, OBString.class, (short) 3);
        
        super.initIndex(index, query, db);
        
        
        search(index, query);

    }
    
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
            Byte pivots = Byte.parseByte(args[3]);
            logger.info("Doing pivots: " + pivots);
            ScalabilityBenchmark  s = new ScalabilityBenchmark(dbFolder, query, dbData);
            s.doIt(pivots);
        }catch(Exception e){
            logger.fatal(e);
        }
        
    }

}
