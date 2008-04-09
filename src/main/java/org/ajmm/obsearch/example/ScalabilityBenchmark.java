package org.ajmm.obsearch.example;

import java.io.File;

import org.ajmm.obsearch.cache.OBStringFactory;
import org.ajmm.obsearch.index.DPrimeIndexShort;
import org.ajmm.obsearch.index.pivotselection.AcceptAll;
import org.ajmm.obsearch.index.pivotselection.IncrementalBustosNavarroChavezShort;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.storage.bdb.BDBFactory;

public class ScalabilityBenchmark
        extends Benchmark<OBString> {
    
    private File dbFolder;
    private String query;
    private String db;
    public ScalabilityBenchmark(File dbFolder, String query, String db){
        super(new OBStringFactory());
        this.dbFolder = dbFolder;
        this.query = query;
        this.db = db;
    }
    
    public void doIt() throws Exception{
        
        doIt(100000);
        doIt(200000);
        doIt(300000);
        doIt(400000);
        doIt(500000);
        doIt(600000);
        doIt(700000);
        doIt(800000);
        doIt(900000);
        doIt(1000000);
    }
    
    private void doIt(int maxElements) throws Exception{
        Directory.deleteDirectory(dbFolder);
        dbFolder.mkdirs();
        super.MAX_DATA = maxElements;
        
        BDBFactory fact = new BDBFactory(dbFolder);
        IncrementalBustosNavarroChavezShort<OBString> ps = new
        IncrementalBustosNavarroChavezShort<OBString>(new AcceptAll<OBString>(),
                 1000, 1000);
        DPrimeIndexShort < OBString > index = new DPrimeIndexShort < OBString >(
                fact, (byte)18, ps, OBString.class, (short) 3);
        
        super.initIndex(index, query, db);
        
        
        search(index, query);

    }

}
