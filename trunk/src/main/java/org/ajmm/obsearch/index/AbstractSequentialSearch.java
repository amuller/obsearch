package org.ajmm.obsearch.index;

import hep.aida.bin.StaticBin1D;

import java.io.File;
import java.io.IOException;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.Status;
import org.ajmm.obsearch.cache.OBCache;
import org.ajmm.obsearch.cache.OBCacheLoader;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.utils.StatsUtil;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreInt;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

public class AbstractSequentialSearch<O extends OB> implements Index<O> {
    
    protected transient OBStoreFactory fact;
    
    /**
     * The type used to instantiate objects of type O.
     */
    private Class < O > type;
    
    protected transient OBStoreInt A;
    
    protected long distanceComputations = 0;
    
   
    
    private StaticBin1D dataSize = new StaticBin1D();
    
    /**
     * Cache used for storing recently accessed objects O.
     */
    protected transient OBCache < O > aCache;
    
    protected AbstractSequentialSearch(OBStoreFactory fact, Class < O > type) throws OBStorageException, OBException {
        this.fact = fact;
        this.type = type;
        init();
    }
    
    private void init() throws OBStorageException, OBException {
        A = fact.createOBStoreInt("A", false);
        if (aCache == null) {
            aCache = new OBCache < O >(new ALoader());
        }
    }

    @Override
    public void close() throws DatabaseException, OBException {
        // TODO Auto-generated method stub

    }

    @Override
    public int databaseSize() throws DatabaseException, OBStorageException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public Result delete(OB object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException, NotFrozenException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Result exists(OB object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getBox(OB object) throws OBException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public O getObject(int i) throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException, OBException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getStats() {
        StringBuilder sb = new StringBuilder();
        sb.append("Distance Computations: " + distanceComputations + "\n");
        sb.append(StatsUtil.mightyIOStats("Object sizes", this.dataSize));
        return sb.toString();
    }

    @Override
    public Result insert(OB object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        // TODO Auto-generated method stub
        TupleOutput out = new TupleOutput();
        object.store(out);
        int id = (int) A.nextId();
        byte [] data = out.getBufferBytes();
        this.dataSize.add(data.length);
        this.A.put(id, data);
        Result res = new Result();
        res.setStatus(Status.OK);
        return res;
    }

    @Override
    public boolean isFrozen() {
        return true;
    }

    @Override
    public O readObject(TupleInput in) throws InstantiationException,
            IllegalAccessException, OBException {
        O res = type.newInstance();
        res.load(in);
        return res;
    }

    @Override
    public void relocateInitialize(File dbPath) throws DatabaseException,
            NotFrozenException, IllegalAccessException, InstantiationException,
            OBException, IOException {
      
    }

    @Override
    public void resetStats() {
        distanceComputations = 0;
    }

    @Override
    public String toXML() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int totalBoxes() {        
        return 0;
    }
    
    private class ALoader implements OBCacheLoader < O > {

        public int getDBSize() throws OBStorageException {
            return (int) A.size();
        }

        public O loadObject(int i) throws DatabaseException,
                OutOfRangeException, OBException, InstantiationException,
                IllegalAccessException, IllegalIdException {
            O res = type.newInstance();
            byte[] data = A.getValue(i);
            if (data == null) {
                throw new IllegalIdException(i);
            }
            TupleInput in = new TupleInput(data);
            res.load(in);
            return res;
        }

    }

}
