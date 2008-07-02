package net.obsearch;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import net.obsearch.ob.MultiplicityAware;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.OperationStatus;
import org.ajmm.obsearch.Status;
import org.ajmm.obsearch.cache.OBCache;
import org.ajmm.obsearch.cache.OBCacheLoader;
import org.ajmm.obsearch.cache.OBCacheLoaderLong;
import org.ajmm.obsearch.cache.OBCacheLong;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.stats.Statistics;
import org.ajmm.obsearch.storage.OBStore;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreLong;

/**
 * AbstractOBIndex contains functionality regarding object storage. The search
 * index should be implemented deeper in the hierarchy.
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractOBIndex < O extends OB > implements Index < O > {

    /**
     * Objects are stored by their id's here.
     */
    private transient OBStoreLong A;

    /**
     * Factory used by this class and by subclasses to create appropiate storage
     * devices.
     */
    protected transient OBStoreFactory fact;

    /**
     * Required during pre-freeze to make only one copy of an object is
     * inserted. (the index is not built at this stage therefore it is not
     * possible to know if an object is already in the DB.
     */
    private transient OBStore preFreeze;

    /**
     * Cache used for storing recently accessed objects O.
     */
    private transient OBCacheLong < O > aCache;

    /**
     * The type used to instantiate objects of type O.
     */
    private Class < O > type;

    protected AbstractOBIndex(OBStoreFactory fact, Class < O > type)
            throws OBStorageException, OBException {
        this.fact = fact;
        this.type = type;
        init();
    }

    /**
     * Initialize the index.
     * @throws OBStorageException
     * @throws OBException
     */
    protected void init() throws OBStorageException, OBException {
        initStorageDevices();
        initCache();
    }

    /**
     * Initializes storage devices required by this class.
     * @throws OBStorageException
     *                 If the storage device could not be created.
     */
    protected void initStorageDevices() throws OBStorageException {
        this.A = fact.createOBStoreLong("A", false);
        if (!this.isFrozen()) {
            this.preFreeze = fact.createOBStore("pre", true);
        }
    }

    /**
     * Initializes the object cache {@link #aCache}.
     * @throws DatabaseException
     *                 If something goes wrong with the DB.
     */
    protected void initCache() throws OBException {
        aCache = new OBCacheLong < O >(new ALoader());
    }

    /**
     * This class is in charge of loading objects.
     * @author amuller
     */
    private class ALoader implements OBCacheLoaderLong < O > {

        public long getDBSize() throws OBStorageException {
            return A.size();
        }

        public O loadObject(long i) throws OBException, InstantiationException,
                IllegalAccessException, IllegalIdException {

            byte[] data = A.getValue(i);
            if (data == null) {
                throw new IllegalIdException(i);
            }

            return instantiateObject(data);
        }

    }

    /**
     * Instantiates an object O from the given data array.
     */
    protected O instantiateObject(byte[] data) throws OBException,
            InstantiationException, IllegalAccessException, IllegalIdException {
        O res = type.newInstance();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        DataInputStream ind = new DataInputStream(in);
        try {
            res.load(ind);
        } catch (IOException e) {
            throw new OBException(e);
        }
        return res;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#close()
     */
    @Override
    public void close() throws OBException {
        A.close();
        if (this.preFreeze != null) {
            preFreeze.close();
        }
        fact.close();
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#databaseSize()
     */
    @Override
    public long databaseSize() throws OBStorageException {
        return A.size();
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#databaseSizeMSet()
     */
    @Override
    public long databaseSizeMSet() throws OBStorageException {
        throw new UnsupportedOperationException();
        // return 0;
    }

    @Override
    public OperationStatus delete(O object) throws OBException,
            IllegalAccessException, InstantiationException, NotFrozenException {
        if (this.isFrozen()) {
            OperationStatus res = deleteAux(object);
            if (res.getStatus() == Status.OK) {                
                this.A.delete(res.getId());
            }
            return res;
        } else {
            throw new NotFrozenException();
        }
    }

    /**
     * Deletes the entry of this object in the index. The current class will
     * remove the object from A if the result status is {@link org.ajmm.obsearch.Status#OK}.
     * @param object
     *                object to be deleted.
     * @return {@link org.ajmm.obsearch.Status#OK} if the object was deleted.
     *         {@link org.ajmm.obsearch.Status#NOT_EXISTS} no object matched.
     */
    protected abstract OperationStatus deleteAux(O object);

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#deleteSingle(org.ajmm.obsearch.OB)
     */
    @Override
    public OperationStatus deleteSingle(O object) throws OBStorageException,
            OBException, IllegalAccessException, InstantiationException,
            NotFrozenException {
        if(object instanceof MultiplicityAware){
            // find the id of the object.
            OperationStatus res  = findAux(object);
            if(res.getStatus() == Status.OK){
                O  obj = getObject(res.getId());
                MultiplicityAware m = (MultiplicityAware) obj;
                if(m.getMultiplicity() == 1){
                    // delete the object if multiplicity becomes 0 after this delete.
                    return delete(object);
                }else{
                    // reduce the multiplicity
                    m.decrementMultiplicity();
                    // update the object in the DB.
                    A.put(res.getId(), objectToBytes(obj));
                    return res;
                }
            }
            return res;
            
        }else{
            return delete(object);
        }
    }
    
    /**
     * Converts an object into an array of bytes.
     * @param object Object to convert.
     * @return The bytes array representation of the object.
     */
    protected byte[] objectToBytes(O object) throws OBException{
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream outD = new DataOutputStream(out);
        try{
            object.store(outD);
            outD.close();
        }catch(IOException e){
            throw new OBException(e);
        }
        
        return out.toByteArray();
    }
    
    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#getObject(long)
     */    
    @Override
    public O getObject(long id) throws  IllegalIdException,
            IllegalAccessException, InstantiationException, OBException {
        // get the object from A, this is easy.
        return aCache.get(id);
    }    
    
    
    /**
     * Find the Id of the given object. (the distance 0 is considered as equal)
     * @param object The object to search
     * @return {@link #org.ajmm.obsearch.Status.OK} if the object is found (with the id)
     *                otherwise, {@link org.ajmm.obsearch.Status.NOT_EXISTS}
     * @throws IllegalIdException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws OBException
     */
    protected abstract OperationStatus  findAux(O object) throws  IllegalIdException,
            IllegalAccessException, InstantiationException, OBException ;
    
        
    

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#exists(org.ajmm.obsearch.OB)
     */
    @Override
    public OperationStatus exists(OB object) throws OBStorageException,
            OBException, IllegalAccessException, InstantiationException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#freeze()
     */
    @Override
    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBStorageException, OutOfRangeException, OBException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#getBox(org.ajmm.obsearch.OB)
     */
    @Override
    public int getBox(OB object) throws OBException {
        // TODO Auto-generated method stub
        return 0;
    }

    

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#getStats()
     */
    @Override
    public Statistics getStats() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#insert(org.ajmm.obsearch.OB)
     */
    @Override
    public OperationStatus insert(OB object) throws OBStorageException,
            OBException, IllegalAccessException, InstantiationException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#isFrozen()
     */
    @Override
    public boolean isFrozen() {
        // TODO Auto-generated method stub
        return false;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#readObject(java.io.DataInputStream)
     */
    @Override
    public O readObject(DataInputStream in) throws InstantiationException,
            IllegalAccessException, OBException {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#relocateInitialize(java.io.File)
     */
    @Override
    public void relocateInitialize(File dbPath) throws OBStorageException,
            NotFrozenException, IllegalAccessException, InstantiationException,
            OBException, IOException {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#resetStats()
     */
    @Override
    public void resetStats() {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#toXML()
     */
    @Override
    public String toXML() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * @see org.ajmm.obsearch.Index#totalBoxes()
     */
    @Override
    public int totalBoxes() {
        // TODO Auto-generated method stub
        return 0;
    }

}
