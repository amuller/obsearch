package net.obsearch.index;

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
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.cache.OBCache;
import net.obsearch.cache.OBCacheLoader;
import net.obsearch.cache.OBCacheLoaderLong;
import net.obsearch.cache.OBCacheLong;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.ob.MultiplicityAware;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.OBStore;
import net.obsearch.utils.bytes.ByteConversion;

import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreLong;

import com.thoughtworks.xstream.XStream;

/**
 * AbstractOBIndex contains functionality regarding object storage. Children of
 * this class should define a way of searching those objects. This class
 * provides default implementations of methods that are considered optional in
 * the Index interface.
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractOBIndex < O extends OB > implements Index < O > {

    /**
     * Statistics related to this index.
     */
    protected transient Statistics stats;

    /**
     * Objects are stored by their id's here.
     */
    protected transient OBStoreLong A;

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
     * True if this index is frozen.
     */
    private boolean isFrozen;

    /**
     * The type used to instantiate objects of type O.
     */
    private Class < O > type;

    /**
     * Constructors of an AbstractOBIndex should receive only parameters related
     * to the operation of the index. The factory and the initialization will be
     * executed by {@link #init(OBStoreFactory)}
     * @param type
     * @throws OBStorageException
     * @throws OBException
     */
    protected AbstractOBIndex(Class < O > type) throws OBStorageException,
            OBException {
        this.type = type;
    }
    
    /**
     * Returns the type of the object to be stored.
     * @return {@link #type}
     */
    protected final Class < O > getType(){
        return type;
    }
    
    /**
     * If the database is frozen returns silently if it is not throws
     * NotFrozenException.
     * @throws NotFrozenException
     *                 if the index has not been frozen.
     */
    protected void assertFrozen() throws NotFrozenException {
        if (!isFrozen()) {
            throw new NotFrozenException();
        }
    }

    /**
     * Initialize the index.
     * @throws OBStorageException
     * @throws OBException
     */
    public void init(OBStoreFactory fact) throws OBStorageException,
    OBException, NotFrozenException, 
    IllegalAccessException, InstantiationException, OBException {
        this.fact = fact;
        initStorageDevices();
        initCache();
        stats = new Statistics();
    }

    /**
     * Initializes storage devices required by this class.
     * @throws OBStorageException
     *                 If the storage device could not be created.
     */
    protected void initStorageDevices() throws OBStorageException {
        this.A = fact.createOBStoreLong("A", false,false);
        if (!this.isFrozen()) {
            this.preFreeze = fact.createOBStore("pre", true,false);
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

            ByteBuffer data = A.getValue(i);
            if (data == null) {
                throw new IllegalIdException(i);
            }

            return bytesToObject(data.array());
        }

    }
    
    /**
     * Instantiates an object O from the given data array.
     */
    protected O bytesToObject(ByteBuffer data) throws OBException,
    InstantiationException, IllegalAccessException, IllegalIdException {
        return bytesToObject(data.array());
    }

    /**
     * Instantiates an object O from the given data array.
     */
    protected O bytesToObject(byte[] data) throws OBException,
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
     * @see net.obsearch.result.Index#close()
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
     * @see net.obsearch.result.Index#databaseSize()
     */
    @Override
    public long databaseSize() throws OBStorageException {
        return A.size();
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#databaseSizeMSet()
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
     * remove the object from A if the result status is
     * {@link net.obsearch.Status#OK}.
     * @param object
     *                object to be deleted.
     * @return {@link net.obsearch.Status#OK} if the object was deleted.
     *         {@link net.obsearch.Status#NOT_EXISTS} no object matched.
     */
    protected abstract OperationStatus deleteAux(O object) throws OBException, IllegalAccessException,
    InstantiationException ;

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#deleteSingle(net.obsearch.result.OB)
     */
    public OperationStatus deleteSingle(O object) throws OBStorageException,
            OBException, IllegalAccessException, InstantiationException,
            NotFrozenException {
        if (object instanceof MultiplicityAware) {
            // find the id of the object.
            OperationStatus res = findAux(object);
            if (res.getStatus() == Status.OK) {
                O obj = getObject(res.getId());
                MultiplicityAware m = (MultiplicityAware) obj;
                if (m.getMultiplicity() == 1) {
                    // delete the object if multiplicity becomes 0 after this
                    // delete.
                    return delete(object);
                } else {
                    // reduce the multiplicity
                    m.decrementMultiplicity();
                    // update the object in the DB.
                    A.put(res.getId(), ByteConversion
                            .createByteBuffer(objectToBytes(obj)));
                    return res;
                }
            }
            return res;

        } else {
            return delete(object);
        }
    }
    
    

    /**
     * Converts an object into an array of bytes.
     * @param object
     *                Object to convert.
     * @return The bytes array representation of the object.
     */
    protected byte[] objectToBytes(O object) throws OBException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream outD = new DataOutputStream(out);
        try {
            object.store(outD);
            outD.close();
        } catch (IOException e) {
            throw new OBException(e);
        }

        return out.toByteArray();
    }

    protected ByteBuffer objectToByteBuffer(O object) throws OBException {
        return ByteConversion.createByteBuffer(objectToBytes(object));
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#getObject(long)
     */
    @Override
    public O getObject(long id) throws IllegalIdException,
            IllegalAccessException, InstantiationException, OBException {
        // get the object from A, this is easy.
        return aCache.get(id);
    }

    /**
     * Find the Id of the given object. (the distance 0 is considered as equal)
     * @param object
     *                The object to search
     * @return {@link net.obsearch.Status#OK} if the object is found (with
     *         the id) otherwise, {@link net.obsearch.Status#NOT_EXISTS}
     * @throws IllegalIdException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws OBException
     */
    protected  OperationStatus findAux(O object)
            throws IllegalIdException, IllegalAccessException,
            InstantiationException, OBException{
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#exists(net.obsearch.result.OB)
     */
    
   /* public OperationStatus exists(O object) throws OBStorageException,
            OBException, IllegalAccessException, InstantiationException {
        OperationStatus t = findAux(object);
        if (t.getStatus() == Status.OK) {
            t.setStatus(Status.EXISTS);
        }
        return t;
    }*/

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#getStats()
     */
    @Override
    public Statistics getStats() {
        return stats;
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#insert(net.obsearch.result.OB)
     */
    @Override
    public OperationStatus insert(O object) throws OBStorageException,
            OBException, IllegalAccessException, InstantiationException {
        OperationStatus res = new OperationStatus();
        res.setStatus(Status.OK);
        if (this.isFrozen()) {
            res = exists(object);
            if (res.getStatus() == Status.NOT_EXISTS) {
                // must insert object into A before the index is updated
                long id = A.nextId();
                this.A.put(id, objectToByteBuffer(object));
                // update the index:
                res = insertAux(id, object);
                res.setId(id);
            }
            // check again if the Status is Exists (there might have been an
            // insertion)
            if (res.getStatus() == Status.EXISTS) {
                if (object instanceof MultiplicityAware) {
                    O obj = getObject(res.getId());
                    MultiplicityAware m = (MultiplicityAware) obj;
                    m.incrementMultiplicity();
                    A.put(res.getId(), ByteConversion
                            .createByteBuffer(objectToBytes(obj)));

                }
                // no multiplicity, simply ignore the situation.
            }
        } else { // before freeze
            // we keep track of objects that have been inserted
            // based on their binary signature.
            // TODO: maybe change this to a hash to avoid the problem
            // with objects that have multiplicity.
            byte[] key = objectToBytes(object);
            ByteBuffer value = this.preFreeze.getValue(key);
            if (value == null) {
                long id = A.nextId();
                res.setId(id);
                preFreeze.put(key, ByteConversion.longToByteBuffer(id));
            } else {
                res.setStatus(Status.EXISTS);
                res.setId(ByteConversion.byteBufferToLong(value));
            }

            // insert the object in A if everything is OK.
            if (res.getStatus() == Status.OK) {
                this.A.put(res.getId(), objectToByteBuffer(object));
            }

        }
        return res;
    }

    /**
     * Inserts the given object into the particular index. The caller inserts
     * the actual object so the implementing class only has to worry about
     * adding the id in the aproppiate place inside the index.
     * @param id
     *                The id that will be used to insert the object.
     * @param object
     *                The object that will be inserted.
     * @return If {@link net.obsearch.Status#OK} or
     *         {@link net.obsearch.Status#EXISTS} then the result will hold
     *         the id of the inserted object and the operation is successful.
     *         Otherwise an exception will be thrown.
     * @throws OBStorageException
     * @throws OBException
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    protected abstract OperationStatus insertAux(long id, O object)
            throws OBStorageException, OBException, IllegalAccessException,
            InstantiationException;

    /**
     * @see net.obsearch.Index#freeze()
     */
    @Override
    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBStorageException, OutOfRangeException, OBException {
        if(isFrozen()){
            // TODO: allow indexes to freeze multiple times.
            throw new AlreadyFrozenException();
        }
        this.isFrozen = true;
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#getBox(net.obsearch.result.OB)
     */
    @Override
    public long getBox(O object) throws OBException {
        throw new UnsupportedOperationException();
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#isFrozen()
     */
    @Override
    public boolean isFrozen() {
        return this.isFrozen;
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#resetStats()
     */
    @Override
    public void resetStats() {
        stats.resetStats();
    }

    /*
     * (non-Javadoc)
     * @see net.obsearch.result.Index#totalBoxes()
     */
    @Override
    public long totalBoxes() {
        throw new UnsupportedOperationException();
    }

}
