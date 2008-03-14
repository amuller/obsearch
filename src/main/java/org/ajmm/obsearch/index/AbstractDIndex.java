package org.ajmm.obsearch.index;

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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.ajmm.obsearch.AbstractOBPriorityQueue;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.Result;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OBStorageException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.ajmm.obsearch.storage.OBStoreInt;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;

/**
 * AbstractDIndex contains functionality common to specializations of the
 * D-Index for primitive types.
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractDIndex < O extends OB > implements Index < O > {

    /**
     * The pivot selector used by the index.
     */
    private PivotSelector < O > pivotSelector;

    /**
     * The type used to instantiate objects of type O.
     */
    private Class < O > type;

    /**
     * Tells if this index is frozen or not.
     */
    private boolean frozen = false;

    /**
     * Pivots will be stored here for quick access.
     */
    protected transient O[][] pivots;

    /**
     * We store here the pivots when we want to de-serialize/ serialize them.
     */
    private byte[][][] pivotBytes;

    /**
     * Factory used by this class and by subclasses to create appropiate storage
     * devices.
     */
    protected transient OBStoreFactory fact;

    // TODO: We have to quickly move to longs.
    /**
     * Objects are stored by their id's here.
     */
    protected transient OBStoreInt A;

    /**
     * Initializes this abstract class.
     * @param fact
     *                The factory that will be used to create storage devices.
     * @param pivotCount #
     *                of Pivots that will be used.
     * @param pivotSelector
     *                The pivot selection mechanism to be used. (64 pivots can
     *                create 2^64 different buckets, so we have limited the # of
     *                pivots available. If you need more pivots, then you would
     *                have to use the Smapped P+Tree (PPTree*).
     * @param type
     *                The type that will be stored in this index.
     * @param nextLevelThreshold
     *                How many pivots will be used on each level of the hash
     *                table.
     * @throws OBStorageException
     *                 if a storage device could not be created for storing
     *                 objects.
     */
    protected AbstractDIndex(OBStoreFactory fact, byte pivotCount,
            PivotSelector < O > pivotSelector, Class < O > type,
            float nextLevelThreshold) throws OBStorageException {
        this.pivotSelector = pivotSelector;
        this.type = type;
        initStorageDevices();
    }

    /**
     * Initializes storage devices required by this class.
     * @throws OBStorageException
     *                 If the storage device could not be created.
     */
    protected void initStorageDevices() throws OBStorageException {
        this.A = fact.createOBStoreInt("A", false);
        initSpecializedStorageDevices();
    }

    /**
     * This method will be called by {@link #initSpecializedStorageDevices()} to
     * initialize other storage devices required by the subclasses. If your
     * subclass is not an immediate subclass of
     * {@link #AbstractDIndex(OBStoreFactory, short, PivotSelector, Class)},
     * then you should always call super.
     * {@link #initSpecializedStorageDevices()} after initializing your device.
     * @throws OBStorageException
     *                 If the storage device could not be created.
     */
    protected abstract void initSpecializedStorageDevices()
            throws OBStorageException;

    /**
     * Subclasses must call this method after they have closed the storage
     * devices they created.
     */
    public void close() throws OBStorageException {
        A.close();
        fact.close();
    }

    @Override
    public int databaseSize() throws OBStorageException {
        // TODO: fix this, change the interface so that we support long number
        // of objects.
        return (int) A.size();
    }

    @Override
    public Result delete(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException, NotFrozenException {
        // get the bucket, erase the object from the bucket and return the
        // bucket.
        return null;
    }

    @Override
    public Result exists(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        // get the bucket, and check if the object is there.
        return null;
    }

    @Override
    public void freeze() throws IOException, AlreadyFrozenException,
            IllegalIdException, IllegalAccessException, InstantiationException,
            DatabaseException, OutOfRangeException, OBException {
        // get n pivots. 
        // ask the bucket for each object and insert those who are not excluded.
        // repeat iteratively with the objects that could not be inserted until the remaining
        // objects are small enough.
        
        frozen = true;
    }
    
    /**
     * Returns the bucket information for the given object. 
     * It will be ca
     * @param object
     * @param level
     * @return
     */
    protected abstract Bucket getBucket(O object, int level );

    @Override
    public int getBox(O object) throws OBException {
        // Calculate the bucket of the given object.
        return 0;
    }

    @Override
    public O getObject(int i) throws DatabaseException, IllegalIdException,
            IllegalAccessException, InstantiationException, OBException {
        // get the object from A, this is easy.
        return null;
    }

    @Override
    public Result insert(O object) throws DatabaseException, OBException,
            IllegalAccessException, InstantiationException {
        // need an object bucket that will always be sorted, and that will find
        // efficiently objects based on the first pivot.
        // TODO: fix this!!!
        int nextId = (int) A.nextId();

        TupleOutput out = new TupleOutput();
        object.store(out);
        this.A.put(nextId, out.getBufferBytes());

        if (this.isFrozen()) {
            // store the SMAP vector.
        }
        return null;
    }

    @Override
    public boolean isFrozen() {
        return frozen;
    }

    @Override
    public O readObject(TupleInput in) throws InstantiationException,
            IllegalAccessException, OBException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void relocateInitialize(File dbPath) throws DatabaseException,
            NotFrozenException, IllegalAccessException, InstantiationException,
            OBException, IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public String toXML() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public int totalBoxes() {
        // TODO Auto-generated method stub
        return 0;
    }

}
