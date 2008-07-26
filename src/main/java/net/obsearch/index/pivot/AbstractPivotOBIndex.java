package net.obsearch.index.pivot;

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



import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import net.obsearch.storage.OBStoreFactory;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.AbstractOBIndex;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;

/** 
 *  AbstractPivotOBIndex defines abstract functionality for an index that
 *  uses n pivots to partition the data into more tractable subsets.
 *  To create a new index, do the following:
 *  1) Implement objectToProjectionBytes(O object) (smap an object and codify it)
 *  2) Implement deleteAux
 *  3) Implement insertAux
 *  4) Implement searchOB. That's all!
 *  @author  Arnoldo Jose Muller Molina    
 */
public abstract class AbstractPivotOBIndex < O extends OB >
        extends AbstractOBIndex < O > {
    
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractPivotOBIndex.class);
    
    /**
     * The pivot selector used by the index.
     */
    private IncrementalPivotSelector < O > pivotSelector;
    
    private int pivotCount;
    
    // TODO: in the future, make an array of arrays of pivots for indexes like D.
    /**
     * The pivots for this Tree. When we instantiate or de-serialize this object
     * we load them from {@link #pivotsBytes}.
     */
    protected transient O[] pivots;
   
    /**
     * Bytes of the pivots. From this array we can load the pivots into
     * {@link #pivots}.
     */
    protected byte[][] pivotsBytes;

    /**
     * Creates an index that uses pivots as its major data partitioning strategy.
     * @param type The type of object that will be stored.
     * @param pivotSelector The pivot selection strategy to be employed.
     * @param pivotCount The number of pivots that will be selected.
     * @throws OBStorageException
     * @throws OBException
     */
    protected AbstractPivotOBIndex(Class < O > type, IncrementalPivotSelector < O > pivotSelector, int pivotCount) throws OBStorageException,
            OBException {
        super(type);
        OBAsserts.chkAssert(pivotCount > 0, "Pivot count must be > 0");
        this.pivotCount = pivotCount;
        this.pivotSelector = pivotSelector;       
    }

    @Override
    public void freeze() throws IOException, AlreadyFrozenException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBStorageException, OutOfRangeException, OBException {
        super.freeze();
        selectPivots(getPivotCount());
       
    }
    
   
    
    /**
     * Override this method if selection must be changed
     */
    protected PivotResult  selectPivots(int pivotCount) throws IOException, AlreadyFrozenException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBStorageException, OutOfRangeException, OBException{
        // select pivots.
        LongArrayList elementsSource = null;
        try{
        PivotResult pivots = this.pivotSelector.generatePivots(pivotCount,
                elementsSource, this);       
        // store the pivots selected for serialization.
        this.storePivots(pivots.getPivotIds());
        return pivots;
        }catch(PivotsUnavailableException e){
            throw new OBException(e);
        }
        
    }
    
    /**
     * Loads the pivots from the pivots array {@link #pivotsBytes} and puts them
     * in {@link #pivots}.
     * @throws NotFrozenException
     *                 if the freeze method has not been invoqued.
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    protected void loadPivots() throws NotFrozenException,
            IllegalAccessException, InstantiationException, OBException {
        
        if (!isFrozen()) {
            throw new NotFrozenException();
        }       
       this.pivots = loadPivots(this.pivotsBytes);
    }
    
    /**
     * Stores the given pivots in a local array. Takes the pivots from the
     * database using the given ids.
     * @param ids
     *                Ids of the pivots that will be stored.
     * @throws IllegalIdException
     *                 If the pivot selector generates invalid ids
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    protected void storePivots(final long[] ids) throws IllegalIdException,
            IllegalAccessException, InstantiationException,
            OBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Pivots selected " + Arrays.toString(ids));

        }
        createPivotsArray(ids.length);
        assert ids.length == pivots.length && pivots.length == this.getPivotCount();
        this.pivotsBytes = serializePivots(ids);
        int i = 0;
        for(long id : ids){
            O obj = getObject(id);
            this.pivots[i] = obj;
            i++;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Detail: " + Arrays.toString(pivots));
        }
    }
    
    
    /**
     * Creates an array with the pivots. It has to be created like this because
     * we are using generics. Subclasses must override this if different
     * sets of pivots are to be used. We can override this later to have
     * a multiple set of pivots for indexes like D... 
     */
    protected void createPivotsArray(int size) {
        this.pivots = emptyPivotsArray(size);
    }

    /**
     * @return The number of pivots used in this index.
     */
    protected int getPivotCount(){
        return pivotCount;
    }
    
    
    

    public void init(OBStoreFactory fact) throws OBStorageException,
    OBException, NotFrozenException, 
    IllegalAccessException, InstantiationException, OBException{
        super.init(fact);
        if(isFrozen()){
            this.loadPivots();
        }
    }
}
