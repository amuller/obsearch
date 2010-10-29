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



import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleLong;

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
	
	private static int MAX_PIVOT_SAMPLE = 500000;
    
    /**
     * Logger.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractPivotOBIndex.class);
    
    /**
     * The pivot selector used by the index.
     */
    protected IncrementalPivotSelector < O > pivotSelector;
    
    private int pivotCount;
    
    // TODO: in the future, make an array of arrays of pivots for indexes like D.
    /**
     * The pivots for this Tree. When we instantiate or de-serialize this object
     * we load them from {@link #pivotsBytes}.
     */
    protected  O[] pivots;
   
    /**
     * Use the following amount of pairs when the intrinsic dimensionality
     * is calculated.
     */
    protected int intrinsicDimensionalityPairs = 1000000;

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
        OBAsserts.chkAssert(pivotCount >= 0, "Pivot count must be >= 0");
        this.pivotCount = pivotCount;
        this.pivotSelector = pivotSelector;       
    }
    
    protected AbstractPivotOBIndex(){
    	super();
    }

    @Override
    public void freeze() throws  AlreadyFrozenException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBStorageException, OutOfRangeException, OBException, PivotsUnavailableException, IOException {
        super.freeze();
        if(pivotCount > 0){
        	pivots = getObjects(selectPivots(pivotCount, pivotSelector).getPivotIds());
        }
    }
    
    /**
     * Calculates the intrinsic dimensionality of the inserted dataset.
     * @return
     * @throws IllegalIdException
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @throws OBException
     */
    protected double calculateIntrinsicDimensionality() throws IllegalIdException, IllegalAccessException, InstantiationException, OBException{
    	StaticBin1D stats = new StaticBin1D();
    	Random r = new Random();
    	long max = databaseSize();
    	for(int i = 0 ; i < this.intrinsicDimensionalityPairs ; i++){
    		O a = getObject(r.nextInt((int)max));
    		O b = getObject(r.nextInt((int)max));
    		stats.add(distance(a,b));
    	}
    	logger.info(stats.toString());
    	return Math.pow(stats.mean(),2) / (2 * stats.variance());
    }
    
    
    
    /**
     * Override this method if selection must be changed
     * @throws IOException 
     */
    protected PivotResult  selectPivots(int pivotCount, IncrementalPivotSelector < O > pivotSelector) throws  AlreadyFrozenException,
    IllegalIdException, IllegalAccessException, InstantiationException,
    OBStorageException, OutOfRangeException, OBException, IOException{
        // select pivots.
    	OBAsserts.chkAssert(A.size() <= Integer.MAX_VALUE, "Cannot accept more than " + Integer.MAX_VALUE + " on freeze");
    	/*int max = Math.min( (int)A.size(), MAX_PIVOT_SAMPLE);
        LongArrayList elementsSource = new LongArrayList(max);

        int i = 0;
        while( i < max){
        	elementsSource.add(i);
        	i++;
        }*/
        try{
        PivotResult pivots = pivotSelector.generatePivots(pivotCount,
                null, this);       
        // store the pivots selected for serialization.
        //this.storePivots(pivots.getPivotIds());
        return pivots;
        }catch(PivotsUnavailableException e){
            throw new OBException(e);
        }
        
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
    protected O[] getObjects(final long[] ids) throws IllegalIdException,
            IllegalAccessException, InstantiationException,
            OBException {
        if (logger.isDebugEnabled()) {
           // logger.debug("Pivots selected " + Arrays.toString(ids));

        }
        O[] p  = emptyPivotsArray(ids.length);
        int i = 0;
        for(long id : ids){
            O obj = getObject(id);
            p[i] = obj;
            i++;
        }
        if (logger.isDebugEnabled()) {
           // logger.debug("Detail: " + Arrays.toString(p));
        }
        return p;
        
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
    public int getPivotCount(){
        return pivotCount;
    }
    
    
    

    public void init(OBStoreFactory fact) throws OBStorageException,
    OBException, NotFrozenException, 
    IllegalAccessException, InstantiationException, OBException{
        super.init(fact);
//        if(isFrozen()){
//            this.loadPivots();
//       }
    }
}
