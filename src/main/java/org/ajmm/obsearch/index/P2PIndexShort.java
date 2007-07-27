package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;

import net.jxta.exception.PeerGroupException;

import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;

import com.sleepycat.je.DatabaseException;

public class P2PIndexShort<O extends OBShort> extends AbstractP2PIndex<O> implements IndexShort<O> {

	// we keep two different references of the same object
	// to avoid casts
	IndexShort<O> index;
	SynchronizableIndex<O> syncIndex;
	
	/**
	 * Creates a P2P Index short
	 * The provided index must be SynchronizableIndex and also 
	 * implement IndexShort. The index must be frozen.
	 * The number of boxes served will be the maximum number of boxes serviced by the index
	 * @param index
	 * @param dbPath place were to put p2p related data
	 * @param clientName the name 
	 * @throws IOException
	 * @throws PeerGroupException
	 * @throws OBException 
	 */
	public P2PIndexShort(SynchronizableIndex<O> index, File dbPath, String clientName) throws  IOException,
	PeerGroupException, OBException, NotFrozenException {
		this(index,dbPath,clientName, index.totalBoxes());
	}
	/**
	 * Creates a P2P Index short
	 * The provided index must be SynchronizableIndex and also 
	 * implement IndexShort. The index must be frozen.
	 * @param index
	 * @param dbPath place were to put p2p related data
	 * @param clientName the name 
	 * @param boxesToServe # of boxes that will be served
	 * @throws IOException
	 * @throws PeerGroupException
	 * @throws OBException 
	 */
	public P2PIndexShort(SynchronizableIndex<O> index, File dbPath, String clientName, int boxesToServe) throws IOException,
	PeerGroupException, OBException, NotFrozenException {
		super(index, dbPath, clientName, boxesToServe);
		if(!  (index instanceof IndexShort)){
			throw new OBException("Expecting an IndexShort");
		}
		if(! index.isFrozen()){
			throw new NotFrozenException();
		}
		this.index = (IndexShort<O>)index;
		this.syncIndex = index;
		
	}
	
	protected SynchronizableIndex<O> getIndex(){
		return syncIndex;
	}
	
	/**
	 * Perform a distributed search in the network
	 * @param object
	 * @param r
	 * @param result
	 * @throws NotFrozenException
	 * @throws DatabaseException
	 * @throws InstantiationException
	 * @throws IllegalIdException
	 * @throws IllegalAccessException
	 * @throws OutOfRangeException
	 * @throws OBException
	 */
	public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
			throws NotFrozenException, DatabaseException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		// TODO Auto-generated method stub

	}
	
	



}
