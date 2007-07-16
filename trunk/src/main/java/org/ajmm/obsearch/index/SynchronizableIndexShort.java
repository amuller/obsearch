package org.ajmm.obsearch.index;

import java.io.File;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;

import com.sleepycat.je.DatabaseException;

public class SynchronizableIndexShort<O extends OBShort> extends AbstractSynchronizableIndex<O> implements
		IndexShort<O> {
	
	protected IndexShort<O> source;
	
	public SynchronizableIndexShort(IndexShort<O> source, File dbDir) throws DatabaseException{		
		super(source,dbDir);
		this.source = source;
	}

	public void searchOB(O object, short r, OBPriorityQueueShort result)
			throws NotFrozenException, DatabaseException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		source.searchOB(object, r, result);
	}
	
	public Index<O> getIndex(){
		return source;
	}

}
