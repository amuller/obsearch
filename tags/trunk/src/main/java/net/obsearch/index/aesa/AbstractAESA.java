package net.obsearch.index.aesa;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.stats.Statistics;

/**
 * In-memory AESA class
 * @author amuller
 *
 * @param <O>
 */
public abstract class AbstractAESA<O extends OB>  {

	protected List<O> objects;
	private boolean frozen = false;
	protected Statistics stats;
	private Class<O> type;
	public AbstractAESA(Class<O> type, int expectedSize){
		objects = new ArrayList<O>(expectedSize);
		resetStats();
		this.type = type;
	}
	
	public void resetStats(){
		stats = new Statistics();
	}
	
	protected O get(int i){
		return objects.get(i);
	}
	
	public OperationStatus insert(O object){
		objects.add(object);
		return new OperationStatus(Status.OK);
	}
	
	public int size(){
		return objects.size();
	}
	
	public void freeze() throws OBException{
		OBAsserts.chkAssert(! frozen, "Cannot freeze twice!");
		frozen = true;
		prepare();
	}
	
	
	public void loadObject(long i, O object) throws OBException {
		OBAsserts.chkAssert(i <= Integer.MAX_VALUE, "int exceeded");
		try {
			object.load(objects.get((int)i).store());
		} catch (IOException e) {
			throw new OBException(e);
		}
	}
	public long databaseSize() throws OBStorageException {
		// TODO Auto-generated method stub
		return objects.size();
	}
	
	
	public Class<O> getType() {
		return type;
	}
	
	public Statistics getStatistics(){
		return stats;
	}
	/**
	 * compute all the distances 
	 * @throws OBException 
	 */
	protected abstract void prepare() throws OBException;
}
