package org.ajmm.obsearch;
/**
 * A container used to return an OB and its insertion or deletion
 * timestamp
 * 
 *
 */
public class TimeStampResult<O extends OB> {
	
	private O object;
	private long timestamp;
	private boolean insert;
	
	
	/**
	 * Creates a time stamp result where the object and its associated
	 * timestamp are kept. We also have a flag that indicates if
	 * the object was inserted or deleted
	 * @param object
	 * @param timestamp
	 * @param insert
	 */
	public TimeStampResult(O object, long timestamp, boolean insert) {
		super();
		this.object = object;
		this.timestamp = timestamp;
		this.insert = insert;
	}
	
	public boolean isInsert() {
	    return insert;
	}
	public void setInsert(boolean insert) {
	    this.insert = insert;
	}
	
	public O getObject() {
		return object;
	}
	public void setObject(O object) {
		this.object = object;
	}
	public long getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}
	

}
