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
	
	public TimeStampResult(O object, long timestamp) {
		super();
		this.object = object;
		this.timestamp = timestamp;
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
