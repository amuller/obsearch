package net.obsearch.storage;

public class OBStorageConfig {
	
	private boolean temp = false;
	private boolean duplicates = false; 
	private boolean bulkMode = false;
	/**
	 * Size of the records that will be stored
	 * Some storage devices require this information or
	 * will behave differently if this information
	 * is given.
	 * 
	 */
	private int recordSize = -1;
	
	public boolean isTemp() {
		return temp;
	}
	public void setTemp(boolean temp) {
		this.temp = temp;
	}
	public boolean isDuplicates() {
		return duplicates;
	}
	public void setDuplicates(boolean duplicates) {
		this.duplicates = duplicates;
	}
	public boolean isBulkMode() {
		return bulkMode;
	}
	public void setBulkMode(boolean bulkMode) {
		this.bulkMode = bulkMode;
	}
	public int getRecordSize() {
		return recordSize;
	}
	public void setRecordSize(int recordSize) {
		this.recordSize = recordSize;
	}
	
	
}
