package net.obsearch.storage.cuckoo;

public class Entry {
	private long offset;
	private int length;

	public long getOffset() {
		return offset;
	}

	public int getLength() {
		return length;
	}

	public Entry(long offset, int length) {
		super();
		this.length = length;
		this.offset = offset;
	}

	public void setNull() {
		offset = -1;
		length = -1;
	}

	public boolean isNull() {
		return offset == -1 && length == -1;
	}
	
	public boolean equals(Object obj){
		Entry other = (Entry)obj;
		return offset == other.offset && length == other.length;
	}
	
	public String toString(){
		return "Off: " + offset + " len: " + length;
	}
}
