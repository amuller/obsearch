package net.obsearch.storage.cuckoo;

public class Tuple {
	private long id;
	private byte[] entry;

	public long getId() {
		return id;
	}

	public byte[] getEntry() {
		return entry;
	}

	public Tuple(long id, byte[] entry) {
		super();
		this.entry = entry;
		this.id = id;
	}

}

