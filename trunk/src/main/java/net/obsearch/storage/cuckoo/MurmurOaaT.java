package net.obsearch.storage.cuckoo;

public class MurmurOaaT extends Hash32 {

	@Override
	public long compute(byte[] data) {
		long res = super.murmur(data);
		res = res<<32;
		res = res | super.joaat(data);
		return res;
	}

}
