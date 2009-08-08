package net.obsearch.storage.cuckoo;

public class JenkinsMurmur extends Hash32 {

	@Override
	public long compute(byte[] data) {
		long res = super.jenkins(data);
		res = res<<32;
		res = res | super.murmur(data);
		return res;
	}

}
