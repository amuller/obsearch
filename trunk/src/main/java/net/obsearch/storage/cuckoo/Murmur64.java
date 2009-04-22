package net.obsearch.storage.cuckoo;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class Murmur64 implements HashFunction {
	public static ByteOrder order = ByteOrder.nativeOrder();
	@Override
	public long compute(byte[] data) {
		return computeAux(data, data.length, 1);
	}
	
	/**
	 * Return a long from the given data array
	 * @param key
	 * @param offset
	 * @return
	 */
	private long getLong(byte[] key, int offset){
		long res = 0;
		res = res | key[offset]; // 0
		offset++;
		res = res << 8;
		res = res | key[offset]; // 1
		offset++;
		res = res << 8;
		res = res | key[offset]; // 2
		offset++;
		res = res << 8;
		res = res | key[offset]; // 3
		offset++;
		res = res << 8;
		res = res | key[offset]; // 4
		offset++;
		res = res << 8;
		res = res | key[offset]; // 5
		offset++;
		res = res << 8;
		res = res | key[offset]; // 6
		offset++;
		res = res << 8;
		res = res | key[offset]; // 7
		
		return res;
	}
	
	
	private long computeAux(byte[] key, int len, int seed){

		final long m = 0xc6a4a7935bd1e995L;
		final int r = 47;

		long h = seed ^ (len * m);

		//const uint64_t * data = (const uint64_t *)key;
		//const uint64_t * end = data + (len/8);
		
		int i = 0;
		int end = (len/8) * 8;
		while(i != end)
		{
			long k = getLong(key, i);

			k *= m; 
			k ^= k >> r; 
			k *= m; 
			
			h ^= k;
			h *= m;
			
			i += 8;
		}
		int pos = i;
		//const unsigned char * data2 = (const unsigned char*)data;

		switch(len & 7)
		{
		case 7: h ^= ((long)(key[pos + 6])) << 48;
		case 6: h ^= ((long)(key[pos + 5])) << 40;
		case 5: h ^= ((long)(key[pos + 4])) << 32;
		case 4: h ^= ((long)(key[pos + 3])) << 24;
		case 3: h ^= ((long)(key[pos + 2])) << 16;
		case 2: h ^= ((long)(key[pos + 1])) << 8;
		case 1: h ^= ((long)(key[pos + 0]));
		        h *= m;
		};
	 
		h ^= h >> r;
		h *= m;
		h ^= h >> r;

		return h;

		

	}

}
