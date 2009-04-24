package net.obsearch.storage.cuckoo;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class Murmur64 implements HashFunction {
	public final static ByteOrder order = ByteOrder.nativeOrder();
	@Override
	public long compute(byte[] data) {
		return computeAux(data, data.length, 1);
	}
	
	
	
	private long computeAux(byte[] key, int len, int seed){

		final long m = 0xc6a4a7935bd1e995L;
		final int r = 47;

		long h = seed ^ (len * m);

		//const uint64_t * data = (const uint64_t *)key;
		//const uint64_t * end = data + (len/8);
		ByteBuffer buf = ByteBuffer.wrap(key);
		buf.order(order);
		int end = len;
		while(end >= 8)
		{
			long k = buf.getLong();///getLong(key, i);

			k *= m; 
			k ^= k >> r; 
			k *= m; 
			
			h ^= k;
			h *= m;
			
			end -= 8;
		}
		int pos = buf.position();
		
		//const unsigned char * data2 = (const unsigned char*)data;
		
		switch(end)
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
