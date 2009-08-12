package net.obsearch.index.perm;

import java.nio.ByteBuffer;

import net.obsearch.constants.ByteConstants;
import net.obsearch.index.sorter.Projection;
import net.obsearch.utils.bytes.ByteConversion;

public class PermProjection implements Projection<PermProjection, short[]> {
	
	private  short[] addr;
	private int distance;
	
	public PermProjection(short[] addr, int distance){
		this.addr = addr;
		this.distance = distance;
	}
	@Override
	public byte[] getAddress() {
		return shortToBytes(addr);
	}
	
	public static byte[] shortToBytes(short[] addr){
		ByteBuffer res = ByteConversion.createByteBuffer(addr.length * ByteConstants.Short.getSize());
		for(short s : addr){
			res.putShort(s);
		}
		return res.array();
	}

	@Override
	public short[] getCompactRepresentation() {
		return addr;
	}

	

	@Override
	public int compareTo(PermProjection o) {
		if(distance < o.distance){
			return -1;
		}else if(distance > o.distance){
			return 1;
		}else{
			return 0;
		}
	}

	@Override
	public PermProjection distance(short[] b) {
		int i = 0;
		int res = 0; 
		while(i < addr.length){
			res += Math.abs(addr[i] - b[i]);
			i++;
		}
		return new PermProjection(b, res);
	}
	
	public String toString(){
		return distance + "";
	}

}
