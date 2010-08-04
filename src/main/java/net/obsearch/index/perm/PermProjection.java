package net.obsearch.index.perm;

import java.nio.ByteBuffer;

import net.obsearch.constants.ByteConstants;
import net.obsearch.index.sorter.Projection;
import net.obsearch.utils.bytes.ByteConversion;

public class PermProjection implements Projection<PermProjection, CompactPerm> {
	
	private  CompactPerm addr;
	private int distance;
	
	public PermProjection(CompactPerm addr, int distance){
		this.addr = addr;
		this.distance = distance;
	}
	@Override
	public byte[] getAddress() {
		return shortToBytes(addr.perm);
	}
	
	public static byte[] shortToBytes(short[] addr){
		ByteBuffer res = ByteConversion.createByteBuffer(addr.length * ByteConstants.Short.getSize());
		for(short s : addr){
			res.putShort(s);
		}
		return res.array();
	}

	@Override
	public CompactPerm getCompactRepresentation() {
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
	
	public int getDistance(){
		return distance;
	}

	@Override
	public PermProjection distance(CompactPerm b) {
		
		return new PermProjection(b, sfrDistance(b));
	}
	
	public int sfrDistance(CompactPerm b){
		int i = 0;
		int res = 0; 
		while(i < addr.perm.length){			
			int cx = 0;
			while(cx < b.perm.length){
				if(addr.perm[i] == b.perm[cx]){
					break;
				}
				cx++;
			}
			res += Math.abs(i - cx);
			i++;
		}
		return res;
	}
	
	public String toString(){
		return distance + "";
	}
	
	public void set(int i, short pivot){
		addr.set(i, pivot);
	}

}
