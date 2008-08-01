package net.obsearch.index;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import net.obsearch.exception.OBException;
import net.obsearch.ob.OBShort;
/**
 * L1 distance implementation.
 * @author amuller
 *
 */
public class OBVectorShort implements OBShort {
	
	private short[] data;
	
	/**
     * Default constructor must be provided by every object that implements the
     * interface OB.
     */
    public OBVectorShort() {
    	data = null;
    }
	
	public OBVectorShort(short[] data){
		this.data = data;
	}

	@Override
	public short distance(OBShort object) throws OBException {
		// TODO Auto-generated method stub
		OBVectorShort o = (OBVectorShort) object;
		assert data.length == o.data.length;
		short res = 0;
		int i = 0;
		while(i < data.length){
			res += Math.abs(data[i] - o.data[i]);
			i++;
		}
		return res;
	}

	@Override
	public void load(DataInputStream in) throws OBException, IOException {
		int size = in.readInt();
		data = new short[size];
		int i = 0;
		while(i < size){
			data[i] = in.readShort();
			i++;
		}
	}

	@Override
	public void store(DataOutputStream out) throws OBException, IOException {
		out.writeInt(data.length);
		for(short v : data){
			out.writeShort(v);
		}
	}

}
