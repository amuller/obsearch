package net.obsearch.example.protein;

import java.io.IOException;
import java.nio.ByteBuffer;

import net.obsearch.constants.ByteConstants;
import net.obsearch.example.lev.OBString;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
import net.obsearch.ob.OBShort;
import net.obsearch.utils.bytes.ByteConversion;

public class Protein extends OBString  implements OBFloat{
	
	private String id;
	
	public Protein(String id, String protein) throws OBException{
		super(protein);
		this.id = id;
	}
	
	public Protein(){
		
	}
	@Override
	public float distance(OBFloat object) throws OBException {
		return (float) super.distance((OBShort) object);
	}
	@Override
	public void load(byte[] input) throws OBException, IOException {
		ByteBuffer buf = ByteConversion.createByteBuffer(input);
		byte[] bufId = new byte[buf.getInt()];
		buf.get(bufId);
		id = new String(bufId);
		byte[] bufStr = new byte[buf.getInt()];
		buf.get(bufStr);
		super.str = new String(bufStr);
	}
	@Override
	public byte[] store() throws OBException, IOException {
		byte[] idB = id.getBytes();
		byte[] prot = super.str.getBytes();
		ByteBuffer buf = ByteConversion.createByteBuffer((2 * ByteConstants.Int.getSize()) + idB.length + prot.length);
		buf.putInt(idB.length);
		buf.put(idB);
		buf.putInt(prot.length);
		buf.put(prot);
		return buf.array();
	}
	
	public String getId(){
		return id;
	}
	
	

}
