package net.obsearch.example.protein;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
import net.obsearch.utils.bytes.ByteConversion;

public class MyDistance implements OBFloat {
	
	private String id;
	private Entry[] entries;
	
	
	public MyDistance(String id, String sequence ) throws OBException{
		this.id = id;	
		HashMap<Character, List<Integer>> data = new HashMap<Character, List<Integer>>();
		int i = 0;
		while(i < sequence.length()){
			char c = sequence.charAt(i);
			List<Integer> l = data.get(c);
			if(l == null){
				l = new LinkedList<Integer>();
				data.put(c, l);
			}
			l.add(i);
			i++;
		}
		entries = new Entry[data.size()];
		int en = 0;
		for(Map.Entry<Character, List<Integer>> e : data.entrySet()){
			List<Integer> l = e.getValue();
			int [] indexes = new int[l.size()];
			int cx = 0;
			for(Integer s : l){
				indexes[cx] = s;
				cx++;
			}
			Arrays.sort(indexes);
			entries[en] = new Entry(e.getKey(), indexes);
			en++;
		}
		
		Arrays.sort(entries);

	}
	
	public MyDistance(){
		id = null;
	}
	
	/**
	 * Find the given entry or return null if it is not found.
	 * @param e
	 * @return
	 */
	private Entry findEntry(Entry e){
		int index = Arrays.binarySearch(entries, e);
		index = Math.abs(index);
		if(index < entries.length && (entries[index].getCode() == e.getCode())){
			return entries[index];
		}
		return null;
	}

	@Override
	public float distance(OBFloat object) throws OBException {
		MyDistance other = (MyDistance)object;
		return (distanceAux(object) + other.distanceAux(this)) / 2;
	}
	
	private float distanceAux(OBFloat object){
		float res = 0;
		float nulls = 0;
		float resCount = 0;
		MyDistance m = (MyDistance)object;
		for(Entry e : entries){
			Entry other = m.findEntry(e);
			if(other != null){
				for(int s : e.getIndexes()){
					res += other.findClosest(s);
					resCount++;
				}				
			}else{
				nulls++;
			}
		}
		
		return res + nulls; //(nulls * (resCount * res));
	}

	@Override
	public void load(byte[] input) throws OBException, IOException {
		ByteBuffer buf = ByteConversion.createByteBuffer(input);
		int idSize = buf.getInt();
		byte[] string = new byte[idSize];
		buf.get(string);
		id = new String(string);
		entries = new Entry[buf.getInt()];
		int i = 0;
		while(i < entries.length){
			byte[] b = new byte[buf.getInt()];
			buf.get(b);
			Entry e = new Entry();
			e.load(b);
			entries[i] = e; 
			i++;
		}
	}

	@Override
	public byte[] store() throws OBException, IOException {
		int sz = 0;
		sz += ByteConstants.Int.getSize();
		byte[] idBytes = id.getBytes();
		sz += idBytes.length;
		List<byte[]> serialized =  new LinkedList<byte[]>();
		sz += ByteConstants.Int.getSize();
		for(Entry e : entries){
			byte[] buf = e.store();
			serialized.add(buf);
			sz += buf.length;
			sz += ByteConstants.Int.getSize();
		}
		ByteBuffer buf = ByteConversion.createByteBuffer(sz);
		buf.putInt(idBytes.length);
		buf.put(idBytes);
		buf.putInt(serialized.size());
		for(byte[] b : serialized){
			buf.putInt(b.length);
			buf.put(b);
		}
		return buf.array();
	}
	
	private class Entry implements Comparable<Entry>{
		
		private byte code;
		private int[] indexes;
		
		public Entry(){
			
		}
		public Entry(char c, int[] indexes) throws OBException{
			OBAsserts.chkAssert(c <= Byte.MAX_VALUE, "Exceeded char val");
			this.code = (byte)c;
			this.indexes = indexes;
		}
		
		public byte getCode(){
			return code;
		}

		@Override
		public int compareTo(Entry o) {
			if(code < o.code){
				return -1;
			}else if(code > o.code){
				return 1;
			}else{
				return 0;
			}
		}
		
		public int[] getIndexes(){
			return indexes;
		}
		
		/**
		 * find the closest distance to a position
		 * @param index
		 * @return
		 */
		public int findClosest(int position){
			int ind = Arrays.binarySearch(indexes, position);
			ind = Math.abs(ind);
			ind = Math.min(ind, indexes.length - 1); // put the index in a safe position
			int d = get(ind - 1);
			int res = Integer.MAX_VALUE;
			res = Math.min(res, Math.abs(d - position));
			d = get(ind);
			res = Math.min(res, Math.abs(d - position));
			d = get(ind + 1);
			res = Math.min(res, Math.abs(d - position));
			return res;			
		}
		
		private int get(int index ){
			if(index >= 0 && index < indexes.length){
				return indexes[index];
			}else{
				return Integer.MAX_VALUE;
			}
		}
		
		public byte[] store(){
			ByteBuffer buf = ByteConversion.createByteBuffer(ByteConstants.Byte.getSize() + (ByteConstants.Int.getSize() * (indexes.length + 1)) );
			buf.put(code);
			buf.putInt(indexes.length);
			for(int x : indexes){
				buf.putInt(x);
			}
			return buf.array();
		}
		
		public void load(byte [] buf){
			ByteBuffer b = ByteConversion.createByteBuffer(buf);
			code = b.get();
			indexes = new int[b.getInt()];
			int i = 0;
			while(i < indexes.length){
				indexes[i] = b.getInt();
				i++;
			}
		}
	}
}
