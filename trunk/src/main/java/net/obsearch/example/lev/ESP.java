package net.obsearch.example.lev;

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import net.obsearch.OperationStatus;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.ob.OBLong;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.TupleLong;

public class ESP implements OBLong {

	private Entry[] data;

	public ESP(String s) {

		List<Long> string = new ArrayList<Long>(s.length());
		int i = 0;
		while (i < s.length()) {
			string.add(Long.valueOf(s.charAt(i)));
			i++;
		}

		// apply the Edit Sensitive Parsing algorithm to the data.
		// we go parsing each metablock and we fragment each
		// metablock.
		List<String> partitions = new ArrayList<String>(s.length() / 2);

	}

	/**
	 * Finds metablocks, finds blocks, and encodes the string into a new string.
	 * 
	 * @param string
	 * @return
	 * @throws OBException 
	 */
	private List<Long> reduce(List<Long> string) throws OBException {
		List<Long> res = new ArrayList<Long>(string.size() / 2);
		int alpha = countAlphabet(string);
		List<Long> metablock = new ArrayList<Long>(string.size() / 2);
		for (Long l : string) {
			// our char is equal to the previous
			if (l.equals(last(metablock))) {
				// two things can happen
				if (isRepeated(metablock, l)) {
					// all the previous symbols are equal, don't do anything
					// continue extracting the data.
				} else {
					// otherwise we have to output the current metablock.
					res.addAll(outputDifferentLong(metablock));
					// remove items from the list.
					metablock.clear();
				}
			} else { // characters differ.
				if (last(metablock) != null) {
					// if we have a sequence of repetitions
					if (isRepeatedMetaBlock(metablock)) {
						res.addAll(outputSame(metablock));
						metablock.clear();
					}
				}
			}
			metablock.add(l);
		}
		return res;
	}
	/**
	 * Returns true if the metablock is a sequence of the same symbol
	 * @param metablock
	 * @return
	 */
	private boolean isRepeatedMetaBlock(List<Long> metablock){
		return metablock.size() > 1
		&& isRepeated(metablock, last(metablock));
	}
	
	private int logAsterisk(double alpha){
		double res = alpha;
		int i = 0;
		while(res > 1){
			res = log2(res);
			i++;
		}
		return i; 
	}
	
	private double log2(double num){
		return Math.log10(num) / Math.log10(num);
	}
	
	private List<Long> outputDifferentLong(List<Long> metablock) throws OBException {
		List<Long> res = new ArrayList(metablock.size());
		res.addAll(metablock);
		int alpha = countAlphabet(metablock);
		int logAsterisk = logAsterisk(alpha);
		boolean changed = true;
		while(changed){
			res =  outputDifferentAux(res);
			int newAlpha = countAlphabet(res);
			changed = newAlpha != alpha;
			alpha = newAlpha;
		}
		// now the alphabet size should be 6 or less
		OBAsserts.chkAssert(alpha <= 6 , "Theorem says we are at 6 at this point");
		// now we perform 3 iterations.
		res = pass(3L, res);
		res = pass(4L, res);
		res = pass(5L, res);
		// now we have filtered the guys I will just make sure that 
		// everything is a 0,1,2
		validate(res);
		// we can now find special locations called landmarks!
		int i = 0;
		BitSet landmarks = new BitSet(res.size());
		// find maximum landmarks
		while(i < res.size()){
			if(isMaximumLandmark(i, res)){
				landmarks.set(i);
			}
			i++;
		}
		i = 0;
		while(i < res.size()){
			if(isMinimumLandmark(i, res, landmarks)){
				landmarks.set(i);
			}
			i++;
		}
		
		return res;
	}
	
	private List<Long> outputDifferentShort(List<Long> metablock) throws OBException{
		int alpha = countAlphabet(metablock);
		int logAsterisk = logAsterisk(alpha);
		OBAsserts.chkAssert(logAsterisk < metablock.size(), "wrong size");
		assert false;
		return null;
	}		
	
	private boolean isMaximumLandmark(int index, List<Long> list){
		Long left = get(list, index -1);
		Long center = get(list, index);
		Long right = get(list, index + 1);
		boolean res = false;
		if(left != null){
			res = left < center;
		}
		if(res && right != null){
			res = center > right;
		}
		return res;
	}
	
	private boolean isMinimumLandmark(int index, List<Long> list, BitSet landmarks){
		Long left = get(list, index -1);
		Long center = get(list, index);
		Long right = get(list, index + 1);
		boolean res = false;
		if(left != null){
			res = left > center && ! landmarks.get(index - 1);
		}
		if(res && right != null){
			res = center < right && ! landmarks.get(index + 1);
		}
		return res;
	}
	
	private void validate(List<Long> list) throws OBException{
		HashSet<Long> ids = createBasic();
		for(Long l : list){
			OBAsserts.chkAssert(ids.contains(l), "Wrong item: " + l);
		}
	}
	
	
	private List<Long> pass(Long toDelete, List<Long> list){
		List<Long> res = new ArrayList<Long>(list.size());
		int i = 0;
		for(Long l : list){
			if(l.equals(toDelete)){
				res.add(findNeighbour(list, i));
			}else{
				res.add(l);
			}
			i++;
		}
		return res;
	}
	
	private HashSet<Long> createBasic(){
		HashSet<Long> ids = new HashSet<Long>();
		ids.add(Long.valueOf(0L));
		ids.add(Long.valueOf(1L));
		ids.add(Long.valueOf(2L));
		return ids;
	}
	
	private Long findNeighbour(List<Long> list, int index){
		Long left = get(list, index -1);
		Long center = get(list, index);
		Long right = get(list, index + 1);
		HashSet<Long> ids = createBasic();
		if(left != null){
			ids.remove(left);
		}
		if(right != null){
			ids.remove(right);
		}
		List<Long> remaining = new ArrayList<Long>(ids.size());
		remaining.addAll(ids);
		Collections.sort(remaining);
		return remaining.get(0);
	}
	
	private Long get(List<Long> list, int i){
		if(i >= 0 && i < list.size()){
			return list.get(i);
		}else{
			return null;
		}
	}

	private List<Long> outputDifferentAux(List<Long> metablock) throws OBException {
		int i = 1;
		List<Long> res = new ArrayList<Long>(metablock.size());
		while(i < metablock.size()){
			Long am1 = metablock.get(i - 1);
			Long a1 = metablock.get(i);
			OBAsserts.chkAssert(! am1.equals(a1), "Cannot have two equal elements");
			res.add(alphabetReduction(am1,a1));
			i++;
		}
		return res;
	}
	
	private Long alphabetReduction(Long a, Long b){
		long xored = a ^ b;
		long mask = 1;
		int i = 0;
		while(i < ByteConstants.Long.getBits()){
			if((xored & mask) != 0){
				break;
			}
			mask = mask << 1;
			i++;
		}
		// i holds the bits in which a and b differ.
		long difference  = b | mask;
		if(difference != 0){
			difference = 1;
		}
		return (2 * i + difference);
	}

	private List<Long> outputSame(List<Long> metablock) {
		assert false;
		return null;
	}

	private boolean isRepeated(List<Long> metablock, Long symbol) {

		for (Long l : metablock) {
			if (!l.equals(symbol)) {
				return false;
			}
		}
		return true;
	}

	private Long last(List<Long> metablock) {
		if (metablock.size() == 0) {
			return null;
		} else {
			return metablock.get(metablock.size() - 1);
		}
	}

	private int countAlphabet(List<Long> string) {
		HashSet<Long> count = new HashSet<Long>(string.size());
		for (Long l : string) {
			count.add(l);
		}
		return count.size();
	}

	@Override
	public long distance(OBLong object) throws OBException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void load(byte[] input) throws OBException, IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public byte[] store() throws OBException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	private class Entry implements Comparable<Entry> {
		private long id;
		private long count;

		public Entry(long count, long id) {
			super();
			this.count = count;
			this.id = id;
		}

		@Override
		public int compareTo(Entry o) {
			if (id < o.id) {
				return -1;
			} else if (id > o.id) {
				return 1;
			} else {
				return 0;
			}
		}

		public long getId() {
			return id;
		}

		public long getCount() {
			return count;
		}

	}

}
