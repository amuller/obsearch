package net.obsearch.index.dprime;

import java.io.Serializable;

public final class BinaryTrie implements Serializable{

	private BinaryTrie zero;
	private BinaryTrie one;
	public static int objectCount = 0;
	
	public  BinaryTrie() {
		zero = null;
		one = null;		
	}
	
	public boolean isOne(){
		return one != null;
	}
	
	public BinaryTrie getZero(){
		return zero;		
	}
	
	public BinaryTrie getOne(){
		return one;
	}
	
	public boolean isZero(){
		return zero != null;
	}
	
	private String padZeros(int fullWidth, String s){
		StringBuilder b = new StringBuilder();
		int l = s.length();
		
		while(l < fullWidth){
			b.append("0");
			l++;
		}
		b.append(s);
		String res = b.toString();
		assert res.length() == fullWidth;
		return res;
	}
	
	public void add(String binary){
		add(binary.length(), binary);
	}
	
	/**
	 * Add a binary string to the trie.
	 * @param binary Binary string
	 * @param fullWidth Pad zeroes to the left.
	 */
	public void add(int padding, String binary){
		String padded = padZeros(padding, binary);
		add(padded, padded.length()-1);
	}
	
	private void add(String binary, int index){
		if(-1 == index){
			return;
		}
		if(binary.charAt(index) == '0'){
			if(zero == null){
				zero = new BinaryTrie();
				objectCount++;
			}
			zero.add(binary, index - 1);
		}else{
			if(one == null){
				one = new BinaryTrie();
				objectCount++;
			}
			one.add(binary, index - 1);
		}
	}

	/**
	 * Returns true if the trie contains the given prefix.
	 * @param binary 
	 * @return
	 */
	public boolean containsPrefix(String binary){
		return containsPrefix(binary, 0);
	}
	
	public boolean contains(long binary){
		return containsPrefix(Long.toBinaryString(binary));
	}
	
	
	
	private boolean containsPrefix(String binary, int index){
		if(binary.length() == index){
			return true;
		}
		if(binary.charAt(index) == '0'){
			if(zero != null){
				return zero.containsPrefix(binary, index+1);
			}else{
				return false;
			}
		}else{
			if(one != null){
				return one.containsPrefix(binary, index+1);
			}else{
				return false;
			}
		}
	}
	
	public boolean containsInv(long binary){
		return containsPrefixInv(Long.toBinaryString(binary));
	}
	
	public boolean containsPrefixInv(String binary){
		return containsPrefixInv(binary, binary.length() -1);
	}
	
	private boolean containsPrefixInv(String binary, int index){
		if(-1== index){
			return true;
		}
		if(binary.charAt(index) == '0'){
			if(zero != null){
				return zero.containsPrefixInv(binary, index-1);
			}else{
				return false;
			}
		}else{
			if(one != null){
				return one.containsPrefixInv(binary, index-1);
			}else{
				return false;
			}
		}
	}
	
}
