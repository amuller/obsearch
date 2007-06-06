package org.ajmm.obsearch.index.utils;

import com.sleepycat.bind.tuple.TupleInput;

public class MyTupleInput extends TupleInput {
	public static final byte [] t = {1};
	/**
	 * Creates a new tuple input
	 * @param buffer
	 */
	 public MyTupleInput(byte[] buffer) {
		 super(buffer);		 
	 }
	 
	 public MyTupleInput() {
		 this(t);
	 }
	 
	 /**
	  * Sets the current buffer
	  * Avoid an object cration with this	
	  * @param buffer
	  */
	 public void setBuffer(byte[] buffer){
		 buf = buffer;
	     len = buffer.length;
	     off = 0;
	     mark = 0;
	 }
}
