/**
 * 
 */
package org.ajmm.obsearch.testutils;

import org.ajmm.obsearch.exception.OBException;

/**
 * @author amuller
 *
 */
public class SliceParseException extends OBException {
	
	private String slice;
	
	SliceParseException(String x){
		slice = x;
	}
	
	public String toString(){
		return slice;
	}

}
