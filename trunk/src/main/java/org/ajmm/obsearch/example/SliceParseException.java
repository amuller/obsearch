/**
 * 
 */
package org.ajmm.obsearch.example;

import org.ajmm.obsearch.exception.OBException;

/**
 * @author amuller
 */
public class SliceParseException
        extends OBException {

    private String slice;

    private Exception e;

    SliceParseException(String x, Exception e) {
        slice = x;
        this.e = e;
    }

    public String toString() {
        return slice + e;
    }

}
