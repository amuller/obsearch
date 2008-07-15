package org.ajmm.obsearch.storage;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.ajmm.obsearch.exception.OBException;

/**
 * Iterator that must be closed after usage (remove locks, etc)
 * @author amuller
 *
 */
public interface CloseIterator<O> extends Iterator<O> {

    
   void closeCursor() throws OBException;
    
}
