package org.ajmm.obsearch.index.utils;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;

/**
 * Instantiates objects from string lines.
 * @author amuller
 *
 */
public interface OBFactory <O extends OB>{

    /**
     * Creates a new object from the given String.
     * @param x The string to use to instantiate the obj.
     * @return The object.
     */
    O create(String x) throws OBException;
    
    /**
     * Returns true if we should process (add / search) the given
     * object.
     * @return true if we should process (add / search) the given
     * object.
     */
    boolean shouldProcess(O obj)throws OBException ;
}
