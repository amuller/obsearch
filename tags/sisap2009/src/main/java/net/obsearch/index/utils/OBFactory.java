package net.obsearch.index.utils;

import net.obsearch.OB;
import net.obsearch.exception.OBException;


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
