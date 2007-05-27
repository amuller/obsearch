/**
 * 
 */
package org.ajmm.obsearch;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/**
 * @author amuller
 *
 */
public interface Storable {
    /**
     * Stores this object in a byte array.
     * 
     * @param out
     *            A TupleOutput where values can be stored
     * @since 0.0
     */
    void store(TupleOutput out);

    /**
     * Populates the object's internal properties from the given byte stream.
     * 
     * @param byteInput
     *            A TupleInput object from where primitive types can be loaded.
     * @since 0.0
     */
    void load(TupleInput byteInput);
}
