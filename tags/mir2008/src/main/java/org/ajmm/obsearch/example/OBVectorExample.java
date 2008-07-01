package org.ajmm.obsearch.example;

import java.util.Arrays;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.ob.OBInt;
import org.ajmm.obsearch.ob.OBShort;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * Example Object. Store many shorts into a vector and use 1-norm distance on
 * them.
 * @author Arnoldo Jose Muller Molina.
 * @since 0.7
 */

public class OBVectorExample implements OBShort {

    /**
     * Total number of elements to store.
     */
    private static final int VECTOR_SIZE = 20000;

    /**
     * 1) Actual data.
     */
    private short[] data;

    /**
     * 2) Default constructor is required by OBSearch.
     */
    public OBVectorExample() {
        data = new short[VECTOR_SIZE];
    }

    /**
     * Additional constructors can be created to make your life easier.
     * (OBSearch does not use them)
     */
    public OBVectorExample(short[] data) {
        assert data.length == VECTOR_SIZE;
        this.data = data;
    }

    /**
     * 3) 1-norm distance function. A casting error can happen here, but we
     * don't check it for efficiency reasons.
     * @param object
     *            The object to compare.
     * @return The distance between this and object.
     * @throws OBException
     *             if something goes wrong. But nothing should be wrong in this
     *             function.
     */
    public final short distance(final OBShort object) throws OBException {
        OBVectorExample o = (OBVectorExample) object;
        short res = 0;
        int i = 0;
        while (i < VECTOR_SIZE) {
            res += Math.abs(data[i] - o.data[i]);
            i++;
        }
        return res;
    }

    /**
     * 4) Load method. Loads the data into this object. This is analogous to
     * object deserialization.
     * @param in
     *            Byte Stream with all the data that has to be loaded into this
     *            object.
     */
    public final void load(final TupleInput in) throws OBException {
        int i = 0;
        while (i < VECTOR_SIZE) {
            // read a short from the stream and
            // store it into our array.
            data[i] = in.readShort();
            i++;
        }
    }

    /**
     * 5) Store method. Write the contents of the object into out. Analogous to
     * Java's object serialization.
     * @param out
     *            Stream where we will store this object.
     */
    public final void store(TupleOutput out) {
        int i = 0;
        while (i < VECTOR_SIZE) {
            // write each short into
            // the stream
            out.writeShort(data[i]);
            i++;
        }
    }

    /**
     * 6) Equals method. Implementation of the equals method is required. A
     * casting error can happen here, but we don't check it for efficiency
     * reasons.
     * @param object
     *            The object to compare.
     * @return true if this and object are equal.
     */
    public final boolean equals(final Object object) {
        OBVectorExample o = (OBVectorExample) object;
        return Arrays.equals(data, o.data);
    }

    /**
     * 7) Hash code method. This method is required too.
     * @return The hash code of the data array.
     */
    public final int hashCode() {
        // TODO: this value should be cached.
        return Arrays.hashCode(data);
    }

}
