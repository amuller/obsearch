package org.ajmm.obsearch.index.utils;

import com.sleepycat.bind.tuple.TupleInput;

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
 * Class that allows the reuse of a TupleInput. Only used in certain places were
 * performance is important.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class MyTupleInput
        extends TupleInput {
    /**
     * Magic byte used for the default constructor.
     */
    private static final byte[] t = { 1 };

    /**
     * Creates a new tuple input.
     * @param buffer Buffer to use as input.
     */
    public MyTupleInput(final byte[] buffer) {
        super(buffer);
    }

    /**
     * Default constructor.
     */
    public MyTupleInput() {
        this(t);
    }

    /**
     * Sets the current buffer. This avoids re-creating TupleInput objects.
     * @param buffer The buffer that will be set.
     */
    public void setBuffer(final byte[] buffer) {
        buf = buffer;
        len = buffer.length;
        off = 0;
        mark = 0;
    }
}
