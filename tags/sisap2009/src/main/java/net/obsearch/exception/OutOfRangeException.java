package net.obsearch.exception;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; either version 2 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License along
 with this program; if not, write to the Free Software Foundation, Inc.,
 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.   
 */
/**
 * Class: OutOfRangeException. For each value, we convert it in [0,1] (so that
 * we can handle it inside the pyramid technique). If the distance returns a
 * value that is beyond the limits specified by the user.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public class OutOfRangeException
        extends OBException {
    private String message;

    /**
     * Default Constructor.
     */
    public OutOfRangeException() {
        super("N/A");
    }

    /**
     * Constructor.
     * @param min min value specified by the user
     * @param max max value specified by the user
     * @param obtained value obtained from the distance function
     */
    public OutOfRangeException(String min, String max, String obtained) {
        super(" min: " + min + " max: " + max + " obtained: " + obtained);
    }

}
