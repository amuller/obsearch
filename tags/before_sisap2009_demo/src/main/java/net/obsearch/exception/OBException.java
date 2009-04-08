package net.obsearch.exception;

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
 * OBException, the mother of all Exceptions in OBSearch.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public class OBException
        extends Exception {
   

    /**
     * Constructor.
     * @param msg
     *            A message for the user
     */
    public OBException(final String msg) {
        super(msg);
    }

    /**
     * Constructor.
     * @param msg
     *            A message for the user
     * @param ex
     *            An exception for the user
     */
    public OBException(final String msg, final Exception ex) {
        super(msg, ex);
    }

    /**
     * Default constructor.
     */
    public OBException() {
        super();
    }

    /**
     * Constructor.
     * @param e
     *            An exception that will be wrapped.
     */
    public OBException(Exception e) {
        super(e);
    }

    /**
     * @return A description for the user of the exception.
     */
    /*public String toString() {
        if (ex != null && str != null) {
            return str + " " + ex.getMessage();
        } else if (ex != null) {
            return ex.toString();
        } else if (str != null) {
            return str;
        } else {
            return "N/A, Sing this Corrosion!";
        }
    }*/
}
