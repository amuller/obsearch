package net.obsearch.example;

import net.obsearch.exception.OBException;

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
 * Class used to generate an error when a Tree cannot be parsed properly.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class SliceParseException
        extends OBException {

    /**
     * Serial version of the class.
     */
    private static final long serialVersionUID = 3774865697155505953L;

    /**
     * Constructor.
     * @param x
     *            Original slice
     * @param e
     *            Exception that was thrown
     */
    public SliceParseException(final String x, final Exception e) {
        super(x, e);
    }

}
