package org.ajmm.obsearch;

import org.ajmm.obsearch.exception.OBException;

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
 * An index wrapper that . Whenever you perform a search the
 * control will return inmediately to the caller and the matching will be
 * performed in background. A synchronization method called waitQueries() is
 * provided. When the caller calls it, the control will return until all the
 * submitted queries have completed
 * @param <O> The object that is stored in the index.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public interface ParallelIndex < O extends OB > {

    /**
     * Waits for all the current searches to complete.
     * @throws OBException If an error occurrs while waiting.
     */
    void waitQueries() throws OBException;

    /**
     * @return The underlying index.
     */
    Index < O > getIndex();

}
