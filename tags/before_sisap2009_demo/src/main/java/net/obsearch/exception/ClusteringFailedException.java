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
 * Exception thrown when a clustering error is signaled.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class ClusteringFailedException
        extends OBException {

    /**
     * Constructor with the msg to be shown to the user.
     * @param msg
     *            A message that describes the clustering problem to a user.f
     */
    public ClusteringFailedException(final String msg) {
        super(msg);
    }

}
