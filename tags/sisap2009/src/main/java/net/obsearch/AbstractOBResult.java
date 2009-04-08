package net.obsearch;

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
 * Holds a match result. It includes the object matched, its internal id and
 * subclasses of this class will hold the distance value. WARNING: The internal
 * id held in this class does not make sense in a P2P environment.
 * @author Arnoldo Jose Muller Molina
 * @param <O>
 *            The object that was added into the result.
 * @since 0.7
 */

public abstract class AbstractOBResult < O  > implements Comparable {

    /**
     * The object to be stored.
     */
    protected O object;

    /**
     * Its internal id.
     */
    protected long id;
    
   

    /**
     * Default constructor.
     */
    public AbstractOBResult() {

    }

    /**
     * Creates a new result with the given object and id.
     * @param object
     *            The object to be stored in the result.
     * @param id
     *            The internal id of the object.
     */
    public AbstractOBResult(final O object, final long id) {
        super();
        this.object = object;
        this.id = id;

    }

    /**
     * @return The object of the result.
     */
    public final O getObject() {
        return this.object;
    }

    /**
     * Sets the object of the result.
     * @param obj
     */
    public final void setObject(final O obj) {
        this.object = obj;
    }

    /**
     * @return The internal id of the result. (Not useful in P2P mode)
     */
    public final long getId() {
        return this.id;
    }

    /**
     * Sets the internal id of the result.
     * @param id The identification number of this result's object.
     */
    public final void setId(final long id) {
        this.id = id;
    }

    
}
