package net.obsearch.ob;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

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
 * MultiplicityAware defines an OB object that takes into account multiplicity.
 * OBSearch indexes may not support this interface in which case, the multiplicity value is
 *  simply ignored.
 * @author Arnoldo Jose Muller Molina
 */

public interface MultiplicityAware {

    /**
     * When an insert is performed on an OBSearch object that already exists,
     * OBSearch calls this method. Calling this method will increment the
     * multiplicity of the object by one. It is the responsibility of the OB
     * object to store and update the value and use it (OBSearch does not use
     * this value).
     */
    void incrementMultiplicity();

    /**
     * Returns the current multiplicity of the given object.
     * @return
     */
    long getMultiplicity();

    /**
     * When a delete is performed on an OBSearch object that already exists,
     * OBSearch calls this method. Calling this method will decrement the
     * multiplicity of the object by one. It is the responsibility of the OB
     * object to store and update the value and use it (OBSearch does not use
     * this value).
     */
    void decrementMultiplicity();

}
