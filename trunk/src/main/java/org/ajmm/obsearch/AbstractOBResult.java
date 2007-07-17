package org.ajmm.obsearch;

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
 * @param <
 *            D > Dimension type to use.
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public abstract class AbstractOBResult<O extends OB> implements Comparable{

    protected O object;
    protected int id;


    public AbstractOBResult(){

    }

    public AbstractOBResult(O object, int id) {
		super();
		this.object = object;
		this.id = id;

	}

	public O getObject() {
        return object;
    }

    public void setObject(O obj) {
        this.object = obj;
    }

    public int getId() {
        return id;
    }

    public void setId(int identification) {
        this.id = identification;
    }
}