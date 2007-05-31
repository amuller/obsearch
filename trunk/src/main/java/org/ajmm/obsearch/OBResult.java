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

public class OBResult<O extends OB, D extends Dim> extends Result<D> {
    protected O object;

    public O getObject() {
        return object;
    }

    public void setObject(O obj) {
        this.object = obj;
    }

    public int compareTo(Object o) {
        assert o instanceof OBResult; // faster!
        // if(! (o instanceof OBResult)){
        // throw new ClassCastException();
        // / }
        return compareToAux((OBResult) o);
    }
    
    /**
     * Sets the values of this object to the values of r.
     * @param r The values that will be copied into this object.
     */
    public void set(OBResult<O, D> r) throws InstantiationException, IllegalAccessException{
        super.set(r);
        this.object = r.object;
    }
    
    public boolean equals(Object obj){
        OBResult<O, D> o = (OBResult<O, D>) obj;
        return super.equals(o) && this.object.equals(o.object);        
    }
}
