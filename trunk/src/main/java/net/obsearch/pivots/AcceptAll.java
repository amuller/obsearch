package net.obsearch.pivots;

import net.obsearch.OB;
import net.obsearch.exception.OBException;

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
* A class that accepts any object as pivot.
* @author Arnoldo Jose Muller Molina
* @since 0.8
*/
public class AcceptAll<O extends OB> implements Pivotable<O> {

    public boolean canBeUsedAsPivot(O object) throws OBException {
        return true;
    }

}
