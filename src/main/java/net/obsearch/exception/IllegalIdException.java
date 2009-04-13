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
 * Occurs when an internal id requested but the id is not available.
 * This Exception is kept as a debugging aid. Normally you should
 * not receive it. If you do, please report the problem to:
 * http://code.google.com/p/obsearch/issues/list
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class IllegalIdException
        extends OBException {
    private long id;
    private byte[] idbytes;
    public String toString(){
        return "Illegal id was received :( : " + id;
    }
    public  IllegalIdException(long id){
        this.id = id;
    }
    
    public IllegalIdException(){
        this.id = -1;
    }
	public IllegalIdException(byte[] i) {
		this.idbytes = i;
	}
    
}
