package org.ajmm.obsearch.exception;

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
	  OBException, the mother of all Exceptions.

    @author      Arnoldo Jose Muller Molina
    @version     %I%, %G%
    @since       1.0
*/

public class OBException extends Exception {
	protected Exception ex;
	// sing it!
	protected String str;

	public OBException(String msg){
		this.str = msg;
	}
	public OBException (){
		ex = null;
	}
	public OBException(Exception e){
		this.ex = e;
	}

	public String toString(){
		if(ex != null){
			return ex.toString();
		}
		else if(str != null){
			return str;
		}else{
			return "N/A, Sing this Corrosion!";
		}
	}
}
