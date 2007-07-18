package org.ajmm.obsearch.grid;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Date;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.AlreadyFrozenException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.exception.UndefinedPivotsException;
import org.apache.commons.net.TimeTCPClient;
import org.apache.commons.net.TimeUDPClient;
import org.apache.commons.net.ntp.NTPUDPClient;
import org.apache.commons.net.ntp.TimeInfo;

import com.sleepycat.je.DatabaseException;
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
	  Class: OBGrid
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       1.0
*/

public  class OBGrid<O extends OB> implements Index<O> {

	/**
	 * Maximum amount of time difference that will be tolerated
	 * Before considering a peer sick
	 */
	private static final long MAX_TIME_SEPARATION = 600000;
	
	/**
	 * We will sync with data that is as old as:
	 * System.currentTime() - (MAX_TIME_SEPARATION * UPDATE_THRESHOLD)
	 */
	private static final long UPDATE_THRESHOLD = 3;
	
	public void close() throws DatabaseException {
		// TODO Auto-generated method stub

	}
	
	public  int databaseSize(){
		return -1;
	}
	
	public int getBox(O obj){
		return -1;
	}
	
	public int totalBoxes(){
		return -1;
	}

	public int delete(O object) throws NotFrozenException, DatabaseException {
		// TODO Auto-generated method stub
		return 0;
	}

	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			DatabaseException, OutOfRangeException, OBException,
			UndefinedPivotsException {
		// TODO Auto-generated method stub

	}

	public O getObject(int i) throws DatabaseException, IllegalIdException,
			IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return null;
	}

	public int insert(OB object) throws IllegalIdException,
			DatabaseException, OBException, IllegalAccessException,
			InstantiationException {
		// TODO Auto-generated method stub
		return 0;
	}

	public boolean isFrozen() {
		// TODO Auto-generated method stub
		return false;
	}
	
	

}
