package org.ajmm.obsearch;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C) 2007 Kyushu Institute of Technology

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
 * This class is used to return the result of operations performed by OBSearch.
 * The result also includes an id field that, depending on the operation returns
 * the last inserted or deleted object.
 * @author Arnoldo Jose Muller Molina
 * @since 0
 */

public enum Result {
    
    
    OK,
    EXISTS,
    NOT_EXISTS,
    ERROR;
    
    /**
     * Object id for relevant 
     */
    private int id;
  
   Result(){
       this.id = -1;
   }
    
   /**
    * Sets the id returned in the enumeration.
    * @param id The new id.
    */
   public void setId(int id){
       this.id = id;
   }
   /**
    * Returns the id of the affected item of the operation
    * -1 if the id does not apply.
    * @return The id.
    */
   public int getId(){
       return id;
   }
}
