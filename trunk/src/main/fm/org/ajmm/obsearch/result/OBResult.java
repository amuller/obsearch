<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="OBResult"+Type+".java" />
package org.ajmm.obsearch.result;
import org.ajmm.obsearch.AbstractOBResult;
import org.ajmm.obsearch.ob.OB${Type};

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
	  Class: OBResult
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       1.0
*/


public class OBResult${Type}<O extends OB${Type}> extends AbstractOBResult<O> {
		
		
		
		protected ${type} distance;

		public OBResult${Type}(){
		}

		public OBResult${Type}(O object, int id, ${type} distance){

				super(object,id);
				this.distance = distance;
				
		}
		
		public ${type} getDistance(){
				return distance;
		}
		
		public void setDistance(${type} x){
				this.distance = x;
		}

		public int compareTo(Object o) {
				assert o instanceof OBResult${Type};
				int res = 0;
				OBResult${Type}<O> comp = (OBResult${Type}<O>) o;
        if (distance < comp.distance) {
            res = 1;
        } else if (distance > comp.distance) {
            res = -1;
        }
        return res;
		}

		public int hashCode(){
				return object.hashCode() + (int) distance;
		}

    public boolean equals(Object obj){        
				if(obj == null){
						return false;
				}
				if(! (obj instanceof OBResult${Type})){
						return false;
				}
				OBResult${Type}<O> comp = (OBResult${Type}<O>) obj;
				// a result object is the same if the distance is the same
				// we do not care about the id.
				return distance == comp.distance;
		}

		public String toString(){				
				return "<" + id + " " + distance + ">";
		}
}

</#list>
