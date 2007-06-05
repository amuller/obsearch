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

    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       1.0
*/


public class OBResult${Type}<O extends OB${Type}> extends AbstractOBResult<O> {
		
		
		protected ${type} distance;
		
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

    public boolean equals(Object obj){        
				OBResult${Type}<O> comp = (OBResult${Type}<O>) obj;
				return this.id == comp.id && distance == comp.distance;
		}
}

</#list>
