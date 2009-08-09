<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#include "/@inc/index.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="Commons${Type}.java" />
package net.obsearch.index;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.ob.OB${Type};
import java.util.Arrays;
import net.obsearch.exception.OBException;
/*
		OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
    Copyright (C) 2009 Arnoldo Jose Muller Molina

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
	*  Commons 
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */


 public class Commons${Type}{

		/**
	 * This method returns a list of all the distances of the query against  the DB.
	 * This helps to calculate EP values in a cheaper way. results that are equal to the original object are added
	 * as ${ClassType}.MAX_VALUE
	 * @param query
	 * @param filterSame if True we do not return objects o such that query.equals(o)
	 * @return
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
		public static ${type}[] fullMatchLite(OB${Type} query, boolean filterSame, Index${Type} index) throws OBException, IllegalAccessException, InstantiationException{
		long max = index.databaseSize();
		OBAsserts.chkAssert(max < Integer.MAX_VALUE, "Cannot exceed 2^32");
		${type}[] result = new ${type}[(int)max];
		int id = 0;
		OB${Type} o = (OB${Type})index.getType().newInstance();
		while(id < max){
			index.loadObject(id, o);
			${type} distance = o.distance(query);
			if(distance == 0 && o.equals(query) && filterSame){
				result[id] = ${ClassType}.MAX_VALUE;				
			}else{
				result[id] = distance;
			}
			id++;
		}
		
		Arrays.sort(result);
		return result;		
	}

 }



</#list>