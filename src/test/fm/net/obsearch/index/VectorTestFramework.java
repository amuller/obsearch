<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="VectorTestFramework${Type}.java" />
package net.obsearch.index;
/*
		OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
    Copyright (C) 2008 Arnoldo Jose Muller Molina

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
	*  VectorTestFramework creates objects of type OBVector${Type} 
	*  and feeds parent TestFramework${Type} with them.
  *  @author      Arnoldo Jose Muller Molina    
  */
import java.util.Random;

public class VectorTestFramework${Type} extends TestFramework${Type}<OBVector${Type}> {

	private int vectorDimensionality;
	private Random r;
	public VectorTestFramework${Type}(int vectorDimensionality, int dbSize, int querySize,
			Index${Type}<OBVector${Type}> index) {
		super(OBVector${Type}.class, dbSize, querySize, index);
		this.vectorDimensionality = vectorDimensionality;
		r = new Random();
	}

	@Override
	protected OBVector${Type} next() {
		${type}[] vector = new ${type}[vectorDimensionality];
		int i = 0;
		while(i < vector.length){
			vector[i] = <@random r="r" dimensionality="vectorDimensionality"/>
					//(${type})r.nextInt(${ClassType}.MAX_VALUE/vectorDimensionality);
			i++;
		}
		return new OBVector${Type}(vector);
	}
	
	protected void search() throws Exception{
		super.search();
		search(index, (${type})(${ClassType}.MAX_VALUE/vectorDimensionality * 6) , (byte) 3);       
	}
	
	
}
</#list>