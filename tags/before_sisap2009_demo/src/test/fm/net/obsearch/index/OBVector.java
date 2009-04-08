<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="OBVector${Type}.java" />
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
import java.io.IOException;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OB${Type};
import java.nio.ByteBuffer;
import net.obsearch.utils.bytes.ByteBufferFactoryConversion;
/**
 * L1 distance implementation for ${type}s.
 * @author Arnoldo Jose Muller Molina
 *
 */
<@gen_warning filename="OBVector.java "/>
public class OBVector${Type} implements OB${Type} {
	
	private ${type}[] data;
	
	/**
     * Default constructor must be provided by every object that implements the
     * interface OB.
     */
    public OBVector${Type}() {
    	data = null;
    }
	
	public OBVector${Type}(${type}[] data){
		this.data = data;
	}

	@Override
	public ${type} distance(OB${Type} object) throws OBException {
		// TODO Auto-generated method stub
		OBVector${Type} o = (OBVector${Type}) object;
		assert data.length == o.data.length;
		${type} res = 0;
		int i = 0;
		while(i < data.length){
				assert (data[i] - o.data[i]) >= <@min_value/> && (data[i] - o.data[i]) <= ${ClassType}.MAX_VALUE : "a: " + data[i] + " b: "  + o.data[i] ;		 

				
			res += Math.abs(data[i] - o.data[i]);
			i++;
		}
		return res;
	}

	<@gen_warning filename="OBVector.java "/>
	@Override
	public void load(byte[] input) throws OBException, IOException {
		ByteBuffer in = ByteBufferFactoryConversion.createByteBuffer(input);
		int size = in.getInt();
		
		data = new ${type}[size];
		int i = 0;
		while(i < size){
			data[i] = in.get${BBType}();
			i++;
		}
	}
	
	<@gen_warning filename="OBVector.java "/>
	@Override
	public byte[] store() throws OBException, IOException {
			ByteBuffer out = ByteBufferFactoryConversion.createByteBuffer(ByteConstants.${Type}.getSize() * data.length + ByteConstants.Int.getSize() );
		
		out.putInt(data.length);
		for(${type} v : data){
			out.put${BBType}(v);
		}
			return out.array();
	}

}
</#list>