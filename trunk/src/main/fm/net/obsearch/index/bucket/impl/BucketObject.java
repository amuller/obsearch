<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="BucketObject${Type}.java" />
package net.obsearch.index.bucket.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OB${Type};

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
 * BucketObject${Type} extends {@link BucketObject} by adding an SMAP vector used to minimize
 * the number of distance computations required per object.
 * @author Arnoldo Jose Muller Molina
 */
<@gen_warning filename="BucketObject.java "/>
		public class BucketObject${Type}<O extends OB${Type}>
        extends BucketObject<O> implements Comparable<BucketObject${Type}>{
			
			
    /**
     * SMAP vector of the object.
     */
    private ${type}[] smapVector;
    
    public BucketObject${Type}(){
    	super(-1);
    	smapVector = null;
    }

    /**
     * Creates a new bucket ${type} with
     * @param bucket
     *                Bucket number.
     * @param level
     *                Level within the hash table.
     * @param smapVector
     *                The distances of the corresponding object and the pivots
     *                for the given level.
     * @param exclusionBucket
     *                If true, the corresponding object is in the exclusion
     *                zone.
     * @param id Optional id of the given object. Not always used.
     */
    public BucketObject${Type}(${type}[] smapVector,
             long id) {
        this(smapVector,id,null);
    }

		/**
     * Creates a new bucket ${type} with
     * @param bucket
     *                Bucket number.
     * @param level
     *                Level within the hash table.
     * @param smapVector
     *                The distances of the corresponding object and the pivots
     *                for the given level.
     * @param exclusionBucket
     *                If true, the corresponding object is in the exclusion
     *                zone.
     * @param id Optional id of the given object. Not always used.
     */
    public BucketObject${Type}(${type}[] smapVector,
															 long id, O object) {
        super(id, object);
				this.smapVector = smapVector;
    }

    
    /**
     * Execute l-inf between this object and b.
     * @param b
     * @return l-inf
     */
    public ${type} lInf(BucketObject${Type} b){
        return lInf(smapVector, b.getSmapVector());
    }


		public static ${type} lInf(${type}[] smapVector, ${type}[] other){
        int cx = 0;
        ${type} max = <@min_value/>;
        ${type} t;
        assert smapVector.length == other.length;
        while (cx < smapVector.length) {
            t = (${type}) Math.abs(smapVector[cx] - other[cx]);
            if (t > max) {
                max = t;               
            }
            cx++;
        }
        return max;
    }
    

		public static ${type}[] convertTuple(OB${Type} object, OB${Type}[] pivots) throws OBException{
				int i = 0;
				${type}[] smapVector = new ${type}[pivots.length];
				while (i < pivots.length) {
						${type} distance = pivots[i].distance(object);
						smapVector[i] = distance;
						i++;
				}
				return smapVector;
		}
   

    /**
     * @return the smapVector
     */
    public ${type}[] getSmapVector() {
        return smapVector;
    }
    
    /**
     * Writes itself into the given data buffer.
     * @param data The ByteBuffer that will be modified.
     */
    public void write(ByteBuffer out){
        for (${type} j : getSmapVector()) {
            out.put${BBType}(j);
        }
        out.putLong(getId());
    }
    
    /**
     * Reads the given # of pivots from the given bytebuffer.
     * @param out
     * @param pivots
     */
    public void read(ByteBuffer in, int pivots){
    	this.smapVector = new ${type}[pivots];
    	int i = 0;
    	while(i < pivots){
    		smapVector[i] = in.get${BBType}();
    		i++;
    	}        
        super.setId(in.getLong());
    }
    
    
    /**
     * @param smapVector
     *                the smapVector to set
     */
    public void setSmapVector(${type}[] smapVector) {
        this.smapVector = smapVector;
    }
    
    public int getPivotSize(){
        return smapVector.length;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(BucketObject${Type} o) {
        int i = 0;
        assert smapVector.length == o.smapVector.length;
        while(i < smapVector.length){
            int res = compareDim(smapVector[i], o.smapVector[i]);
            if(res != 0){
                return res;
            }
            i++;
        }
        // if we finish the loop both objects are equal.
        return 0;
    }
    
    /*public int compareTo(BucketObject${Type} o) {
        if(smapVector[0] < o.smapVector[0]){
            return -1;
        }else if (smapVector[0] > o.smapVector[0]){
            return 1;
        }else{
            return 0;
        }
    }*/
    
    /**
     * Compare one dimension.
     * @param a one vector value.
     * @param b another vector value.
     * @return -1 if a < b, 0 if a == b otherwise 1.
     */
    private final int compareDim(${type} a, ${type} b){
        if( a < b){
            return -1;
        }else if( a > b){
            return 1;
        }else{
            return 0;
        }
    }
    
    public String toString(){
        return Arrays.toString(smapVector);
    }
		<@gen_warning filename="BucketObject.java "/>
}
</#list>