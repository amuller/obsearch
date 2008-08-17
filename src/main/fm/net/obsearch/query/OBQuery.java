<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<#if type == "int">
<#assign Type2 = "Integer">
<#else>
<#assign Type2 = Type>
</#if>
<@pp.changeOutputFile name="OBQuery"+Type+".java" />
package net.obsearch.query;
import net.obsearch.ob.OB${Type};
import net.obsearch.result.OBResult${Type};
import net.obsearch.result.OBPriorityQueue${Type};

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
 * Object used to store a query request.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */


public final class OBQuery${Type}<O extends OB${Type}> extends OBResult${Type}<O> {


    /**
     * Holds the results for the query.
     */
    protected OBPriorityQueue${Type}<O> result;

    /**
     * Minimum part of the rectangle of the query.
     */ 
		protected ${type}[] min;

		
		/**
     * Maximum part of the rectangle of the query.
     */ 
		protected ${type}[] max;

		
		/**
     * SMAPed vector
     */ 

		protected ${type}[] smap;

    /**
     * Constructor.
     */
    public OBQuery${Type}(){
    }

    /**
     * Creates a new OBQuery${Type} object.
     * @param object
     *            The object that will be matched.
     * @param range
     *            The range to be used for the match.
     * @param result
     *            The priority queue were the results will be stored.
     */
    public OBQuery${Type}(O object, ${type} range, OBPriorityQueue${Type}<O> result){

        super(object,-1,range);
        this.result = result;
    }

		/**
		 * Returns true if the given rectangle collides
     * with this query.
     * @param rectangle The rectangle to search.
     */
		public synchronized boolean collides(${type}[][] rectangle){
				${type}[] minOther = rectangle[0];
				${type}[] maxOther = rectangle[1];
				boolean res = true;
				assert minOther.length == smap.length;
				int i = 0;
				while (i < minOther.length) {
				    if(max[i] < minOther[i] || min[i] > maxOther[i]){
								res =false;
								break;
						}
				    i++;
				}
				return res;
		}

		/**
     * Creates a new OBQuery${Type} object.
     * @param object
     *            The object that will be matched.
     * @param range
     *            The range to be used for the match.
     * @param result
     *            The priority queue were the results will be stored.
		 * @param smap 
     *            SMAP vector representation of the given object.
     */
    public OBQuery${Type}(O object, ${type} range, OBPriorityQueue${Type}<O> result, ${type}[] smap){

        this(object,range,result);
				this.smap = smap;
				if(smap != null){
						min = new ${type}[smap.length];
				max = new ${type}[smap.length];
				}
				updateRectangle();
				
    }

		private void updateRectangle(){
				if(smap != null){
				int i = 0;
				while (i < smap.length) {
				    min[i] = (${type})Math.max(smap[i] - distance, 0);
						max[i] = (${type})Math.min(smap[i] + distance, ${Type2}.MAX_VALUE);
				    i++;
				}
				}
		}

		/**
		 * Return low of the query rectangle.
     */ 
		public synchronized ${type}[] getLow(){
				return min;
		}

		/**
		 * Return low of the query rectangle.
     */ 
		public synchronized ${type}[] getHigh(){
				return max;
		}

    /**
     * @return The current results of the matching.
     */
    public OBPriorityQueue${Type}<O> getResult(){
        return result;
    }

    /**
     * Set the results of the matching to a new object.
     * @param result
     *            The new result.
     */
    public void setResult(OBPriorityQueue${Type}<O> result){
        this.result = result;
    }

   /**
    * Returns true if we should calculate the real distance.
    * @param smapDistance The lower-resolution distance calculated
    * with SMAP.
    * 
    */
    public boolean isCandidate(${type} smapDistance){
        return result.isCandidate(smapDistance);
    }

   /**
    * Add the given object, object id and distance of type float to the
    * queue. Updates the range of the query as needed if the range
    * shrinks after this insertion.
    * @param id
    *            The id of the object to be used
    * @param obj
    *            The object to be added
    * @param d
    *            The distance to be added
    * @throws IllegalAccessException
    *             If there is a problem when instantiating objects O
    * @throws InstantiationException
    *             If there is a problem when instantiating objects O
    */
    public synchronized void add(long id, O obj, ${type} d) throws InstantiationException, IllegalAccessException {
				result.add(id,obj,d);
				${type} temp = result.updateRange(this.distance);
				if(temp != this.distance){
						<#if type=="int" || type =="short" || type=="long">
						this.distance = (${type})(temp-(${type})1);
            if(this.distance < 0){
								this.distance = (${type})0;
						}

						<#else>
						this.distance = temp;
						</#if>

						updateRectangle();
        }
		}

		/**
		 * Returns true if the originalRange has been modified.
		 * @return true If the current range (getDistance()) is different than originalRange.
		 */
		public boolean updatedRange(${type} originalRange){
				return getDistance() != originalRange;
		}
}

</#list>
