<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="OBQuery"+Type+".java" />
package net.obsearch.query;
import java.util.List;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Set;
import net.obsearch.ob.OB${Type};
import net.obsearch.result.OBResult${Type};
import net.obsearch.result.OBPriorityQueue${Type};
import net.obsearch.exception.OBException;
import net.obsearch.AbstractOBResult;
import net.obsearch.asserts.OBAsserts;
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


public final class OBQuery${Type}<O extends OB${Type}> extends AbstractOBQuery<O> {


		private OBResult${Type}<O> query;
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

		public O getObject(){
				return query.getObject();
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

        query = new OBResult${Type}<O>(object,-1,range);
        this.result = result;
    }

		public OBQuery${Type}(O object, OBPriorityQueue${Type}<O> result){
				this(object, ${ClassType}.MAX_VALUE, result);
		}

		/**
		 * Returns true if the given rectangle collides
     * with this query.
     * @param rectangle The rectangle to search.
     */
		public boolean collides(${type}[][] rectangle){
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
				    min[i] = (${type})Math.max(smap[i] - query.getDistance(), 0);
						max[i] = (${type})Math.min(smap[i] + query.getDistance(), ${ClassType}.MAX_VALUE);
				    i++;
				}
				}
		}

		/**
		 * Return low of the query rectangle.
     */ 
		public ${type}[] getLow(){
				return min;
		}

		/**
		 * Return low of the query rectangle.
     */ 
		public ${type}[] getHigh(){
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
        return smapDistance <= query.getDistance() && result.isCandidate(smapDistance);
    }


		public  boolean add(long id, O object) throws InstantiationException, IllegalAccessException , OBException{
				return add(id, object,  query.getObject().distance(object));
		}

		public ${type} getDistance(){
				return query.getDistance();
		}

		public List<AbstractOBResult<O>> getSortedElements(){
				List<AbstractOBResult<O>> res = new LinkedList<AbstractOBResult<O>>();
				for(OBResult${Type}<O> r : result.getSortedElements()){
						res.add(r);
				}
				return res;	
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
    public boolean add(long id, O obj, ${type} d) throws InstantiationException, IllegalAccessException {
				boolean res = result.add(id,obj,d);
				${type} temp = result.updateRange(query.getDistance());
				if(temp != query.getDistance()){
						<#if type=="int" || type =="short" || type=="long">
  					query.setDistance( (${type})(temp-(${type})1));
            if(query.getDistance() < 0){
								query.setDistance( (${type})0);
						}

						<#else>
								 query.setDistance( temp );
						</#if>

						updateRectangle();
        }
				return res;
		}

		/**
		 * Returns true if the originalRange has been modified.
		 * @return true If the current range (getDistance()) is different than originalRange.
		 */
		public boolean updatedRange(${type} originalRange){
				return query.getDistance() != originalRange;
		}

		/**
		 * @return true if the underlying priority queue's size is equal to k
     */
		public boolean isFull(){
				return result.isFull();
		}

		

		public  double recall(List<AbstractOBResult<O>> perfectQuery){
				int hits = 0;
				Set<AbstractOBResult<O>> s = new HashSet<AbstractOBResult<O>>();
				for(OBResult${Type}<O> r : this.result.getSortedElements()){			
						int i = 0;
						for(AbstractOBResult<O> d : perfectQuery){
								if(! s.contains(d) && d.compareTo(r) == 0){
										s.add(d);
										hits++;
										break;
								}
								i++;
						}
				}
				return (double)hits / (double)perfectQuery.size();
		}


		public  double ep(List<AbstractOBResult<O>> dbin){
				List<OBResult${Type}<O>> query = getResult().getSortedElements();
				int i = 0;
				int result = 0;
				Set<OBResult${Type}<O>> s = new HashSet<OBResult${Type}<O>>();
				for(OBResult${Type}<O> r : query){
						// find the position in the db. 
						int cx = 0;
						for(AbstractOBResult<O> cr : dbin){
								OBResult${Type}<O> c = (OBResult${Type}<O>)cr;
								if(! s.contains(c) &&c.compareTo(r) == 0){
										s.add(c);
										result += cx - i;
										break;
								}
								cx++;
						}
						i++;
				}
				if(query.size() == 0){
						return 0;
				}else{
						double res = ((double)result)/ ((double)(query.size() * dbin.size()));
						return res;
				}
		}


		/**
     * Calculate the EP value for a sorted list of distances.
     */
		public  double ep(${type}[] dbin){
				return epAux(dbin) * (1 / (double)getResult().getSize());
		}


		/**
     * Calculate the EP value for a sorted list of distances.
		 * Does not multiply by 1/k
     */
		private  double epAux(${type}[] dbin){
				List<OBResult${Type}<O>> query = getResult().getSortedElements();
				int i = 0;
				int result = 0;
        // hold the visited elements
				Set<Integer> s = new HashSet<Integer>();
				for(OBResult${Type}<O> r : query){
						// find the position in the db. 
						int cx = 0;
						for(${type} cr : dbin){								
								if(! s.contains(cx) && cr == r.getDistance()){
										s.add(cx);
										result += cx - i;
										break;
								}
								cx++;
						}
						i++;
				}
				if(query.size() == 0){
						return 0;
				}else{
						double res = ((double)result)/ ((double)(dbin.length));
						return res;
				}
		}


		/**
		 * Calculates ep without multiplying by 1/k and 
		 */ 
		public  double compound(${type}[] dbin){
				List<OBResult${Type}<O>> query = getResult().getSortedElements();
				int i = 0;
				
				double res = 0;
        // hold the visited elements
				Set<Integer> s = new HashSet<Integer>();
				for(OBResult${Type}<O> r : query){

						double ep = 1 - (((double)rank(r.getDistance(), dbin) - (i+1))/(double)(dbin.length));
						// fix rounding error
						res += ((double)dbin[i] / (double)r.getDistance()) * ep;
						i++;
				}
				return res / (double) query.size();
		}

		/*
		 * Calculates the relative distance error.
		 */
    public double rde(${type}[] dbin){
				return rdeAux(dbin) / (double) getResult().getSize();
		}

		/*
		 * Calculates the relative distance error without dividing by 1/k
		 */
		private  double rdeAux(${type}[] dbin){
				List<OBResult${Type}<O>> query = getResult().getSortedElements();
				int i = 0;
				double result = 0;
        // hold the visited elements
				for(OBResult${Type}<O> r : query){
						// find the position in the db. 
						result +=  ( (double) r.getDistance() / (double)dbin[i]) - 1; 
						i++;
				}
				return result;
		}

		/** 
     * Calculates the precision
		 */
		public double precision(${type}[] dbin){
				List<OBResult${Type}<O>> query = getResult().getSortedElements();
				int count = 0;
        // hold the visited elements
				for(OBResult${Type}<O> r : query){
						if(rank(r.getDistance(), dbin) <= query.size()) {
								count++;
						}
				}
				return (double) count / (double) query.size();
		}

		/**
		 * find the position of distance in the given dbin list
		 */ 
		private int rank(${type} distance, ${type}[] dbin){
				int i = 0;
				while(i < dbin.length){
						if(distance == dbin[i]){
								break;
						}
						i++;
				}
				return i + 1;
		}

		/**
		 * peek to the largest value if the queue is full.
		 */
		public ${type} peek() throws OBException{
				OBAsserts.chkAssert(isFull(), "Queue is not full");
				return result.peek().getDistance();
		}
		
}

</#list>
