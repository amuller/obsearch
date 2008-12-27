<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>

<@pp.changeOutputFile name="Dimension"+Type+".java" />
package net.obsearch.dimension;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.ob.OB${Type};
import net.obsearch.Index;
import java.util.Random;
import net.obsearch.exception.OBStorageException;
import cern.colt.list.LongArrayList;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.List;
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
	*  Dimension${Type} Stores objects of type ${type}.
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */
		 public class Dimension${Type} extends AbstractDimension implements Comparable<Dimension${Type}>{
				 
				 /**
					* The value of type ${type} of the dimension;
					*/
				 private ${type} value;
				 
				 /**
					* Creates a new Dimension${Type} object.
          * @param position The position of this dimension.
          * @param value The value of this dimension.
					*/
				 public Dimension${Type}(int position, ${type} value){
				     super(position);
						 this.value = value;
				 }

				 /**
					* Creates a dimension array from the given pivots and the given object.
					* @param pivots The pivots used for the embedding.
					* @param object The object to be projected.
					* @return A new dimension array.
					*/
				 public static Dimension${Type}[] getTuple(OB${Type}[] pivots, OB${Type} object
           ) throws 
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException

    {
        Dimension${Type}[]  res = new Dimension${Type} [pivots.length];
        int i = 0;
        for (OB${Type} p : pivots) {
            res[i] = new Dimension${Type}(i, p.distance(object));
            i++;
        }
        return res;
    }

  
   

	/**
	 * Selects k random elements from the given source.
	 * 
	 * @param k
	 *            number of elements to select
	 * @param r
	 *            Random object used to randomly select objects.
	 * @param source
	 *            The source of item ids.
	 * @param index
	 *            underlying index.
	 * @param will
	 *            not add pivots included in excludes.
   * @param minDistance The min distance required by the objects.
	 * @return The ids of selected objects.
	 */
	public static long[] select(int k, Random r, LongArrayList source,
															Index<OB${Type}> index, LongArrayList excludes, ${type} minDistance) throws IllegalIdException, OBException, IllegalAccessException, InstantiationException {
		int max = max(source, index);
		long[] res = new long[k];
		int i = 0;
		List<Long> l = new LinkedList<Long>();
		
		while (i < res.length) {
			long id = mapId(r.nextInt(max), source);
			if ((excludes == null || !excludes.contains(id)) && minDistance(l, index) > minDistance ) {
				res[i] = id;
				l.add(id);
			} else {
				l.remove(l.size() -1);
				continue; // repeat step.
			}
			i++;
		}
		return res;
	}


	

	/**
   * Calculate the minimum distance of the given objects (all objects
   * compared against all objects)
   * @param objects
   * @return the min distance
   */ 
  public static ${type} minDistance(List<Long> objects, Index<OB${Type}> index)throws IllegalIdException, OBException, IllegalAccessException, InstantiationException{
			${type} res = ${ClassType}.MAX_VALUE;
			Iterator<Long> it = objects.iterator();
			for(long l1 : objects){
					for(long l2 : objects){
							if(l1 == l2){
									continue;
							}
							res = (${type})Math.min(res, index.getObject(l1).distance(index.getObject(l2)));
					}
			}
			return res;
	}

				 /**
					* Creates a ${type} array from the given pivots and the given object.
					* @param pivots The pivots used for the embedding.
					* @param object The object to be projected.
					* @return A new ${type} array.
					*/
				  public static ${type}[] getBasicTuple(OB${Type}[] pivots, OB${Type} object
           ) throws 
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException

    {
        ${type}[]  res = new ${type} [pivots.length];
        int i = 0;
        for (OB${Type} p : pivots) {
            res[i] =  p.distance(object);
            i++;
        }
        return res;
    }

				 /**
					* Creates a dimension array from the given pivots and the given
					* object id. Loads the objects from the DB.
					*/
				  public static Dimension${Type}[] getTuple(long[] pivots, long objectId,
																				 Index<? extends OB${Type}>  index) throws 
            IllegalIdException, IllegalAccessException, InstantiationException,
							OBException{
							Dimension${Type}[]  res = new Dimension${Type} [pivots.length];
							OB${Type} o = index.getObject(objectId);
							int i = 0;
							for(long pivotId : pivots){
									res[i] = new Dimension${Type}(i, index.getObject(pivotId).distance(o));
									i++;
							}
							return res;
					}

					

					/**
					 * Compares this object with other. 
					 */
					public int compareTo(Dimension${Type} other){
							if(value < other.value){
									return -1;
							}else if(value > other.value){
									return 1;
							}else{
									return 0;
							}
					}


					public ${type} getValue(){
							return value;
					}



		/**
     * Calculates the smap tuple for the given objectId, and the given pivots
     * @param pivots
     * @param objectId
     * @return
     */
    public static ${type}[] getPrimitiveTuple(long[] pivots, long objectId,
            Index<? extends OB${Type}>  index) throws 
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException

    {
        ${type}[] res = new ${type}[pivots.length];
        int i = 0;
        OB${Type} o = index.getObject(objectId);
        for (long pivotId : pivots) {
            res[i] = index.getObject(pivotId).distance(o);
            i++;
        }
        return res;
    }

		/**
		 * Transform the given primitive tuple into a Dimension tuple.
		 * @param tuple The tuple that will be transformed.
		 * @return A Dimension${Type} of dimensions.
		 */
		public static Dimension${Type}[] transformPrimitiveTuple(${type}[] tuple){
				Dimension${Type}[] res = new Dimension${Type}[tuple.length];
				int i = 0;
				while(i < tuple.length){
						res[i] = new Dimension${Type}(i, tuple[i]);
						i++;
				}
				return res;
		}
    

    /**
     * Calculates the smap tuple for the given objectId, and the given pivots
     * @param pivots
     * @param objectId
     * @return
     */
    public static ${type}[] getPrimitiveTuple(OB${Type}[] pivots, OB${Type} object
           ) throws 
            IllegalIdException, IllegalAccessException, InstantiationException,
            OBException

    {
        ${type}[] res = new ${type}[pivots.length];
        int i = 0;
        for (OB${Type} p : pivots) {
            res[i] = p.distance(object);
            i++;
        }
        return res;
    }
    
    /**
     * Calculates L-inf for two ${type} tuples.
     * @param a  tuple
     * @param b tuple
     * @return L-infinite for a and b.
     */
    public static ${type} lInfinite(${type}[] a, ${type}[] b){
        assert a.length == b.length;
        ${type} max = <@min_value/>;
        int i = 0;
        while(i < a.length){
            // ${type} t = (${type})Math.abs(a[i]  - b[i]);
						${type} t = (${type})(a[i]  - b[i]);
						t = t < 0 ? (${type})-t : t;
            if(t > max){
                max = t;
            }
            i++;
        }
        return max;
    }


		/**
     * Calculates the euc distance for two tuples
     * @param a  tuple
     * @param b tuple
     * @return L-infinite for a and b.
     */
    public static ${type} euclidean(${type}[] a, ${type}[] b){
        assert a.length == b.length;
        double result = 0;
				int i = 0;
        while(i < a.length){
            double j = a[i] - b[i];
            result += j * j;
            i++;
        }
        return (${type})Math.sqrt(result);
    }

		 }


</#list>