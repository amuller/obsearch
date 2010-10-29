package net.obsearch.pivots;

import java.lang.reflect.Array;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

import com.sleepycat.je.DatabaseException;

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
 * AbstractIncrementalPivotSelector holds common functionality to all the
 * incremental pivot selectors.
 * 
 * @author Arnoldo Jose Muller Molina
 */

public abstract class AbstractIncrementalPivotSelector<O extends OB>  {
	
	protected Random r = new Random();
	
	private static Logger logger = Logger.getLogger(AbstractIncrementalPivotSelector.class.toString());

	protected AbstractIncrementalPivotSelector(Pivotable<O> pivotable) {
		this.pivotable = pivotable;
	}
	
	protected AbstractIncrementalPivotSelector(){
		
	}

	/**
	 * Pivotable objects determine if a given object is suitable. For example,
	 * in the case of trees, very big trees will become a burden and we should
	 * avoid using them as pivots.
	 */
	protected Pivotable<O> pivotable;

	// TODO: The id auto increment must be initialized properly. We should leave
	// this
	// auto-increment to the underlying storage system.

	/**
	 * Returns the given object. If elements != null, then the returned item id
	 * is elements[i].
	 * 
	 * @param i
	 *            The id in the database or in elements of the object that will
	 *            be accessed.
	 * @param elements
	 *            Elements that will be searched.
	 * @return O object of the corresponding id.
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws DatabaseException
	 * @throws IllegalIdException
	 * @throws OBException
	 */
	protected final O getObject(long i, LongArrayList elements, Index<O> index)
			throws IllegalAccessException, InstantiationException,
			 IllegalIdException, OBException {

		return index.getObject(mapId(i, elements));
	}

	protected long mapId(long i, LongArrayList elements) {
		if (elements != null) {
			return elements.get((int) i);
		} else {
			return i;
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see net.obsearch.pivots.IncrementalPivotSelector#generatePivots(int,
	 * net.obsearch.result.Index)
	 */
	public PivotResult generatePivots(int pivotsCount, Index<O> index)
			throws OBException, IllegalAccessException, InstantiationException,
			OBStorageException, PivotsUnavailableException {

		return generatePivots(pivotsCount, null, index);
	}
	
	public abstract PivotResult generatePivots(int pivotCount, LongArrayList elements,  Index<O> index) throws OBException,
    IllegalAccessException, InstantiationException, OBStorageException,
    PivotsUnavailableException; 

	/**
	 * Returns the max # of elements. if source != null then source.size()
	 * otherwise index.databaseSize();
	 * 
	 * @param source
	 *            The source of data (can be null)
	 * @param index
	 *            The underlying index.
	 * @return The max # of elements of source if source != null or of index if
	 *         source == null.
	 */
	protected int max(LongArrayList source, Index<O> index)
			throws OBStorageException {
		int max;
		if (source == null) {	
			max = (int) Math.min(index.databaseSize(), Integer.MAX_VALUE);
		} else {
			max = source.size();
		}
		assert max >= 0 : "src: " + source + " index: " + index.databaseSize();
		return max;
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
	 * @return The ids of selected objects.
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 * @throws OBException 
	 * @throws IllegalIdException 
	 */
	protected long[] select(int k, Random r, LongArrayList source,
			Index<O> index, LongArrayList excludes) throws OBStorageException, IllegalIdException, OBException, IllegalAccessException, InstantiationException {
		return selectUnique(k, r, source,index,excludes);
	}
	
	
	protected long[] selectUnique(int k, Random r, LongArrayList source,
			Index<O> index, LongArrayList excludes) throws OBStorageException, IllegalIdException, OBException, IllegalAccessException, InstantiationException {
		int max = max(source, index);
		long[] res = new long[k];
		int i = 0;
		int excludesSize = 0;
		if(excludes != null){
			excludesSize = excludes.size();
		}
		HashSet<Long> excludesSet = new HashSet<Long>(excludesSize);
		HashSet<Long> generatedSet = new HashSet<Long>(k);
		int cx = 0;
		while(cx < excludesSize){
			excludesSet.add(excludes.get(cx));
			cx++;
		}
		LongArrayList generated = new LongArrayList(k);
		while (i < res.length) {
			
			long id = mapId(r.nextInt(max), source);
			if (excludes == null || !excludesSet.contains(id) && ! generatedSet.contains(id)) {
				res[i] = id;
				generated.add(id);
				generatedSet.add(id);
			} else {
				continue; // repeat step.
			}
			i++;
		}
		return res;
	}

	protected O[] selectO(int k, Random r, LongArrayList source,
			Index<O> index, LongArrayList excludes) throws IllegalIdException,
			IllegalAccessException, InstantiationException,
			OBException {
		long[] sample = select(k, r, source, index, null);
		O[] objs = (O[]) Array.newInstance(index.getType(), k);
		int i = 0;
		for (long l : sample) {
			objs[i] = this.getObject(l, source, index);
			i++;
		}
		return objs;
	}

}
