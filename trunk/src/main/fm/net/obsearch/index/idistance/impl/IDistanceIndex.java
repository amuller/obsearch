<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#include "/@inc/index.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="IDistanceIndex${Type}.java" />
package net.obsearch.index.idistance.impl;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ListIterator;

import org.apache.log4j.Logger;

import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.cache.OBCache;
import net.obsearch.dimension.Dimension${Type};
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.Index${Type};

import net.obsearch.index.bucket.impl.BucketContainer${Type};
import net.obsearch.index.bucket.impl.BucketObject${Type};
import net.obsearch.index.idistance.AbstractIDistanceIndex;
import net.obsearch.index.utils.ByteArrayComparator;
import net.obsearch.index.utils.IntegerHolder;
import net.obsearch.ob.OB${Type};
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.OBQuery${Type};
import net.obsearch.result.OBPriorityQueue${Type};
import net.obsearch.result.OBResult${Type};
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStoreInt;
import net.obsearch.storage.TupleBytes;
import net.obsearch.utils.bytes.ByteBufferFactoryConversion;
import net.obsearch.filter.Filter;

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
	*  IDistanceIndex${Type} implements a metric index for ${type}s. The
  *  index employed is called the IDistance. The paper:
	* H. V. Jagadish, Beng Chin Ooi, Kian-Lee Tan, Cui Yu, Rui Zhang: iDistance: 
  *  An adaptive B+-tree based indexing method for nearest neighbor search. 
  *  ACM Trans. Database Syst. 30(2): 364-397 (2005)
	*  describes the technique. It is like the pyramid technique, but for
  * metric datasets.
  *
  *  @author      Arnoldo Jose Muller Molina    
  */
  <@gen_warning filename="IDistanceIndex.java "/>
public class IDistanceIndex${Type}<O extends OB${Type}>
		extends
		AbstractIDistanceIndex<O, BucketObject${Type}, OBQuery${Type}<O>, BucketContainer${Type}<O>>
		implements Index${Type}<O> {

	private static ByteArrayComparator comp = new ByteArrayComparator();

	private static transient final Logger logger = Logger
	.getLogger(IDistanceIndex${Type}.class);


	/**
	 * min max values per pivot.
	 */
	private OBStoreInt minMax;

	private OBCache minMaxCache;

	/**
	 * Creates a new iDistance index.
	 * 
	 * @param type
	 *            Type of object to store.
	 * @param pivotSelector
	 *            Pivot selection procedure that will be used.
	 * @param pivotCount
	 *            Number of pivots used to initialize the index.
	 * @throws OBStorageException
	 * @throws OBException
	 */
	public IDistanceIndex${Type}(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
	}
	
  <#-- default implementations of some methods -->

	<@existsDefault/>

	<@intersectingBoxesUnsupported/>

  <@intersectsUnsupported/>

  <@searchOBBoxesUnsupported/>

	@Override
	protected byte[] getAddress(BucketObject${Type} bucket) {

		${type}[] smap = bucket.getSmapVector();
		int i = 0;
		int iMin = -1;
		${type} minValue = ${ClassType}.MAX_VALUE;
		while (i < smap.length) {
			if (smap[i] < minValue) {
				iMin = i;
				minValue = smap[i];
			}
			i++;
		}
		assert iMin != -1;
		return buildKey(iMin, minValue);
	}

	private byte[] buildKey(int i, ${type} value) {
			byte[] pivotId = fact.serializeInt(i);
      byte[] v = fact.serialize${Type}(value);
		ByteBuffer buf = ByteBufferFactoryConversion.createByteBuffer(pivotId.length + v.length);
		buf.put(pivotId);
		buf.put(v);
		return buf.array();
	}

	@Override
	protected BucketObject${Type} getBucket(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		${type}[] smapTuple = Dimension${Type}.getPrimitiveTuple(super.pivots,
				object);
		return new BucketObject${Type}(smapTuple, -1);
	}

	@Override
	protected BucketContainer${Type}<O> instantiateBucketContainer(
			ByteBuffer data, byte[] address) {
		return new BucketContainer${Type}<O>(this, data, getPivotCount());
	}

  

	
  <@gen_warning filename="IDistanceIndex.java "/>
	@Override
	public void searchOB(O object, ${type} r, OBPriorityQueue${Type}<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
			searchOB(object,r,null,result);
	}

	public void searchOB(O object, ${type} r, Filter<O> filter, OBPriorityQueue${Type}<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		BucketObject${Type} b = getBucket(object);
		OBQuery${Type}<O> q = new OBQuery${Type}<O>(object, r, result, b
				.getSmapVector());
		Dimension${Type}[] smap = Dimension${Type}.transformPrimitiveTuple(b
				.getSmapVector());
		Arrays.sort(smap);
		LinkedList<DimensionProcessor> ll = new LinkedList<DimensionProcessor>();
		for (Dimension${Type} s : smap) {
				ll.add(new DimensionProcessor(b, q, s, filter));
		}
		IntegerHolder smapCount = new IntegerHolder(0);
		while (ll.size() > 0) {
			ListIterator<DimensionProcessor> pit = ll.listIterator();
			while (pit.hasNext()) {
					//					logger.info("Iteration" + stats.getDistanceCount());
				DimensionProcessor p = pit.next();
				if (p.hasNext()) {
					p.doIt(smapCount);
				} else {
					p.close();
					pit.remove();
				}
			}
			//if(q.getResult().getSize() == q.getResult().getK()){
				// do something when k is found.
			//}
		}

		stats.incSmapCount(smapCount.getValue());
	}

	

	/**
	 * Process from the closest dimensions little by little.
	 * 
	 * @author Arnoldo Muller Molina
	 * 
	 */
	private class DimensionProcessor {

		private CloseIterator<TupleBytes> itRight;
		private CloseIterator<TupleBytes> itLeft;
		boolean continueRight = true;
		boolean continueLeft = true;
		boolean iteration = true;// iteration id.
		BucketObject${Type} b;
		private OBQuery${Type}<O> q;
		Dimension${Type} s;
		byte[] centerKey;
		byte[] lowKey;
		byte[] highKey;
		${type} lastRange;
		Filter<O> filter;

		public DimensionProcessor(BucketObject${Type} b, OBQuery${Type}<O> q,
															Dimension${Type} s, Filter<O> filter) throws OBStorageException {
			super();
			this.b = b;
			this.q = q;
			this.s = s;
			updateHighLow();
			itRight = Buckets.processRange(centerKey, highKey);
			itLeft = Buckets.processRangeReverse(lowKey, centerKey);
			if(itLeft.hasNext()){
				itLeft.next();
			}
			this.filter = filter;
		}

		/**
		 * Update high and low intervals.
		 */
		private void updateHighLow() {
			${type} center = s.getValue();
			${type} low = q.getLow()[s.getOrder()];
			${type} high = q.getHigh()[s.getOrder()];
			centerKey = buildKey(s.getOrder(), center);
			lowKey = buildKey(s.getOrder(), low);
			highKey = buildKey(s.getOrder(), high);
			this.lastRange = q.getDistance();
		}

		public void close() throws OBException {
			itRight.closeCursor();
			itLeft.closeCursor();
		}

		public boolean hasNext() {
			return (itRight.hasNext() && continueRight)
					|| (itLeft.hasNext() && continueLeft);
		}

		/**
		 * Perform one matching iteration.
		 * 
		 * @param low
		 * @param high
		 * @return
		 * @throws InstantiationException
		 * @throws OBException
		 * @throws IllegalAccessException
		 * @throws IllegalIdException
		 */
		public void doIt(IntegerHolder smapCount) throws IllegalIdException,
				IllegalAccessException, OBException, InstantiationException {
				//			if (iteration) {
				if (itRight.hasNext() && continueRight) {
					TupleBytes t = itRight.next();
					// make sure we are within the key limits.
					if (comp.compare(t.getKey(), highKey) > 0) {
						continueRight = false;
					} else {
						BucketContainer${Type}<O> bt = instantiateBucketContainer(
								t.getValue(), null);
						stats
								.incDistanceCount(bt.searchSorted(q, b,
																									smapCount, filter));
						stats.incBucketsRead();
						stats.incDataRead(bt.getBytes().array().length);
						// update ranges
						if (q.updatedRange(lastRange)) {
							updateHighLow();
						}
					}
				}
				iteration = false;
				//} else {

				if (itLeft.hasNext() && continueLeft) {
					TupleBytes t = itLeft.next();
					if (comp.compare(t.getKey(), lowKey) < 0) {
						continueLeft = false;
					} else {
						BucketContainer${Type}<O> bt = instantiateBucketContainer(
								t.getValue(), null);
						stats
								.incDistanceCount(bt.searchSorted(q, b,
																									smapCount, filter));
						stats.incDataRead(bt.getBytes().array().length);
						stats.incBucketsRead();
						if (q.updatedRange(lastRange)) {
							updateHighLow();
						}
					}
				}
				iteration = true;
				//}
		}

	}

}

</#list>