package net.obsearch.index.ghs;

import hep.aida.bin.StaticBin1D;
import it.unimi.dsi.io.InputBitStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import net.obsearch.AbstractOBResult;
import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.dimension.AbstractDimension;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.filter.FilterNonEquals;
import net.obsearch.index.bucket.AbstractBucketIndex;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.result.OBResultInt;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.TupleBytes;

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
 * AbstractSketch64 encapsulates functionality common to a Sketch. The maximum
 * number of bits has been artificially set to 64 bits to optimize performance.
 * If you use less than 64 bits the keys used for the sketch only use the amount
 * of bits the user specifies. Nevertheless the handling of priority queues and
 * the matching itself occurs on longs. It should affect considerably AMD64
 * architectures. If you are using a 32 bits processor or operating system, then
 * this implementation will not be as efficient.
 * 
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractSketch64<O extends OB, B extends BucketObject<O>, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractBucketIndex<O, B, Q, BC> {

	private static final transient Logger logger = Logger
			.getLogger(AbstractSketch64.class.getName());

	/**
	 * Estimators for k ranges. This object tells us the amount of buckets we
	 * should read for a given k query to match a certain value of error.
	 */
	private StaticBin1D[] kEstimators;

	/**
	 * Number of samples employed to generate the k estimators.
	 */
	private int sampleSize = 1000;

	/**
	 * For a query k we take the kEstimators[k] estimation and return the value
	 * kEstimators[k].mean() + (kEstimators[k].stdDev() * kAlpha)
	 */
	private float kAlpha = 2f; // two standard deviations, 95% precision

	/**
	 * Maximum k that will be returned by this index. This is required for the
	 * estimation.
	 */
	private int maxK = 50;

	/**
	 * The target EP value used in the estimation.
	 */
	private double expectedEP = 0.0;

	private Random r = new Random();

	/**
	 * Number of bits.
	 */
	private int m;

	/**
	 * A compressed bit set for 64 bits.
	 */
	private transient CompressedBitSet64 sketchSet;

	public AbstractSketch64(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount, int m)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
		this.m = m;
	}

	/**
	 * For a query k we take the kEstimators[k] estimation and return the value
	 * kEstimators[k].mean() + (kEstimators[k].stdDev() * kAlpha) This method
	 * sets kAlpha.
	 * 
	 * @param kAlpha
	 *            new kAlpha to set.
	 */
	public void setKAlpha(float kAlpha) {
		this.kAlpha = kAlpha;
	}

	/**
	 * Set the max value used to calculate the estimation.
	 * 
	 * @param maxK
	 */
	public void setMaxK(int maxK) {
		this.maxK = maxK;
	}

	/**
	 * Set the sample size of the estimation
	 * 
	 * @param size
	 *            the new size
	 */
	public void setSampleSize(int size) {
		this.sampleSize = size;
	}

	/**
	 * Set the expected ep for the k estimation process. We will search for the
	 * number of buckets required to satisfy a value less than ep for a k-nn
	 * query.
	 * 
	 * @param ep
	 *            EP value.
	 * 
	 */
	public void setExpectedEP(double ep) {
		this.expectedEP = ep;
	}

	/**
	 * Calculate the estimators.
	 * 
	 * @throws IllegalIdException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	protected void calculateEstimators() throws IllegalIdException,
			OBException, IllegalAccessException, InstantiationException {
		maxKEstimation();
	}

	public void init(OBStoreFactory fact) throws OBStorageException,
			OBException, InstantiationException, IllegalAccessException {
		super.init(fact);
		// we have to load the masks before the match begins!
		loadMasks();
	}

	protected void initByteArrayBuckets() throws OBException {
		OBStorageConfig conf = new OBStorageConfig();
		conf.setTemp(false);
		conf.setDuplicates(false);
		conf.setBulkMode(!isFrozen());
		conf.setRecordSize(primitiveDataTypeSize());
		this.Buckets = fact.createOBStore("Buckets_byte_array", conf);

	}

	/**
	 * Load the masks from the storage device into memory.
	 * 
	 * @throws OBException
	 */
	protected void loadMasks() throws OBException {
		logger.fine("Loading masks!");
		sketchSet = new CompressedBitSet64();
		CloseIterator<TupleBytes> it = Buckets.processAll();
		// assert Buckets.size() == A.size();
		while (it.hasNext()) {
			TupleBytes t = it.next();
			long bucketId = fact.deSerializeLong(t.getKey());
			sketchSet.add(bucketId);
		}
		it.closeCursor();
		// load the sketch into memory.
		sketchSet.commit();
	}

	/**
	 * Sort all masks, and then start the search until the EP is less than some
	 * threshold. Do this for each k.
	 * 
	 * @throws IllegalIdException
	 * @throws OBException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 */
	private void maxKEstimation() throws IllegalIdException, OBException,
			IllegalAccessException, InstantiationException {
		kEstimators = new StaticBin1D[maxK + 1];
		logger.fine("Max k estimation");
		int i = 0;
		while (i < kEstimators.length) {
			kEstimators[i] = new StaticBin1D();
			i++;
		}

		long[] sample = AbstractDimension.select(sampleSize, r, null,
				(Index) this, null);
		O[] sampleSet = getObjects(sample);
		i = 0;
		for (O o : sampleSet) {
			logger.finer("Estimating k: " + i);
			maxKEstimationAux(o);
			i++;
		}

	}

	private byte[] convertLongToBytesAddress(long bucketId) {
		return fact.serializeLong(bucketId);
	}

	protected abstract long getLongAddress(B b);

	/**
	 * Get the kMax closest objects. Count how many different bucket ids are
	 * there for each k and fill in accordingly the tables.
	 * 
	 * @param object
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	protected void maxKEstimationAux(O object) throws OBException,
			InstantiationException, IllegalAccessException {
		// we calculate first the list of elements against the DB.
		B b = getBucket(object);

		long longAddr = getLongAddress(b);

		byte[] addr = convertLongToBytesAddress(longAddr);
		AbstractOBQuery<O> dbQueue = getKQuery(object, (int) databaseSize());
		int i = 0;
		long max = databaseSize();
		while (i < max) {
			O obj = getObject(i);
			if (!obj.equals(object)) {
				dbQueue.add(i, obj);
			}
			i++;
		}
		List<AbstractOBResult<O>> db = dbQueue.getSortedElements();

		// we now calculate the buckets and we sort them
		// according to the distance of the query.
		long[] sortedBuckets = searchBuckets(longAddr, sketchSet.size());

		// now we have to calculate the EP for each k up to maxK
		// and for each k we calculate how many buckets must be read in order
		// to obtain ep less than
		i = 0;
		BC container = this.instantiateBucketContainer(null, addr); // bucket
		// container
		// used to
		// read
		// data.
		FilterNonEquals<O> fne = new FilterNonEquals<O>();
		while (i < maxK) {
			double ep = 1;
			int goodK = 0;
			// get the query for the
			AbstractOBQuery<O> query = getKQuery(object, i + 1);
			for (long result : sortedBuckets) {
				container.setKey(convertLongToBytesAddress(result));
				// search the objects
				container.search(query, b, fne, getStats());
				// calculate the ep of the query and the DB.
				if (query.isFull()) { // only if the query is full of items.
					ep = query.ep(db);
				}
				goodK++;
				if (ep < this.expectedEP) {
					// add the information to the stats:
					// goodK buckets required to retrieve with k==i.
					kEstimators[i].add(goodK);
					break; // we are done.
				}
			}
			i++;
		}

	}

	/**
	 * Returns a k query for the given object.
	 * 
	 * @param object
	 *            (query object)
	 * @param k
	 *            the number of objects to accept in the query.
	 * @return
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws OBException
	 */
	protected abstract AbstractOBQuery<O> getKQuery(O object, int k)
			throws OBException, InstantiationException, IllegalAccessException;

	/**
	 * Search the f closest buckets to the given query. We drop the distance
	 * values for performance reasons, but we could add them if we wanted in the
	 * future.
	 * 
	 * @param query
	 *            the query to employ
	 * @param maxF
	 *            the max number of items that will be returned
	 * @return
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 * @throws OBException
	 */
	protected long[] searchBuckets(long query, int maxF)
			throws InstantiationException, IllegalAccessException, OBException {
		return sketchSet.searchBuckets(query, maxF, m);
	}

	/**
	 * Estimate the k needed for a k-nn query.
	 * 
	 * @param k
	 *            of the k-nn query.
	 * @return Number of buckets that should be retrieved for this query.
	 */
	public int estimateK(int k) {
		long x = Math.round(this.kEstimators[k - 1].mean()
				+ (this.kEstimators[k - 1].standardDeviation() * kAlpha));
		assert x <= Integer.MAX_VALUE;
		return (int) x;
	}

}
