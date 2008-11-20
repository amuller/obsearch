package net.obsearch.index.dprime;

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

import hep.aida.bin.StaticBin1D;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import net.obsearch.AbstractOBPriorityQueue;
import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.cache.OBCacheHandlerLong;
import net.obsearch.cache.OBCacheLong;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.IndexShort;
import net.obsearch.index.bucket.AbstractBucketIndex;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.index.bucket.SimpleBloomFilter;
import net.obsearch.index.utils.OBFactory;
import net.obsearch.index.utils.StatsUtil;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStore;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreInt;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.TupleLong;
import net.obsearch.utils.BloomFilter64bit;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseException;
import com.thoughtworks.xstream.XStream;

/**
 * AbstractDIndex contains functionality common to specializations of the
 * D-Index for primitive types.
 * 
 * @param < O >
 *            The OB object that will be stored.
 * @param < B >
 *            The Object bucket that will be used.
 * @param < Q >
 *            The query object type.
 * @param < BC>
 *            The Bucket container.
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractDPrimeIndex<O extends OB, B extends BucketObject, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractBucketIndex<O, B, Q, BC> {
	/**
	 * Logger.
	 */
	private static final transient Logger logger = Logger
			.getLogger(AbstractDPrimeIndex.class);

	/**
	 * Filter used to avoid unnecessary block accesses.
	 */
	// protected ArrayList< SimpleBloomFilter<Long>> filter;
	// protected ArrayList<BloomFilter64bit> filter = new
	// ArrayList<BloomFilter64bit>();
	/**
	 * Masks used to speedup the generation of hash table codes.
	 */
	protected long[] masks;

	/**
	 * Accumulated pivots per level (2 ^ pivots per level) so that we can
	 * quickly compute the correct storage device
	 */
	private long[] accum;

	protected transient BinaryTrie filter;

	/**
	 * Initializes this abstract class.
	 * 
	 * @param fact
	 *            The factory that will be used to create storage devices.
	 * @param pivotCount
	 *            # of Pivots that will be used.
	 * @param pivotSelector
	 *            The pivot selection mechanism to be used. (64 pivots can
	 *            create 2^64 different buckets, so we have limited the # of
	 *            pivots available. If you need more pivots, then you would have
	 *            to use the Smapped P+Tree (PPTree*).
	 * @param type
	 *            The type that will be stored in this index.
	 * @param nextLevelThreshold
	 *            How many pivots will be used on each level of the hash table.
	 * @throws OBStorageException
	 *             if a storage device could not be created for storing objects.
	 */
	protected AbstractDPrimeIndex(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount)
			throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount);
	}

	public void init(OBStoreFactory fact) throws OBStorageException,
			OBException, InstantiationException, IllegalAccessException {
		super.init(fact);

		// initialize the masks;
		int i = 0;
		long mask = 1;
		this.masks = new long[64];
		while (i < 64) {
			logger.debug("i: " + i + " " + Long.toBinaryString(mask));
			masks[i] = mask;
			mask = mask << 1;
			i++;
		}
		loadFilter();
	}

	public String debug(O object) throws OBException, InstantiationException,
			IllegalAccessException {
		B b = this.getBucket(object);
		return b.toString() + "\naddr:\n "
				+ Long.toBinaryString(this.getBucketId(b)) + "\n"
				+ "Is in filter: " + filter.containsInv(getBucketId(b));
	}

	public void freeze() throws IOException, AlreadyFrozenException,
			IllegalIdException, IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		// get n pivots.
		// ask the bucket for each object and insert those who are not excluded.
		// repeat iteratively with the objects that could not be inserted until
		// the remaining
		// objects are small enough.
		super.freeze();

		// initialize bloom filter
		int i = 0;
		// this.filter = new ArrayList<SimpleBloomFilter<Long>>();

		/*
		 * this.filter = new ArrayList<BloomFilter64bit>(); long size =
		 * A.size(); while (i < getPivotCount()) { // filter.add(new
		 * SimpleBloomFilter<Long>(i 1000, // (int)Math.pow(2, i)));
		 * filter.add(new BloomFilter64bit(Math.min((int) Math.pow(2, i + 1),
		 * (int) (size) / 10), i + 1)); i++; }
		 */

		filter = new BinaryTrie();

		// the initial list of object from which we will generate the pivots
		LongArrayList elementsSource = null;
		// After generating the pivots, we put here the objects that fell
		// into the exclusion bucket.

		int maxBucketSize = 0;

		int insertedObjects = 0;
		boolean cont = true;

		// calculate medians required to be able to use the bps
		// function.
		calculateMedians(elementsSource);
		i = 0;
		int max;
		if (elementsSource == null) {
			max = (int) A.size();
		} else {
			max = elementsSource.size();
		}
		CloseIterator<TupleLong> it = A.processAll();
		while (it.hasNext()) {
			TupleLong t = it.next();
			long id = t.getKey();
			O o = super.bytesToObject(t.getValue());
			B b = getBucket(o);
			updateProbabilities(b);
			if (i % 100000 == 0) {
				logger.debug("Adding... " + i + " trie: "
						+ BinaryTrie.objectCount);
				// logger.debug(getStats());
			}

			b.setId(idMap(i, elementsSource));
			// TODO: we have to use bulk mode here.
			this.insertBucketBulk(b, o);
			i++;
		}
		logger.debug("Trie size: " + BinaryTrie.objectCount);
		it.closeCursor();
		normalizeProbs();

		// bucketStats();


		logger.debug("Max bucket size: " + maxBucketSize);
		logger.debug("Bucket count: " + A.size());

		storeFilter();
	}

	/**
	 * Serialize the filter and store it in the root of the factory's directory.
	 * 
	 * @throws IOException
	 */
	private void storeFilter() throws OBException {
		logger.debug("Storing filter");
		String base = fact.getFactoryLocation();
		try {
			FileOutputStream fos = new FileOutputStream(
					new File(base, "filter"));
			ObjectOutputStream out = new ObjectOutputStream(fos);
			out.writeObject(filter);
		} catch (IOException e) {
			throw new OBException(e);
		}
		logger.debug("Filter was stored");
	}

	/**
	 * Serialize the filter and store it in the root of the factory's directory.
	 * 
	 * @throws IOException
	 */
	private void loadFilter() throws OBException {
		if(this.isFrozen()){
		logger.debug("Loading filter");
		String base = fact.getFactoryLocation();
		try {
			File inFile = new File(base, "filter");
			logger.debug("Loading filter from base: " + base);
			FileInputStream fos = new FileInputStream(inFile);
			ObjectInputStream in = new ObjectInputStream(fos);
			filter = (BinaryTrie) in.readObject();
		} catch (IOException e) {
			throw new OBStorageException(e);
		} catch (ClassNotFoundException e) {
			throw new OBException(e);
		}
		logger.debug("Filter was read");
		}
	}

	/**
	 * Return the bucket id of the given bucket
	 * 
	 * @param b
	 *            The bucket that will be processed.
	 * @return The bucket id of b.
	 */
	protected abstract long getBucketId(B b);

	/*
	 * protected void bucketStats() throws OBStorageException,
	 * IllegalIdException, IllegalAccessException, InstantiationException,
	 * OBException {
	 * 
	 * CloseIterator<TupleBytes> it = Buckets.processAll(); StaticBin1D s = new
	 * StaticBin1D(); while (it.hasNext()) { TupleBytes t = it.next(); BC bc =
	 * instantiateBucketContainer(null,t.getKey()); s.add(bc.size()); } // add
	 * exlucion logger.info(StatsUtil.prettyPrintStats("Bucket distribution",
	 * s)); it.closeCursor(); }
	 */

	/**
	 * Updates probability information.
	 * 
	 * @param b
	 */
	protected abstract void updateProbabilities(B b);

	protected abstract void normalizeProbs() throws OBStorageException;

	/**
	 * Calculate median values for pivots of level i based on the
	 * elementsSource. If elementsSource == null, all the objects in the DB are
	 * used.
	 * 
	 * @param level
	 *            The level that will be processed.
	 * @param elementsSource
	 *            The objects that will be processed to generate median data
	 *            information.
	 * @throws OBStorageException
	 *             if something goes wrong with the storage device.
	 */
	protected abstract void calculateMedians(LongArrayList elementsSource)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException;

	/**
	 * Stores the given bucket b into the {@link #Buckets} storage device. The
	 * given bucket b should have been returned by {@link #getBucket(OB, int)}
	 * 
	 * @param b
	 *            The bucket in which we will insert the object.
	 * @param object
	 *            The object to insert.
	 * @return A OperationStatus object with the new id of the object if the
	 *         object was inserted successfully.
	 * @throws OBStorageException
	 */
	protected OperationStatus insertBucket(B b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		// get the bucket id.
		long bucketId = getBucketId(b);
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.

		updateFilters(bucketId);
		return super.insertBucket(b, object);
	}

	protected OperationStatus insertBucketBulk(B b, O object)
			throws OBStorageException, IllegalIdException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		// get the bucket id.
		long bucketId = getBucketId(b);
		// if the bucket is the exclusion bucket
		// get the bucket container from the cache.

		updateFilters(bucketId);
		return super.insertBucketBulk(b, object);
	}

	/** updates the bucket identification filters */
	private void updateFilters(long x) {
		String s = Long.toBinaryString(x);
		filter.add(getPivotCount(), s);
		/*
		 * int max = s.length(); int i = s.length() - 1; int cx = 0; while (i >=
		 * 0) { long j = Long.parseLong(s.substring(i, max), 2); // if(!
		 * filter.get(cx).contains(j)){ filter.get(cx).add(j); // } i--; cx++; }
		 * // add the long to the rest of the layers. while (max <
		 * getPivotCount()) { filter.get(max).add(x); max++; }
		 */
	}

	

}
