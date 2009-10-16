package net.obsearch.index.ghs;

import hep.aida.bin.StaticBin1D;
import it.unimi.dsi.io.InputBitStream;

import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Array;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import cern.colt.bitvector.BitVector;

import net.obsearch.AbstractOBResult;
import net.obsearch.OB;
import net.obsearch.OperationStatus;
import net.obsearch.Status;
import net.obsearch.cache.OBCacheByteArray;
import net.obsearch.cache.OBCacheHandlerByteArray;
import net.obsearch.constants.ByteConstants;
import net.obsearch.constants.OBSearchProperties;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.filter.FilterNonEquals;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.index.sorter.AbstractBucketSorter;
import net.obsearch.pivots.IncrementalPairPivotSelector;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.PivotPairResult;
import net.obsearch.pivots.PivotResult;
import net.obsearch.query.AbstractOBQuery;
import net.obsearch.result.OBResultInt;
import net.obsearch.result.OBResultInvertedByte;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.OBStorageConfig;
import net.obsearch.storage.OBStoreFactory;
import net.obsearch.storage.OBStoreLong;
import net.obsearch.storage.TupleBytes;
import net.obsearch.storage.TupleLong;
import net.obsearch.storage.OBStorageConfig.IndexType;
import net.obsearch.storage.cuckoo.CuckooEntry;
import net.obsearch.utils.Pair;
import net.obsearch.utils.bytes.ByteConversion;

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
		extends AbstractBucketSorter<O, B, Q, BC, SketchProjection, CBitVector> {

	static final transient Logger logger = Logger
			.getLogger(AbstractSketch64.class.getName());

	/**
	 * Number of bits.
	 */
	protected int m;

	

	/**
	 * Only two values per dimension (1, 0)
	 */
	private static final int HEIGHT = 2;

	/**
	 * pivot grid.
	 */
	protected O[][] pivotGrid;

	/**
	 * Distortion stats
	 */
	protected int[][] distortionStats;

	/**
	 * Pivot selector for the masks.
	 */
	protected IncrementalPairPivotSelector<O> maskPivotSelector;

	public AbstractSketch64(Class<O> type,
			IncrementalPairPivotSelector<O> pivotSelector, int m,
			int bucketPivotCount) throws OBStorageException, OBException {
		super(type, null, 0, bucketPivotCount);
		this.m = m;

		this.maskPivotSelector = pivotSelector;
		pivotGrid = (O[][]) Array.newInstance(type, m, HEIGHT);
		distortionStats = new int[m][HEIGHT];
	}

	protected  byte[] compactRepresentationToBytes(CBitVector cp){
		return cp.store();
	}
	
	protected CBitVector bytesToCompactRepresentation(byte[] data){
		return new CBitVector(data, m);
	}

	

	public void freeze() throws AlreadyFrozenException, IllegalIdException,
			OutOfRangeException, IllegalAccessException,
			InstantiationException, OBException, PivotsUnavailableException,
			IOException {
		super.freeze();
		int i = 0;

		/*
		 * while (i < m) { int cx = 0; PivotResult r =
		 * super.selectPivots(HEIGHT, this.maskPivotSelector); while (cx <
		 * HEIGHT) { pivotGrid[i][cx] = getObject(r.getPivotIds()[cx]); cx++; }
		 * i++; }
		 */

		PivotPairResult<O> pivots = maskPivotSelector.generatePivotsPair(m,
				null, this);
		// fill the pivots in the grid.
		i = 0;
		for (Pair<O, O> p : pivots.getPairs()) {
			pivotGrid[i][0] = p.getA();
			pivotGrid[i][1] = p.getB();
			i++;
		}

		logger.info("Moving objects to the buckets...");
		freezeDefault();
		this.bucketCache.clearAll();
		// test the mask thing.
		logger.info("Loading masks...");
		loadMasks();
		logger.info("Calculating estimators...");
		calculateEstimators();
		logger.info("Index stats...");
		bucketStats();
		sketchStats();

	}
	
	public void debugDist() throws IllegalIdException, OBStorageException, IllegalAccessException, InstantiationException, OBException, IOException{
		int i = 0;
		Random r = new Random();
		FileWriter f = new FileWriter("my" + this.getPivotCount() + ".csv");
		StaticBin1D[] stats = new StaticBin1D[m + 1];
		while(i < stats.length){
			stats[i] = new StaticBin1D();
			i++;
		}
		i = 0;
		while(i < databaseSize()){
			O a = getObject(r.nextInt((int)databaseSize()));
			O b = getObject(r.nextInt((int)databaseSize()));
			SketchProjection sa = getProjection(getBucket(a));
			SketchProjection sb = getProjection(getBucket(b));
			double d = distance(a,b);
			int s = sa.distance(sb.getSketch()).getDistance();
			stats[s].add(d);
			i++;
		}
		i = 0;
		f.write("#ds distance	avg	error	max	dmin\n");
		for(StaticBin1D s : stats){
			if(s.size() > 0){
				f.write(i + "\t" + s.mean() + "\t" + + s.standardDeviation() + "\t"  + s.min() + "\t" + s.max() + "\n");
			}
			i++;
		}
		f.close();
	}

	private void sketchStats() throws OBStorageException {
		if (HEIGHT == 2) {
			StaticBin1D s = new StaticBin1D();
			int i = 0;
			while (i < distortionStats.length) {
				s.add(Math.abs(this.distortionStats[i][0]
						- this.distortionStats[i][1]));
				i++;
			}
			logger.info("Distortion:");
			logger.info(s.toString());
			logger.info("Distortion:" + s.mean() / databaseSize());
		}
	}

	

	

}
