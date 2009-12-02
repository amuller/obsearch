package net.obsearch.index.ky0;

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import net.obsearch.OB;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.constants.ByteConstants;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.index.sorter.AbstractBucketSorter;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.utils.bytes.ByteConversion;

public abstract class AbstractKy0<O extends OB, B extends BucketObject<O>, Q, BC extends BucketContainer<O, B, Q>>
		extends AbstractBucketSorter<O, B, Q, BC, Ky0Projection, CompactKy0> {

	static final transient Logger logger = Logger.getLogger(AbstractKy0.class
			.getName());

	/**
	 * Height estimations
	 */
	protected StaticBin1D[] kHeight;

	/**
	 * Number of buckets read within a given kHeight
	 */
	protected StaticBin1D[] kBucketsWithinHeight;

	public AbstractKy0(Class<O> type,
			IncrementalPivotSelector<O> pivotSelector, int pivotCount,
			int bucketPivotCount) throws OBStorageException, OBException {
		super(type, pivotSelector, pivotCount, bucketPivotCount);
	}

	protected byte[] compactRepresentationToBytes(CompactKy0 cp) {
		return cp.getAddress();
	}
	
	protected String printEstimation(int i){
		StringBuilder b = new StringBuilder();
		b.append("Height: \n");
		b.append(kHeight[i]);
		b.append("\n Buckets Within \n");
		b.append(kBucketsWithinHeight[i].toString());
		return b.toString();
	}

	protected void maxKEstimation() throws IllegalIdException, OBException,
			IllegalAccessException, InstantiationException {

		kHeight = new StaticBin1D[getMaxK().length];
		kBucketsWithinHeight = new StaticBin1D[getMaxK().length];
		int i = 0;
		while (i < getMaxK().length) {
			kHeight[i] = new StaticBin1D();
			kBucketsWithinHeight[i] = new StaticBin1D();
			i++;
		}
		
		super.maxKEstimation();
	}

	protected CompactKy0 bytesToCompactRepresentation(byte[] data) {
		ByteBuffer b = ByteConversion.createByteBuffer(data);
		int i = 0;
		int[] d = new int[getPivotCount()];
		int height = b.getInt();
		while (i < getPivotCount()) {
			d[i] = b.getInt();
			i++;
		}
		return new CompactKy0(height, d);
	}

	protected byte[] convertLongToBytesAddress(long bucketId) {
		return ByteConversion.longToBytes(bucketId);
	}

	@Override
	protected int getCPSize() {
		return ByteConstants.Int.getSize() * (getPivotCount() + 1);
	}

	public void freeze() throws AlreadyFrozenException, IllegalIdException,
			OutOfRangeException, IllegalAccessException,
			InstantiationException, OBException, PivotsUnavailableException,
			IOException {
		super.freeze();
		freezeDefault();
		loadMasks();
		logger.info("Calculating estimators...");
		calculateEstimators();
		logger.info("Index stats...");
		bucketStats();

	}

}
