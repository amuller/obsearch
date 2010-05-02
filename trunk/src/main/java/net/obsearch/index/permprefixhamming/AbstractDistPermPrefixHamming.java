package net.obsearch.index.permprefixhamming;

import java.io.IOException;
import java.nio.ByteBuffer;
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

public abstract class AbstractDistPermPrefixHamming <O extends OB, B extends BucketObject<O>, Q, BC extends BucketContainer<O, B, Q>>extends
	AbstractBucketSorter<O, B, Q, BC, PermPrefixProjection, CompactPermPrefix> {

		static final transient Logger logger = Logger
		.getLogger(AbstractDistPermPrefixHamming.class.getName());
	
		private int prefixSize;
		
		public AbstractDistPermPrefixHamming(Class<O> type,
				IncrementalPivotSelector<O> pivotSelector, int pivotCount,
				int bucketPivotCount, int prefixSize) throws OBStorageException, OBException {
			super(type, pivotSelector, pivotCount, bucketPivotCount);
			OBAsserts.chkAssert(prefixSize <= pivotCount, "prefix size cannot exceed the number of pivots");
			this.prefixSize = prefixSize;
		}
		
		protected  byte[] compactRepresentationToBytes(CompactPermPrefix cp){
			return PermPrefixProjection.shortToBytes(cp.perm);
		}
		
		protected int getPrefixSize(){
			return prefixSize;
		}
		
		protected  CompactPermPrefix bytesToCompactRepresentation(byte[] data){
			ByteBuffer b = ByteConversion.createByteBuffer(data);
			int i = 0;
			short[] res = new short[getPrefixSize()];
			while(i < getPrefixSize()){
				res[i] = b.getShort();
				i++;
			}
			return new CompactPermPrefix(res);
		}

		protected byte[] convertLongToBytesAddress(long bucketId) {
			return ByteConversion.longToBytes(bucketId);
		}
		
		@Override
		protected int getCPSize() {
			return ByteConstants.Short.getSize() * getPrefixSize();
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
