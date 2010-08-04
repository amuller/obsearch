package net.obsearch.index.perm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.logging.Logger;

import net.obsearch.OB;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.index.ghs.FixedPriorityQueue;
import net.obsearch.index.sorter.AbstractBucketSorter;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.utils.bytes.ByteConversion;

public abstract class AbstractDistPerm <O extends OB, B extends BucketObject<O>, Q, BC extends BucketContainer<O, B, Q>>extends
	AbstractBucketSorter<O, B, Q, BC, PermProjection, CompactPerm> {

		static final transient Logger logger = Logger
		.getLogger(AbstractDistPerm.class.getName());
	
		public AbstractDistPerm(Class<O> type,
				IncrementalPivotSelector<O> pivotSelector, int pivotCount,
				int bucketPivotCount) throws OBStorageException, OBException {
			super(type, pivotSelector, pivotCount, bucketPivotCount);
			
		}
		
		protected  byte[] compactRepresentationToBytes(CompactPerm cp){
			return PermProjection.shortToBytes(cp.perm);
		}
		
		protected  CompactPerm bytesToCompactRepresentation(byte[] data){
			ByteBuffer b = ByteConversion.createByteBuffer(data);
			int i = 0;
			short[] res = new short[getPivotCount()];
			while(i < super.getPivotCount()){
				res[i] = b.getShort();
				i++;
			}
			return new CompactPerm(res);
		}
		
		
		@Override
		protected void updateDistance(PermProjection query,
				CompactPerm proj, FixedPriorityQueue<PermProjection> queue) {
				int distance = query.sfrDistance(proj);
				if(! queue.isFull() ||  distance < queue.peek().getDistance() ){								
					queue.add(new PermProjection(proj, distance));
				}			
		}

		protected byte[] convertLongToBytesAddress(long bucketId) {
			return ByteConversion.longToBytes(bucketId);
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
