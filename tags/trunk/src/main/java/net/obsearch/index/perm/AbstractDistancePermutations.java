package net.obsearch.index.perm;
import java.io.IOException;

import net.obsearch.OB;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.AlreadyFrozenException;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.bucket.AbstractBucketIndex;
import net.obsearch.index.bucket.BucketContainer;
import net.obsearch.index.bucket.BucketObject;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.storage.CloseIterator;
import net.obsearch.storage.TupleBytes;


	
	public abstract class AbstractDistancePermutations<O extends OB, B extends BucketObject<O>, Q, BC extends BucketContainer<O, B, Q>>
	extends AbstractBucketIndex<O, B, Q, BC> {
		
		private transient byte[][] permlets;

		public AbstractDistancePermutations(Class<O> type,
				IncrementalPivotSelector<O> pivotSelector, int pivotCount)
				throws OBStorageException, OBException {
			super(type, pivotSelector, pivotCount);
			
		}
		
		
		public void freeze() throws AlreadyFrozenException, IllegalIdException, OBStorageException, OutOfRangeException, IllegalAccessException, InstantiationException, OBException, PivotsUnavailableException, IOException{
			super.freeze();
			freezeDefault();
		}
		
		protected void loadPermutations() throws OBStorageException, OBException{
			OBAsserts.chkAssert(Buckets.size() <= Integer.MAX_VALUE, "max bucket size exceeded");
			int total = (int)Buckets.size();
			permlets = new byte[total][getPivotCount()];
			CloseIterator<TupleBytes> it = Buckets.processAll();
			int i = 0;
			while(it.hasNext()){
				TupleBytes t = it.next();
				permlets[i] = t.getKey();
				i++;
			}
			it.closeCursor();
		}
		
}
