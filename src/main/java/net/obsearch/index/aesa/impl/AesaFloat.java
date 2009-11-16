package net.obsearch.index.aesa.impl;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.logging.Logger;

import net.obsearch.OperationStatus;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.filter.Filter;
import net.obsearch.index.CommonsFloat;
import net.obsearch.index.IndexFloat;
import net.obsearch.index.aesa.AbstractAESA;
import net.obsearch.ob.OBFloat;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.result.OBPriorityQueueFloat;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.OBStoreFactory;

public class AesaFloat<O extends OBFloat> extends AbstractAESA<O> implements IndexFloat<O> {
	
	private Computation[][] matrix;
	private Random r = new Random();
	protected static Logger logger = Logger.getLogger(AesaFloat.class
			.getName());
	public AesaFloat(Class<O> type, int expectedSize) {
		super(type, expectedSize);

	}

	
	@Override
	public void prepare() throws OBException {
		matrix = (Computation[][]) Array.newInstance(Computation.class,
				new int[] { super.objects.size(), super.objects.size() });
		
		List<HashMap<Float,Computation>> comps = new ArrayList<HashMap<Float,Computation>>(objects.size());
		int i = 0;
		while(i < objects.size()){
			comps.add(new HashMap<Float,Computation>());
			i++;
		}
		
		
		int i1 = 0;
		while (i1 < objects.size()) {
			int i2 = i1;
			while (i2 < objects.size()) {
				float distance = objects.get(i1).distance(objects.get(i2));
				Computation c1 = new Computation(distance, i1);
				Computation c2 = new Computation(distance, i2);
				matrix[i1][i2] = c2;
				matrix[i2][i1] = c1;
				i2++;
			}
			i1++;
		}
		
		
		//matrix[i1][i2] = new Computation(o1.distance(o2), objects
		//		.size());		
		// now we have to sort each element of the matrix.
		for (Computation[] c : matrix) {
			Arrays.sort(c);
		}
	}
	
	private float distance(OBQueryFloat<O> query, int id, BitSet active, BitSet computed) throws OBException, InstantiationException, IllegalAccessException{
		assert ! computed.get(id) : "Tried to evaluate id: " + id;
		stats.incDistanceCount();
		O b = get(id);
		computed.set(id);
		float d = query.getObject().distance(b);
		query.add(id,b, d);
		active.set(id, false);
		return d;
	}
	
	/**
	 * This method returns a list of all the distances of the query against  the DB.
	 * This helps to calculate EP values in a cheaper way. results that are equal to the original object are added
	 * as Float.MAX_VALUE
	 * @param query
	 * @param filterSame if True we do not return objects o such that query.equals(o)
	 * @return
	 * @throws OBException 
	 * @throws InstantiationException 
	 * @throws IllegalAccessException 
	 */
	public float[] fullMatchLite(O query, boolean filterSame) throws OBException, IllegalAccessException, InstantiationException{
			return CommonsFloat.fullMatchLite((OBFloat)query, filterSame, this);
	}

	public void searchOB(OBQueryFloat<O> query) throws OBException, InstantiationException, IllegalAccessException {
		// we could choose the guy with the greatest STD.
		BitSet computed = new BitSet(objects.size());
		int currentPivot = selectRandom(computed);
		
		// list of computed objects
		BitSet active = new BitSet();
		active.set(0, objects.size() - 1);
		float dist = distance(query, currentPivot, active, computed);
		logger.info("First dist: " + dist);
		int count = 1;
		while(active.cardinality() > 0){
			// find the closest pivot 
			int index = Math.abs(Arrays.binarySearch(matrix[currentPivot], new Computation(dist, 1)));
			//logger.info("Found index: " + index + " curr: " + currentPivot);
			if(index >= matrix[currentPivot].length){
				index = matrix[currentPivot].length - 1;
			}
			
			// based on the current index do:
			
			// 1) based on the current range, update the list of active objects.
			float min;
			float max;
			if(! query.isFull()){
				// query not full. min and max set to inf.
				max = Float.MAX_VALUE;
				min = -1;
			} else{
				max = dist + query.peek();
				min = dist - query.peek();
			}
			
			// now we can populate the active guys.
			int iRight = index;
			int iLeft = index - 1;
			BitSet activeTemp = new BitSet(size());
			while(iRight < matrix[currentPivot].length && matrix[currentPivot][iRight].distance <= max){
				//logger.info("current: " + currentPivot + " right: "  + iRight);
				populateActiveObjects(matrix[currentPivot][iRight], activeTemp);
				iRight++;
			}
			assert iLeft <  matrix[currentPivot].length : " index: "+ index;
			while(iLeft >= 0 && matrix[currentPivot][iLeft].distance >= min){
				populateActiveObjects(matrix[currentPivot][iLeft], activeTemp);
				iLeft--;
			}
			activeTemp.andNot(computed);
			// update the active list
			active.and(activeTemp);
			
			
			// *****************************************************************************
			// 2) find a new pivot		
		//	if(count % 2 == 0){
				currentPivot = findCenter(currentPivot, index, computed, min, max);
		//	}else{
		//		currentPivot = findOuter(currentPivot, index, computed, min, max);
		//	}3
			if(currentPivot != -1){
			dist = distance(query,  currentPivot, active, computed);
			//logger.ivnfo("selected pivot: " + currentPivot + " active: "  + active.cardinality() + " computed: " + computed.cardinality() + " dist: " + query.getDistance());
			}else{
				assert active.cardinality() == 0;
			}
			count++;
			if(active.cardinality() < count){
				// just match the remaining objects!
				 for (int i = active.nextSetBit(0); i >= 0; i = active.nextSetBit(i+1)) {
				    distance(query, i, active, computed);
				 }
				 break;
			}
		
			// *****************************************************************************
		}
	}
	
	private int findCenter(int currentPivot, int index, BitSet computed, float min, float max){
		int iRight = index;
		int iLeft = index - 1;
		int pivotToFind = -1;
		int l240 = -1;
		while(pivotToFind == -1 && (iRight < matrix[currentPivot].length || iLeft >= 0)){
			if(iRight < matrix[currentPivot].length){
				Computation current = getComp(currentPivot, iRight);
				if(current.distance <= max){
				pivotToFind = current.selectPivot(computed);
				l240 = iRight - index;
				}
				iRight++;
			}
			if(pivotToFind == -1 && iLeft >= 0){
				Computation current = getComp(currentPivot, iLeft);
				if(current.distance >= min){
					pivotToFind = current.selectPivot(computed);
					l240 =  index - iLeft;
				}
				
				iLeft--;
			}
		}
		stats.addExtraStats("L240", l240);
		// if no pivot was found we are done!
		return pivotToFind;
	}
	
	
	/*private int findOuter(int currentPivot, int index, BitSet computed, float min, float max){
		
		int pivotToFind = -1;
		boolean direction = r.nextBoolean(); // true = right, false = left
		float distance;
		int piv;
		int iRight = index;
		Computation current = getComp(currentPivot, iRight);
		while(current.distance <= max && iRight < matrix[currentPivot].length){
			int newPiv = current.selectPivot(computed);
			if(newPiv != -1){
				pivotToFind = newPiv;
				distance = 
			}
			current = getComp(currentPivot, iRight);
		}
		
		if(! direction || pivotToFind == -1){
			int iLeft = index;
			Computation current = getComp(currentPivot, iLeft);
			while(current.distance >= min && iLeft> 0 ){
				int newPiv = current.selectPivot(computed);
				if(newPiv != -1){
					pivotToFind = newPiv;
				}
				iLeft--;
			}
		}
		

		
		return pivotToFind;
	}*/
	
	private void populateActiveObjects(Computation c, BitSet active){
		
		active.set(c.getObject());
	}
	
	private Computation getComp(int pivot, int index){
		return matrix[pivot][index];
	}
	
	/**
	 * Randomly Select an object that has not been selected
	 * UPDATES the computed bitSet
	 * @param computed
	 * @return
	 */
	private int selectRandom(BitSet computed){
		return selectRandom(computed, size());
	}
	
	private int selectRandom(BitSet computed, int size){
		int ran = -1;
		while(ran < 0 || computed.get(ran)){
			ran = r.nextInt(size);		
		}
		return ran;
	}
			
	
	protected class Computation implements Comparable<Computation> {

		private float distance;
		private int element;

		/**
		 * Creates a new computation
		 * 
		 * @param distance
		 *            distance element
		 * @param size
		 *            max number of pivots in this index.
		 */
		public Computation(float distance, int element) {
			this.distance = distance;
			this.element = element;
		}

		
		public int getObject(){
			return element;
		}
		public int hashCode(){
			return new Float(distance).hashCode();
		}
		
		public boolean equals(Object o){
			Computation c = (Computation)o;
			return distance == c.distance;
		}

		@Override
		public int compareTo(Computation o) {
			if (distance < o.distance) {
				return -1;
			} else if (distance > o.distance) {
				return 1;
			} else {
				return 0;
			}
		}
		
		/**
		 * Randomly selects a pivot from this set.
		 * @return
		 */
		public int selectPivot(BitSet computed){
			if(! computed.get(element)){
				return element;
			}
		    return -1;
		
		}

	}



	@Override
	public void searchOB(O object, float r, OBPriorityQueueFloat<O> result)
			throws NotFrozenException, InstantiationException,
			IllegalIdException, IllegalAccessException, OutOfRangeException,
			OBException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void searchOB(O object, float r, Filter<O> filter,
			OBPriorityQueueFloat<O> result) throws NotFrozenException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void close() throws OBException {
		// TODO Auto-generated method stub
		
	}

	

	@Override
	public String debug(O object) throws OBException, InstantiationException,
			IllegalAccessException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationStatus delete(O object) throws OBStorageException,
			OBException, IllegalAccessException, InstantiationException,
			NotFrozenException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationStatus exists(O object) throws OBStorageException,
			OBException, IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long getBox(O object) throws OBException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public O getObject(long i) throws IllegalIdException,
			IllegalAccessException, InstantiationException, OBException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Statistics getStats() throws OBStorageException {
		// TODO Auto-generated method stub
		return null;
	}

	

	@Override
	public void init(OBStoreFactory fact) throws OBStorageException,
			NotFrozenException, IllegalAccessException, InstantiationException,
			OBException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public OperationStatus insert(O object, long id) throws OBStorageException,
			OBException, IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationStatus insertBulk(O object) throws OBStorageException,
			OBException, IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public OperationStatus insertBulk(O object, long id)
			throws OBStorageException, OBException, IllegalAccessException,
			InstantiationException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isFrozen() {
		// TODO Auto-generated method stub
		return false;
	}

	

	@Override
	public void setPreFreezeCheck(boolean preFreezeCheck) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public long totalBoxes() {
		// TODO Auto-generated method stub
		return 0;
	}

}
