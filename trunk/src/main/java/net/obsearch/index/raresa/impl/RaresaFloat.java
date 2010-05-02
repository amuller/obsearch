package net.obsearch.index.raresa.impl;

import hep.aida.bin.StaticBin1D;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
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
import net.obsearch.index.raresa.AbstractRaresa;
import net.obsearch.ob.OBFloat;
import net.obsearch.query.OBQueryFloat;
import net.obsearch.result.OBPriorityQueueFloat;
import net.obsearch.stats.Statistics;
import net.obsearch.storage.OBStoreFactory;

public class RaresaFloat<O extends OBFloat> extends AbstractRaresa<O> implements
		IndexFloat<O> {

	private Computation[][] matrix;
	private Random r = new Random();

	private StaticBin1D[] rar;

	protected static Logger logger = Logger.getLogger(RaresaFloat.class
			.getName());

	public RaresaFloat(Class<O> type, int expectedSize) {
		super(type, expectedSize);

	}

	@Override
	public void prepare() throws OBException {
		matrix = (Computation[][]) Array.newInstance(Computation.class,
				new int[] { super.objects.size(), super.objects.size() });

		List<HashMap<Float, Computation>> comps = new ArrayList<HashMap<Float, Computation>>(
				objects.size());
		int i = 0;
		while (i < objects.size()) {
			comps.add(new HashMap<Float, Computation>());
			i++;
		}
		logger.info("Before matrix");
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
		logger.info("Matrix complete");
		// now we need to load raresa's data structure.

		// here we calculate the raresa estimators
		// for each object
		// for each pivot that estimates the object
		// for each object with a common pivot
		rar = new StaticBin1D[matrix.length];
		i = 0;
		while (i < rar.length) {
			rar[i] = new StaticBin1D();
			if(i % 100 == 0){
				logger.info("Doing rarezas" + i);
			}
			int cx = 0;
			while (cx < matrix.length) {
				if (i == cx) {
					cx++;
					continue;
				}
				int ax = 0;
				while(ax < matrix.length){
					if(i == ax || cx == ax){
						ax++;
						continue;
					}
					// distance of i to pivot cx
					Computation pa = matrix[i][cx];
					// distance of ax to pivot cx
					Computation pb = matrix[ax][cx];
					// now we can get a lower and upper bound
					float lower = Math.abs(pa.distance - pb.distance);
					float upper = pa.distance + pb.distance;
					// now we can find the % inside this interval where our result is.
					float real = matrix[i][ax].distance;
					// position within the prediction
					float position = (real - lower) / (upper - lower);
					rar[i].add(position);
					ax++;
				}
				cx++;
			}
			i++;
		}

		for(StaticBin1D s : rar){
			logger.info("Mean: " + s.mean() + " std: " + s.standardDeviation());
		}
		
		// matrix[i1][i2] = new Computation(o1.distance(o2), objects
		// .size());
		// now we have to sort each element of the matrix.
		for (Computation[] c : matrix) {
			Arrays.sort(c);
		}
	}

	private float distance(OBQueryFloat<O> query, int id, BitSet active,
			BitSet computed) throws OBException, InstantiationException,
			IllegalAccessException {
		assert !computed.get(id) : "Tried to evaluate id: " + id;
		stats.incDistanceCount();
		O b = get(id);
		computed.set(id);
		float d = query.getObject().distance(b);
		query.add(id, b, d);
		active.set(id, false);
		return d;
	}

	/**
	 * This method returns a list of all the distances of the query against the
	 * DB. This helps to calculate EP values in a cheaper way. results that are
	 * equal to the original object are added as Float.MAX_VALUE
	 * 
	 * @param query
	 * @param filterSame
	 *            if True we do not return objects o such that query.equals(o)
	 * @return
	 * @throws OBException
	 * @throws InstantiationException
	 * @throws IllegalAccessException
	 */
	public float[] fullMatchLite(O query, boolean filterSame)
			throws OBException, IllegalAccessException, InstantiationException {
		return CommonsFloat.fullMatchLite((OBFloat) query, filterSame, this);
	}

	public void searchOB(OBQueryFloat<O> query) throws OBException,
			InstantiationException, IllegalAccessException {
		List<OrderedSearch> results = new ArrayList<OrderedSearch>(matrix.length);
		int i = 0;
		while(i < matrix.length){
			results.add(new OrderedSearch(i));
			i++;
		}
		float stdDev = 1;
		BitSet computed = new BitSet(objects.size());
		// list of computed objects
		BitSet active = new BitSet();
		active.set(0, objects.size() - 1);
		int currentPivot = selectRandom(computed);
		boolean overlaps = true;
		while(overlaps){
			float dist = this.distance(query, currentPivot, active, computed);
			for(OrderedSearch s : results){
				int index = s.getIndex();
				Computation c = matrix[index][currentPivot];				
				s.update(dist, c.distance, rar[index] , stdDev);
			}
			Collections.sort(results);
			int cx = 1;
			overlaps = false;
			for(OrderedSearch s : results){
				if(cx < results.size() && s.isOverlaping(results.get(cx))){
					overlaps = true;
				}
				cx++;
			}
			currentPivot = selectRandom(computed);
		}
		i = 0;
		while(i < query.getResult().getK()){
			this.distance(query, results.get(i).getIndex(), active, computed);
			i++;
		}
	}
	
	private class OrderedSearch  implements Comparable<OrderedSearch>{
		private double lower = 0;
		private double upper = Double.MAX_VALUE;
		private int index;
		
		public OrderedSearch(int index) {
			super();
			this.index = index;
		}
		public double getLower() {
			return lower;
		}
		public void setLower(double lo) {
			if(lo > this.lower){
				this.lower = lo;
			}
		}
		
		public void update(float pa, float pb, StaticBin1D k, float stdev){
			float lo = Math.abs(pa - pb);
			float up = pa + pb;
			double lowerPercent = Math.max(k.mean() - (k.standardDeviation() * stdev), 0);
			double upperPercent = k.mean() + (k.standardDeviation() * stdev);
			float width = up - lo;
			setLower((width * lowerPercent) + lo);
			setUpper((width * upperPercent) + lo);
		}
		public double getUpper() {
			return upper;
		}
		public void setUpper(double up) {
			if(up < this.upper){
				this.upper = up;
			}
		}
		public int getIndex() {
			return index;
		}
		public void setIndex(int index) {
			this.index = index;
		}
		
		public int compareTo(OrderedSearch s ){
			if(upper < s.lower){
				return -1;
			}else if(lower > s.upper){
				return 1;
			}else{
				return 0;
			}
		}
		
		public boolean isOverlaping(OrderedSearch s){
			return compareTo(s) == 0;
		}
	}

	private int findCenter(int currentPivot, int index, BitSet computed,
			float min, float max) {
		int iRight = index;
		int iLeft = index - 1;
		int pivotToFind = -1;
		int l240 = -1;
		while (pivotToFind == -1
				&& (iRight < matrix[currentPivot].length || iLeft >= 0)) {
			if (iRight < matrix[currentPivot].length) {
				Computation current = getComp(currentPivot, iRight);
				if (current.distance <= max) {
					pivotToFind = current.selectPivot(computed);
					l240 = iRight - index;
				}
				iRight++;
			}
			if (pivotToFind == -1 && iLeft >= 0) {
				Computation current = getComp(currentPivot, iLeft);
				if (current.distance >= min) {
					pivotToFind = current.selectPivot(computed);
					l240 = index - iLeft;
				}

				iLeft--;
			}
		}
		stats.addExtraStats("L240", l240);
		// if no pivot was found we are done!
		return pivotToFind;
	}

	/*
	 * private int findOuter(int currentPivot, int index, BitSet computed, float
	 * min, float max){
	 * 
	 * int pivotToFind = -1; boolean direction = r.nextBoolean(); // true =
	 * right, false = left float distance; int piv; int iRight = index;
	 * Computation current = getComp(currentPivot, iRight);
	 * while(current.distance <= max && iRight < matrix[currentPivot].length){
	 * int newPiv = current.selectPivot(computed); if(newPiv != -1){ pivotToFind
	 * = newPiv; distance = } current = getComp(currentPivot, iRight); }
	 * 
	 * if(! direction || pivotToFind == -1){ int iLeft = index; Computation
	 * current = getComp(currentPivot, iLeft); while(current.distance >= min &&
	 * iLeft> 0 ){ int newPiv = current.selectPivot(computed); if(newPiv != -1){
	 * pivotToFind = newPiv; } iLeft--; } }
	 * 
	 * 
	 * 
	 * return pivotToFind; }
	 */

	private void populateActiveObjects(Computation c, BitSet active) {

		active.set(c.getObject());
	}

	private Computation getComp(int pivot, int index) {
		return matrix[pivot][index];
	}

	/**
	 * Randomly Select an object that has not been selected UPDATES the computed
	 * bitSet
	 * 
	 * @param computed
	 * @return
	 */
	private int selectRandom(BitSet computed) {
		return selectRandom(computed, size());
	}

	private int selectRandom(BitSet computed, int size) {
		int ran = -1;
		while (ran < 0 || computed.get(ran)) {
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

		public int getObject() {
			return element;
		}

		public int hashCode() {
			return new Float(distance).hashCode();
		}

		public boolean equals(Object o) {
			Computation c = (Computation) o;
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
		 * 
		 * @return
		 */
		public int selectPivot(BitSet computed) {
			if (!computed.get(element)) {
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
