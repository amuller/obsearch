package net.obsearch.pivots.rf04;

import hep.aida.bin.StaticBin1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import cern.colt.list.LongArrayList;
import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.pivots.AbstractIncrementalPivotSelector;
import net.obsearch.pivots.IncrementalPairPivotSelector;
import net.obsearch.pivots.IncrementalPivotSelector;
import net.obsearch.pivots.PivotPairResult;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;
import net.obsearch.utils.MultiSet;
import net.obsearch.utils.Pair;

/**
 * Select pairs of pivots.
 * 
 * @author amuller
 * 
 * @param <O>
 * @param <N>
 */
public abstract class AbstractIncrementalRF04<O extends OB> extends
		AbstractIncrementalPivotSelector<O> implements
		IncrementalPairPivotSelector<O> {

	private static final transient Logger logger = Logger
			.getLogger(AbstractIncrementalRF04.class.getName());

	
	private int dataSample = 500;

	private double max_distance = 0;
	
	public AbstractIncrementalRF04(){
		
	}

	protected AbstractIncrementalRF04(Pivotable<O> pivotable) {
		super(pivotable);
	}

	@Override
	public PivotPairResult<O> generatePivotsPair(int pairCount, Index<O> index)
			throws OBException, IllegalAccessException, InstantiationException,
			OBStorageException, PivotsUnavailableException {
		return generatePivotsPair(pairCount, null, index);
	}

	

	public int getDataSample() {
		return dataSample;
	}

	public void setDataSample(int dataSample) {
		this.dataSample = dataSample;
	}

	
	
	/**
	 * Populate all the matrix, inefficient but simple
	 * @param data
	 * @param theMatrix
	 * @throws OBException
	 */
	private void populateMatrix(List<O> data, double[][] theMatrix) throws OBException{
		int x = 0;
		int y = 0;
		assert data.size() == theMatrix.length;
		logger.info("Populating matrix...");
		while(x < theMatrix.length){
			y = x;
			if(x % 100 == 0){
				logger.info("Doing: " + x);
			}
			while(y < theMatrix[x].length){
				if(x == y){
					theMatrix[x][y] = 0;
				}else{
					double dist = this.distance(data.get(x), data.get(y));
					max_distance = Math.max(dist, max_distance);
					theMatrix[x][y]  = dist;
					theMatrix[y][x] = dist;
				}
				y++;
			}
			x++;
		}
	}
	private double normalizeDistance(double d){
		return d / max_distance;
	}
	
	// fill the sketch set with the given list of pivots
	private void calculateSketches(List<Pair<O, O>> pivots, List<O> data, List<BitSet> sketches) throws OBException{
		
		int i = 0;
		for(Pair<O, O> p : pivots){
			int di = 0;
			for(O d : data){
				calculateSketch(p, d, i, sketches.get(di));
				di++;
			}
			i++;
		}
	}
	
	/**
	 * calculate a sketch for the given object at dimension i
	 * @param p
	 * @param object
	 * @param i
	 * @return
	 * @throws OBException 
	 */
	private void calculateSketch(Pair<O, O> p, O object, int i, BitSet currentSketch) throws OBException{
		double distA = distance(p.getA(), object);
		double distB = distance(p.getB(), object);
		calculateSketch(distA, distB, i, currentSketch);
	}
	
	private void calculateSketch(double distA, double distB, int i , BitSet currentSketch){
		if(distA > distB){
			currentSketch.set(i);
		}
	}

	@Override
	public PivotPairResult<O> generatePivotsPair(int pairCount,
			LongArrayList elements, Index<O> index) throws OBException,
			IllegalAccessException, InstantiationException, OBStorageException,
			PivotsUnavailableException {
		ArrayList<Pair<O, O>> pairs = new ArrayList<Pair<O, O>>(pairCount);
		int cx = 0;
		LongArrayList excludes = new LongArrayList(pairCount * 2);
		int i = 0;
		while (pairs.size() != pairCount) {
			
			long[] dataIds = super.selectUnique(dataSample, r, elements, index, excludes);
			List<O> data = new ArrayList<O>(dataIds.length);
			for(long id : dataIds){
				data.add(index.getObject(id));
			}
			
			// calculate the matrix
			double[][] theMatrix = new double[dataSample][dataSample];
			// calculate the distances of all the objects.
			populateMatrix(data, theMatrix);
			// pair of pivots
			int x = 0;
			Score best = null;
			// calculate the sketches for the current dataset.			
			List<BitSet> sketches = initSketches(pairCount, dataSample);
			calculateSketches(pairs,data, sketches);
			while(x < theMatrix.length){
				int y = x + 1;
				if(x % 100 == 0){
					logger.info("Doing: " + x);
				}
				while(y < theMatrix.length){
					assert x != y;
					assert i == pairs.size() : " size of i:" + i + " pairs size: " + pairs.size();
					Score newScore = calculateScore(x, y, theMatrix, sketches, i);
					if(best == null || newScore.isBetter(best)){
						best = newScore;
					}
					y++;
				}
				
				x++;
			}
			logger.info("Found pivot: " + i + " best: "+ best + " id1: " + dataIds[best.getX()] + " id2: " + dataIds[best.getY()]);
			
			assert best.getX() != best.getY();
			pairs.add(new Pair<O,O>(data.get(best.getX()), data.get(best.getY())));
			excludes.add(dataIds[best.getX()]);
			excludes.add(dataIds[best.getY()]);
			i++;
		}

		return new PivotPairResult<O>(pairs);
	}

	/**
	 * Calculate the distances to each of the data queries.
	 * 
	 * @param queries
	 * @param data
	 * @return
	 * @throws OBException
	 */
	private double[][] calculateDistances(O[] queries, O[] data)
			throws OBException {
		double[][] res = new double[queries.length][];
		int i = 0;
		while (i < queries.length) {
			res[i] = sequentialSearch(queries[i], data);
			i++;
		}
		return res;
	}

	private List<Double> initDoubles(int dataSample) {
		List<Double> maxDistanceToHyperplane = new ArrayList<Double>(dataSample);
		int i = 0;
		while (i < dataSample) {
			maxDistanceToHyperplane.add(Double.MIN_VALUE);
			i++;
		}
		return maxDistanceToHyperplane;
	}

	private List<BitSet> initSketches(int pairCount, int dataSample) {
		List<BitSet> sketches = new ArrayList<BitSet>(dataSample);
		int i = 0;
		while (i < dataSample) {
			sketches.add(new BitSet(pairCount));
			i++;
		}
		return sketches;
	}

	/**
	 * 
	 * @param data
	 * @param pairs
	 * @param sketches
	 * @param maxDistanceToHyperplane
	 * @throws OBException
	 */
	private Score calculateScore(int x, int y, double[][] matrix, List<BitSet> sketches, int sketchI) throws OBException {
//		logger.info("Calculating score...");
		assert matrix[x].length == matrix[y].length;
		assert x != y;
		int i = 0;
		int cx = 0;
		int cy = 0;
		while(i < matrix[x].length){
			if(matrix[x][i] <= matrix[y][i]){
				cx++;
			}else{
				cy++;
			}
			i++;
		}
		// update sketches
		i = 0;
		assert matrix[x].length == sketches.size();
		HashSet<BitSet> count = new HashSet<BitSet>(sketches.size());
		while(i < matrix[x].length){
			calculateSketch(matrix[x][i], matrix[y][i],  sketchI, sketches.get(i));
			count.add(sketches.get(i));
			i++;
		}
		double spread  = (double)count.size()/ Math.min(Math.pow(2, (double)(sketchI + 1)), (double)sketches.size());
		
		return new Score(this.normalizeDistance(matrix[x][y]), (double)Math.abs(cx - cy) / (double)sketches.size(),  x, y ,spread);
	}

	private double[] sequentialSearch(O query, O[] data) throws OBException {
		double[] res = new double[data.length];
		int i = 0;
		while (i < data.length) {
			res[i] = distance(query, data[i]);
			i++;
		}
		Arrays.sort(res);
		return res;
	}

	private Pair<O, O> lastPivot(ArrayList<Pair<O, O>> list) {
		return list.get(list.size() - 1);
	}

	/**
	 * a good pivot selection strategy should follow certain rules, but in
	 * general those pivot selection strategies that perform well (find the k-nn
	 * in the least number of iterations). When both pivot selection strategies
	 * perform equally well, then we can use other properties to select a good
	 * pivot strategy.
	 * 
	 */
	protected class Score {


		private double distortion;
		/**
		 * Distance
		 */
		private double distance;
		
		private int x;
		private int y;
		
		private double spread;

		public String toString() {
			return  " Distort: " + distortion + " piv dis: "
					+ distance + " Spread: " + spread;		
		}

		private String printBin(StaticBin1D d) {
			return "mean: " + d.mean() + " std: " + d.standardDeviation();
		}

		public Score(double distance,
				double distortion, int x, int y, double spread) {
			super();
			assert x != y;
			this.distortion = distortion;
			this.distance = distance;
			this.x = x;
			this.y = y;
			this.spread = spread;
		}

		public int getX() {
			return x;
		}

		public void setX(int x) {
			this.x = x;
		}

		public int getY() {
			return y;
		}

		public void setY(int y) {
			this.y = y;
		}

		private double val(){
			//return (1 - distortion) + (10 * spread);
			// this gave really good results
			// return (- distance) + (10 * spread); L456
			return (1 - distance) + (10 * spread); // This is RF04
			//return (1 - distortion) + (5 * (1 - distance)) + (10 * spread); //RF04.1 This is RF04 with distortion included
			//return spread;
		}

			/**
			 * Returns true if this is better than another.
			 */
		public boolean isBetter(Score another) {
				
				/*if(distortion == another.distortion){
					return distance > another.distance;
				}else{
					return distortion < another.distortion;
				}*/
			return val() >= another.val();
		}


	}
	
	protected abstract double distance(O a, O b) throws OBException;

	private Pair<O, O> getPair(LongArrayList elements, Index<O> index)
			throws IllegalIdException, IllegalAccessException,
			InstantiationException, OBException {
		O[] newPair = super.selectO(2, r, elements, index, null);
		return new Pair<O, O>(newPair[0], newPair[1]);
	}

}
