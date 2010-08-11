package net.obsearch.pivots.rf03;

import hep.aida.bin.StaticBin1D;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
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
public abstract class AbstractIncrementalRF03<O extends OB> extends
		AbstractIncrementalPivotSelector<O> implements
		IncrementalPairPivotSelector<O> {

	private static final transient Logger logger = Logger
			.getLogger(AbstractIncrementalRF03.class.getName());

	/**
	 * How many times we are going to repeat the search
	 */
	private int repetitions = 4000;
	private int dataSample = 4000;

	/**
	 * At least get the given spread.
	 */
	private double desiredSpread = 0.5;

	/**
	 * At least get pivots with the given distortion.
	 */
	private double desiredDistortion = 0.10;

	protected AbstractIncrementalRF03(Pivotable<O> pivotable) {
		super(pivotable);
	}

	@Override
	public PivotPairResult<O> generatePivotsPair(int pairCount, Index<O> index)
			throws OBException, IllegalAccessException, InstantiationException,
			OBStorageException, PivotsUnavailableException {
		return generatePivotsPair(pairCount, null, index);
	}

	public int getRepetitions() {
		return repetitions;
	}

	public void setRepetitions(int repetitions) {
		this.repetitions = repetitions;
	}

	public int getDataSample() {
		return dataSample;
	}

	public void setDataSample(int dataSample) {
		this.dataSample = dataSample;
	}

	public double getDesiredSpread() {
		return desiredSpread;
	}

	public void setDesiredSpread(double desiredSpread) {
		this.desiredSpread = desiredSpread;
	}

	public double getDesiredDistortion() {
		return desiredDistortion;
	}

	public void setDesiredDistortion(double desiredDistortion) {
		this.desiredDistortion = desiredDistortion;
	}

	@Override
	public PivotPairResult<O> generatePivotsPair(int pairCount,
			LongArrayList elements, Index<O> index) throws OBException,
			IllegalAccessException, InstantiationException, OBStorageException,
			PivotsUnavailableException {

		O[] data = super.selectO(dataSample, r, elements, index, null);
		// random queries.
		// O[] queries = super.selectO(querySample, r, elements, index, null);
		// double[][] matrix = calculateDistances(queries, data);

		List<BitSet> sketches = initSketches(pairCount, dataSample);

		ArrayList<Pair<O, O>> pairs = new ArrayList<Pair<O, O>>(pairCount);
		int cx = 0;

		while (pairs.size() != pairCount) {
			int i = 0;
			Pair<O, O> p = getPair(elements, index);
			pairs.add(p);
			Score bestScore = calculateScore(data, pairs, sketches);
			Pair<O, O> bestPair = p;
			while (i < repetitions || !bestScore.isValid()) {
				Pair<O, O> newP = getPair(elements, index);
				pairs.set(cx, newP);
				Score newScore = calculateScore(data, pairs, sketches);
				if (newScore.isValid() && newScore.isBetter(bestScore)) {
					bestScore = newScore;
					bestPair = newP;
				}
				i++;
			}
			logger.info("Processed dimension i: " + cx + " iterations: " + i);
			logger.info(bestScore.toString());
			// take the best and calculate the score again.
			pairs.set(cx, bestPair);
			calculateScore(data, pairs, sketches);
			cx++;
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
	private Score calculateScore(O[] data, ArrayList<Pair<O, O>> pairs,
			List<BitSet> sketches) throws OBException {
		// take the last pivot of the list and update sketches and
		// maxDistanceToHyperplane
		OBAsserts.chkAssert(data.length == sketches.size(), "OOPS!");

		Pair<O, O> last = lastPivot(pairs);
		int currentIndex = pairs.size() - 1;
		int i = 0;
		MultiSet<BitSet> m = new MultiSet<BitSet>(data.length);
		double spread;
		double distortion;
		StaticBin1D distanceToHyperplane = new StaticBin1D();
		int partitionA = 0;
		int partitionB = 0;
		while (i < data.length) {
			BitSet b = sketches.get(i);
			O obj = data[i];
			// calculate distances
			double distanceTrue = distance(last.getA(), obj);
			double distanceFalse = distance(last.getB(), obj);
			double distanceToH = Math.max(Math
					.abs(distanceTrue - distanceFalse) / 2, 0);
			distanceToHyperplane.add(distanceToH);
			boolean classification = distanceTrue <= distanceFalse;
			if (classification) {
				partitionA++;
			} else {
				partitionB++;
			}
			b.set(currentIndex, classification);
			// keep a list of the unique buckets.
			m.add(b);
			i++;
		}
		StaticBin1D bucketStats = m.getDistribution();

		spread = bucketStats.size()
				/ Math.min(Math.pow(2, pairs.size()), data.length);
		distortion = ((double)Math.abs(partitionA - partitionB)) / (double)data.length;

		// now we have to calculate the # of buckets read to answer the query
		// sample
		/*
		 * StaticBin1D readBucketStats = new StaticBin1D(); i = 0; while(i <
		 * queries.length){ O q = queries[i]; double[] distance = distances[i];
		 * 
		 * }
		 */

		return new Score(bucketStats, distanceToHyperplane, distortion, spread);
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

		/**
		 * How well distributed the objects are in each bucket. Ideally
		 * everybody is well distributed.
		 */
		private StaticBin1D bucketDistribution;
		/**
		 * From all the posible buckets, how many of them are filled by the
		 * sketch.
		 */
		private double spread;
		/**
		 * How even the partitions are, we want even partitions.
		 */
		private double distortion;
		/**
		 * Distance to the hyperplane, the more the better.
		 */
		private StaticBin1D distanceToH;

		public String toString() {
			return "Spread: " + spread + " distort: " + distortion + " dH: "
					+ printBin(distanceToH) + " buk: "
					+ printBin(bucketDistribution);
		}

		private String printBin(StaticBin1D d) {
			return "mean: " + d.mean() + " std: " + d.standardDeviation();
		}

		public Score(StaticBin1D bucketDistribution, StaticBin1D distanceToH,
				double distortion, double spread) {
			super();
			this.bucketDistribution = bucketDistribution;
			this.distanceToH = distanceToH;
			this.distortion = distortion;
			this.spread = spread;
		}

		/**
		 * Returns true if the score is within acceptable limits.
		 * 
		 * @return
		 */
		public boolean isValid() {
	//return spread
			//		>= desiredSpread && distortion <= desiredDistortion;
			return true;
		}

		private double score() {
				return (spread) + (0.5d * (1d - distortion));
		}

			/**
			 * Returns true if this is better than another.
			 */
		public boolean isBetter(Score another) {

				if(this.isValid() && ! another.isValid()){
					return true; 
				}
				if(distortion < another.distortion){
					return true;
				}else if(distortion == another.distortion){
					return this.distanceToH.mean() > another.distanceToH.mean();
				}
				return false;
				//return score() > another.score();
		}


		/**
		 * Lower part of the first quantile
		 * 
		 * @param a
		 * @return
		 */
		private double lower(StaticBin1D a) {
			// do not let things get smaller than 0
			// return Math.max(a.mean() - a.standardDeviation(), 0);
			return a.mean();
		}

		/**
		 * Higher part of the first quantile
		 * 
		 * @param a
		 * @return
		 */
		private double higher(StaticBin1D a) {
			return a.mean() + a.standardDeviation();
		}

		/**
		 * Compares a against b. By using the mean + std dev we hope to find out
		 * the distribution that has overall smaller values than the other.
		 * 
		 * @param a
		 * @param b
		 * @return -1 if a < b, 0 if a == b, 1 if a > b
		 */
		private int compareDistributions(StaticBin1D a, StaticBin1D b) {
			double topA = a.mean() + a.standardDeviation();
			double topB = a.mean() + a.standardDeviation();
			if (topA == topB) {
				return 0;
			} else if (topA < topB) {
				return -1;
			} else {
				return 1;
			}
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
