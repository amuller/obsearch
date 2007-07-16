package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

import hep.aida.bin.QuantileBin1D;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.ClusteringFailedException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.KMeansException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.pptree.SpaceTree;
import org.ajmm.obsearch.index.pptree.SpaceTreeLeaf;
import org.ajmm.obsearch.index.pptree.SpaceTreeNode;
import org.apache.log4j.Logger;



import cern.colt.bitvector.BitVector;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public abstract class AbstractPPTree<O extends OB> extends
		AbstractExtendedPyramidIndex<O> {

	private static final transient Logger logger = Logger
			.getLogger(AbstractPPTree.class);

	protected byte od;

	protected SpaceTree spaceTree;
	
	/**
	 * Hack to catch when k-meanspp is not able to generate centers that converge.
	 */
	protected static final int  kMeansPPIterations = 3;  

	/**
	 * AbstractPPTree Constructs a P+Tree
	 *
	 * @param databaseDirectory,attrs
	 *            the database directory
	 * @param pivots
	 *            how many pivots will be used
	 * @param od
	 *            parameter used to specify the number of divisions. 2 ^ od
	 *            divisions will be performed.
	 * @throws DatabaseException
	 */
	public AbstractPPTree(final File databaseDirectory, final short pivots,
			final byte od) throws DatabaseException, IOException {
		super(databaseDirectory, pivots);
		this.od = od;
	}

	// TODO: We could override freeze and remove the creation of database B
	// P+Tree needs B so maybe this is not so relevant.
	// recursive is prettier... this method should be recursive. But iterative
	// might
	// be the only way of completing this heavy task in most computers.
	@Override
	protected void calculateIndexParameters() throws DatabaseException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {

		long count = super.bDB.count();
		// each median for each dimension will be stored in this array
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();

		
		Random ran = new Random(System.currentTimeMillis());

			BitSet data = new BitSet(this.databaseSize());
			data.set(0, this.databaseSize());
			SpaceTreeNode node = new SpaceTreeNode(); // this will hold the
			// now we just have to create the space tree
			float[][] minMax = new float[pivotsCount][2];
			initMinMax(minMax);
			int[] SNo = new int[1]; // this is a pointer
			// divide the space
			spaceDivision(node, 0, minMax, data, SNo, ran, null);
			// we created all the spaces.
			assert SNo[0] == Math.pow(2, od);
			// now the space-tree has been built.
			// save the space tree
			this.spaceTree = node;
			if (logger.isDebugEnabled()) {
				logger.debug("Space tree: \n" + spaceTree);
			}
		
		logger.debug("Space Tree calculated");

	}

	/**
	 * Calculates the parameters for the leaf based on minMax, and the center
	 *
	 * @param x
	 * @param minMax
	 * @param center
	 */
	protected void calculateLeaf(SpaceTreeLeaf x, float[][] minMax,
			float[] center) {
		int i = 0;
		assert pivotsCount == minMax.length;
		double[] min = new double[pivotsCount];
		double[] width = new double[pivotsCount];
		double[] exp = new double[pivotsCount];
		while (i < pivotsCount) {
			assert minMax[i][MIN] < center[i] && center[i] < minMax[i][MAX] : "MIN: "
					+ minMax[i][MIN]
					+ " CENTER: "
					+ center[i]
					+ " MAX: "
					+ minMax[i][MAX];
			// divisors != 0
			assert (minMax[i][MAX] - minMax[i][MIN]) != 0;
			assert (Math.log(min[i] * center[i] - width[i]) / Math.log(2)) != 0;

			min[i] = minMax[i][MIN];
			// TODO: doing 1/width[i] might be cheaper as later only a
			// multiplication
			// has to be made... but the tests broke! so I guess we should play
			// safe.
			width[i] = (minMax[i][MAX] - minMax[i][MIN]);
			exp[i] = -(1 / (Math.log((center[i] - min[i]) / width[i]) / Math
					.log(2)));

			assert center[i] >= 0 && center[i] <= 1;
			assert minMax[i][MIN] >= 0 && minMax[i][MAX] <= 1;
			assert minMax[i][MIN] <= center[i] && center[i] <= minMax[i][MAX] : " Center: "
					+ center[i]
					+ " min: "
					+ minMax[i][MIN]
					+ " max: "
					+ minMax[i][MAX];
			i++;
		}
		x.setA(min);
		x.setB(width);
		x.setExp(exp);
		x.setMinMax(minMax);
		assert validateT(x, center);
	}

	/**
	 * validates that the properties of function T are preserved in x
	 *
	 * @param x
	 *            (initialized leaf)
	 * @param center
	 * @return
	 */
	protected final boolean validateT(SpaceTreeLeaf x, float[] center) {
		int i = 0;
		boolean res = true;
		while (i < center.length && res) {
			assert x.normalizeAux(center[i], i) == 0.5 : " c[i]: " + center[i]
					+ " i " + i + " T(c[i] " + x.normalizeAux(center[i], i);
			res = x.normalizeAux(center[i], i) == 0.5;
			i++;
		}
		return res;
	}

	/**
	 * Converts a tuple that has been normalized from 1 to 0 (fist pass) into
	 * one value that is n * 2 * d pv(norm(tuple)) where: n is the space where
	 * the tuple is d is the # of pivots of this index pv is the pyramid value
	 * for a tuple norm() is the normalization applied in the given space
	 *
	 * @param tuple
	 * @return the P+Tree value
	 */
	protected final float ppvalue(final float[] tuple) {

		SpaceTreeLeaf n = (SpaceTreeLeaf) this.spaceTree.search(tuple);
		float[] result = new float[pivotsCount];
		n.normalize(tuple, result);
		return n.getSNo() * 2 * pivotsCount + super.pyramidValue(result);
	}
	
	protected int spaceNumber(final float[] tuple){
		SpaceTreeLeaf n = (SpaceTreeLeaf) this.spaceTree.search(tuple);
		return n.getSNo();
	}
	
	
	/**
	 * A recursive version of the space division algorithm It will use much more
	 * memory
	 *
	 * @param node
	 *            Current node of the tree to be processed
	 * @param currentLevel
	 *            Current depth
	 * @param minMax
	 * @param data
	 * @param SNo
	 */
	protected void spaceDivision(SpaceTree node, final int currentLevel,
			final float[][] minMax, final BitSet data, int[] SNo,
			Random ran, final float[] center)
			throws OBException {
		if (logger.isDebugEnabled()) {
			logger.debug("Dividing space, level:" + currentLevel
					+ " data size: " + data.cardinality());
		}

		try {
			if (currentLevel < od) { // nonleaf node processing
				// initialize clustering algorithm
				float[][] centers = kMeans(data, (byte)2, ran);
				
				// assert centers.numInstances() == 2 : "Centers found: " +
				// centers.numInstances();

				float[] CL = centers[0];
				float[] CR = centers[1];
				short DD = dividingDimension(CL, CR);
				float DV = (float) ((CR[DD] + CL[DD]) / 2);

				if (logger.isDebugEnabled()) {
					logger.debug("Details:" + currentLevel + " DD: " + DD
							+ " DV " + DV);
				}

				// Create "children" spaces
				BitSet SL = new BitSet( this.databaseSize());
				BitSet SR = new BitSet( this.databaseSize());
				
				// update space boundaries
				float[][] minMaxLeft = cloneMinMax(minMax);
				float[][] minMaxRight = cloneMinMax(minMax);
				assert DV <= minMaxLeft[DD][MAX];
				minMaxLeft[DD][MAX] = DV;
				assert DV >= minMaxLeft[DD][MIN];
				minMaxRight[DD][MIN] = DV;
				// Divide the elements of the original space
				divideSpace(data, SL, SR, DD, DV);
				assert data.cardinality() == SL.cardinality()
						+ SR.cardinality();
				SpaceTree leftNode = null;
				SpaceTree rightNode = null;

				SpaceTreeNode ntemp = (SpaceTreeNode) node;
				if (currentLevel < (od - 1)) { // if we are before the last
					// iteration (that is before adding
					// the leaves)
					leftNode = new SpaceTreeNode();
					rightNode = new SpaceTreeNode();
				} else {
					leftNode = new SpaceTreeLeaf();
					rightNode = new SpaceTreeLeaf();
				}
				ntemp.setDD(DD);
				ntemp.setDV(DV);
				ntemp.setLeft(leftNode);
				ntemp.setRight(rightNode);

				if (currentLevel < (od - 1)) {
					spaceDivision(leftNode, currentLevel + 1, minMaxLeft, SL,
							SNo, ran, null);
					spaceDivision(rightNode, currentLevel + 1, minMaxRight, SR,
							SNo, ran, null);
				} else {

					spaceDivision(leftNode, currentLevel + 1, minMaxLeft, SL,
							SNo, ran, calculateCenter(SL));
					spaceDivision(rightNode, currentLevel + 1, minMaxRight, SR,
							SNo, ran, calculateCenter(SR));

				}
			} else { // leaf node processing
				if (logger.isDebugEnabled()) {
					logger.debug("Found Space:" + SNo[0] + " data size: "
							+ data.cardinality());
				}
				assert node instanceof SpaceTreeLeaf;
				SpaceTreeLeaf n = (SpaceTreeLeaf) node;
				calculateLeaf(n, minMax, center);
				// increment the index
				n.setSNo(SNo[0]);
				SNo[0] = SNo[0] + 1;
				assert n.pointInside(center) : " center: "
						+ Arrays.toString(center) + " minmax: "
						+ Arrays.deepToString(minMax);
				assert verifyData(data, n);
			}

		} catch (OBException e1) {
			throw e1;
		} catch (Exception e) {
			// wrap weka's Exception so that we don't have to use
			// Exception in our throws clause
			if (logger.isDebugEnabled()) {
				e.printStackTrace();
			}
			throw new OBException(e);
		}
	}
	
	
	/**
	 * Performs k-means on the given cluster.
	 * @param cluster Each turned bit of the given cluster is an object ID in B
	 * @param k the number of clusters to generate
	 * @return The centroids of the clusters
	 */
	private float[][] kMeans(final BitSet cluster, byte k, Random ran)throws DatabaseException, OutOfRangeException, KMeansException{
		return kMeansAux(cluster,k,ran,0);
	}
	
	/**
	 * Executes k-means, keeps a count of the number of iterations performed...
	 * if clustering cannot converge properly, then we execute the randomized initialization procedure
	 */
	private float[][]kMeansAux(final BitSet cluster, byte k, Random ran, int iteration)throws DatabaseException, OutOfRangeException, KMeansException{
		
		if(cluster.cardinality() <= 1){
			throw new KMeansException("Cannot cluster spaces with one or less elements. Found elements: " + cluster);
		}
		float[][] centroids = new float[k][pivotsCount];
		if(iteration < this.kMeansPPIterations){
			initializeKMeansPP(cluster, k, centroids, ran); 
		}else{
			initializeKMeans(cluster, k, centroids, ran);
		}
        BitSet selection[] = initSubClusters(cluster, k);
        
		assert centroids.length == k;
		boolean modified = true;
		float [] tempTuple = new float[pivotsCount];
		while(modified){ // while there have been modifications
			int card = 0;
			int bitIndex = 0;
			modified = false;
			// we will put here all the averages used to calculate the new cluster
			float[][] averages = new float[k][pivotsCount];
			while(card < cluster.cardinality()){
				// find the closest point
				bitIndex = cluster.nextSetBit(bitIndex);
				// get the tuple
				readFromB(bitIndex, tempTuple);
				// find the closest spot
				byte closest = closest(tempTuple, centroids);
				// check if the closest cluster is still the same
				if(! selection[closest].get(bitIndex)){
					modified = true;
					// set the correct cluster where our item belongs
					updateClusterInfo(closest, selection, bitIndex);
				}
				updateAveragesInfo(closest, tempTuple, averages);
				card++;
				// if we don't do this we will keep getting the same value over and
				// over again
				bitIndex++; 
			}
			// we need to make sure that every centroid has elements, otherwise we have to execute the algorithm again
			if(someoneEmpty(selection)){
				if(logger.isDebugEnabled()){
					logger.debug("Repeating k-means: " + cluster.cardinality());
				}
				return kMeansAux(cluster, k,ran, iteration + 1);
			}
			// after finishing recalculating the pivots, we just have to 
			// center the clusters
			if(modified){
				centerClusters(centroids, averages, selection );
			}
		}
		return centroids;
	}
	
	/**
	 * Returns true if any of the given clusters is empty
	 * @param selection
	 */
	private boolean someoneEmpty(BitSet[] selection){
		byte i = 0;
		while(i < selection.length){
			if(selection[i].cardinality() == 0){
				return true;
			}
			i++;
		}
		return false;
	}
	
	private void centerClusters(float[][] centroids, float[][] averages, BitSet selection[]) {
		byte i = 0;
		assert centroids.length == averages.length && centroids.length == selection.length;
		while(i < averages.length){
			int cx = 0;
			while(cx < pivotsCount){
				centroids[i][cx] = averages[i][cx] / selection[i].cardinality();
				cx++;
			}
			i++;
		}
	}
	
	/**
	 * Adds the contents of tuple to averages[cluster]
	 * @param cluster
	 * @param tuple
	 * @param averages
	 */
	private void updateAveragesInfo(byte cluster, float[] tuple, float[][]averages){
		int i = 0;
		while(i < pivotsCount){
			averages[cluster][i] += tuple[i];
			i++;
		}
	}
	
	/**
	 * Sets the ith element in selection[cluster] and set the ith bit in
	 * the other clusters to 0. 
	 * @param cluster
	 * @param element
	 * @param selection
	 */
	private void updateClusterInfo(byte cluster, BitSet[] selection, int element){
		byte i = 0;
		while(i < selection.length){
			if(i == cluster){
				selection[i].set(element);
			}else{
				selection[i].clear(element);
			}
			i++;
		}
	}
	
	private byte closest(float [] tuple, float[][] centroids){
		byte i = 0;
		byte res = 0;
		float value = Float.MAX_VALUE;
		while(i < centroids.length){
			float temp = euclideanDistance(tuple, centroids[i]);
			if(temp < value){
				value = temp;
				res = i;
			}
			i++;
		}
		return res;
	}
	
	/** 
	 * Computes the euclidean distance for the given tuples
	 * @param a
	 * @param b
	 * @return
	 */
	private float euclideanDistance(float[] a, float[] b){
		int i = 0;
		float res = 0;
		while(i < pivotsCount){
			float t = a[i] - b[i];
			res += t * t;
			i++;
		}
		return (float)Math.sqrt(res);
	}
	
	private BitSet [] initSubClusters(BitSet cluster, byte k){
		BitSet [] res = new BitSet[k];
		byte i = 0;
		while(i < k){
			res[i] = new BitSet(cluster.size());
			i++;
		}
		return res;
	}
	
	public int totalBoxes(){
		return (int)Math.pow(2,  this.od);
	}

	/**
	 * Initializes k centroids (Default method)
	 * @param cluster
	 * @param k
	 * @param centroids
	 */
	private void initializeKMeans(BitSet cluster, byte k, float[][] centroids, Random r) throws DatabaseException, OutOfRangeException{
		int total = cluster.cardinality();
		byte i = 0;
		int centroidIds[] = new int[k];
		while(i < k){
			int t;
			int id;
			do{
				t = r.nextInt(total);	
				// we should actually return the tth element instead of this
				id = returnIth(cluster, t);
			}
			while(id == -1 || contains(id, centroidIds, i));
			
			centroidIds[i] = id;	
			readFromB(id, centroids[i]);
			i++;
		}
	}
	
	
	float kMeansPPDistance(float [] a, float [] b){
		assert a.length == b.length;
		float res = 0;
		int i = 0;
		while(i < a.length){
			float x = a[i] - b[i];
			res += x * x;
			i++;
		}
		return res;
	}
	
	/**
	 * Initializes k centroids by using k-means++ 
	 * leaves the result in "centroids"
	 * The original paper is here:
	 * David Arthur and Sergei Vassilvitskii, "k-means++: The Advantages of Careful Seeding" SODA 2007.	
	 * This method was inspired from the source code provided by the authors
	 * This paper 
	 * @param cluster
	 * @param k
	 * @param centroids
	 */
	private void initializeKMeansPP(BitSet cluster, byte k, float[][] centroids, Random r) throws DatabaseException, OutOfRangeException{
		
	    float potential = 0;
	    int retries = 1;
	    int centroidIds[] = new int[k]; // keep track of the selected centroids
		float[] closestDistances = new float[cluster.cardinality()];
		float[] tempA = new float[pivotsCount];
		float[] tempB = new float[pivotsCount];

		// Randomly select one center
		int t = r.nextInt(cluster.cardinality());			
		int index = returnIth(cluster, t);
		int currentCenter = 0;
		centroidIds[currentCenter] = index;
		readFromB(index, centroids[currentCenter]);
		int i = 0;
		t = 0;
		//Random ran0 = new Random(r.nextInt());
	    while(i < cluster.cardinality()) {
	    	t = cluster.nextSetBit(t);
	    	readFromB(t,tempA);
	    	closestDistances[i] = kMeansPPDistance(tempA, centroids[currentCenter]);
	        potential += closestDistances[i];
	        i++;
	        t++;
	    }

		// Choose the remaining k-1 centers
	    int centerCount = 1;
		while ( centerCount < k) {

	        // Repeat several times
	        float bestPotential = -1;
	        int bestIndex = -1;
	        for (int retry = 0; retry < retries; retry++) {
			
	        	// choose the new center
			    float probability = r.nextFloat()  * potential;
	            for (index = 0; index < cluster.cardinality(); index++) {
	            	if(contains(index, centroidIds, centerCount)){continue;}
	                if(probability <= closestDistances[index])
	                    break;
	                else
	                    probability -= closestDistances[index];
	            }

	    		// Compute the new potential
	            float newPotential = 0;
	            t = 0;
	            readFromB(index, tempB);
			    for (i = 0; i < cluster.cardinality(); i++){
			    	t = cluster.nextSetBit(t);
			    	readFromB(t, tempA);
	                newPotential += Math.min( kMeansPPDistance(tempA, tempB), closestDistances[i] );
	                t++;
			    }

	            // Store the best result
	            if (bestPotential < 0 || newPotential < bestPotential) {
	                bestPotential = newPotential;
	                bestIndex = index;
	            }
			}

	        // Add the appropriate center
	        readFromB(bestIndex, centroids[centerCount]);  
	        potential = bestPotential;
	        t = 0;
	        readFromB(bestIndex, tempB);
	        for (i = 0; i < cluster.cardinality(); i++){
	        	t = cluster.nextSetBit(t);
	        	readFromB(t, tempA);
	            closestDistances[i] = Math.min( kMeansPPDistance(tempA, tempB), closestDistances[i] );
	            t++;
	        }
	        // make sure that the same center is not found
	        assert ! contains(bestIndex, centroidIds, centerCount) ;
	        centerCount++;	        
		}
	}
	
	/**
	 * Returns the ith set bit of the given vector
	 * @param cluster
	 * @param i
	 * @return
	 */
	private int returnIth(BitSet cluster, int i){
		int cx = 0;
		int t = 0;
		assert i < cluster.cardinality();
		while(cx < cluster.cardinality()){
			t = cluster.nextSetBit(t);
			if(cx == i){
				return t;
			}
			cx++;
			t++;
		}
		assert false;
		return t;
	}
	
	/**
	 * returns true if id is in the array ids
	 * performs the operation up to max (inclusive)
	 * if max is 0 this function always returns false
	 * @param id
	 * @param ids
	 * @param max
	 * @return true if id is in the array ids
	 */
	private boolean contains(int id, int[] ids, int max){
		int i = 0;
		if(max == 0){
			return false;
		}
		while(i < ids.length && i <= max){
			if(ids[i] == id){
				return true;
			}
			i++;
		}
		return false;
	}
	
	/**
	 * Read the given tuple from B database and load it into the given tuple
	 * @param id
	 * @param tuple
	 */
	protected abstract void readFromB(int id, float[] tuple) throws DatabaseException, OutOfRangeException;

	
	/**
	 * Verifies that all the data that is going to be inserted in this leaf
	 * belongs to the given leaf
	 *
	 * @param instances
	 * @param n
	 */
	protected boolean verifyData(BitSet instances, SpaceTreeLeaf n) throws OutOfRangeException, DatabaseException{
		int i = 0;
		boolean res = true;
		float[] tempTuple = new float[pivotsCount];
		int t = 0;
		while (i < instances.cardinality() && res) {
			t = instances.nextSetBit(t);
			this.readFromB(t, tempTuple);
			res = n.pointInside(tempTuple);
			i++;
			t++;
		}
		return res;
	}

	/**
	 * Calculates the center of the given data based on medians (just like the
	 * extended pyramid technique)
	 *
	 * @param data
	 * @return the center of the given data
	 */
	protected final float[] calculateCenter(BitSet data) throws DatabaseException, OutOfRangeException{

		QuantileBin1D[] medianHolder = createMedianHolders(data.cardinality());
		int i = 0;
		float [] tempTuple = new float[pivotsCount];
		int t = 0;
		while (i < data.cardinality()) {
			t = data.nextSetBit(t);
			if(t == -1){break;}
			this.readFromB(t, tempTuple);
			super.updateMedianHolder(tempTuple,
					medianHolder);
			i++;
			t++;
		}

		// now we just have to get the medians
		// int
		i = 0;
		float[] res = new float[pivotsCount];
		while (i < pivotsCount) {
			 res[i] = (float)medianHolder[i].median();
			//res[i] = (float) data.meanOrMode(i);
			i++;
		}
		return res;
	}

	private final float[] convertDoubleToFloat(double[] arr) {
		float[] res = new float[arr.length];
		int i = 0;
		while (i < arr.length) {
			res[i] = (float) arr[i];
			i++;
		}
		return res;
	}

	private final float[][] cloneMinMax(float[][] minMax) {
		float[][] res = new float[pivotsCount][2];
		int i = 0;
		while (i < minMax.length) {
			res[i][MIN] = minMax[i][MIN];
			res[i][MAX] = minMax[i][MAX];
			i++;
		}
		return res;
	}

	/**
	 * Initializes minMax bouding values
	 *
	 * @param data
	 */
	private final void initMinMax(float[][] data) {
		int cx = 0;
		assert data.length == pivotsCount;
		while (cx < data.length) {
			data[cx][MIN] = 0;
			data[cx][MAX] = 1;
			cx++;
		}
	}

	/**
	 * Divides original space. For each v that belongs to "original" if v_DD <
	 * DV then v belongs to "left". Otherwise v belongs to "right"
	 *
	 * @param original
	 * @param left
	 *            (output argument)
	 * @param right
	 *            (output argument)
	 * @param DD
	 * @param DV
	 */
	protected final void divideSpace(BitSet original, BitSet left,
			BitSet right, int DD, double DV) throws OutOfRangeException, DatabaseException {
		int i = 0;
		float [] tempTuple = new float[pivotsCount];
		int t = 0;
		while (i < original.cardinality()) {
			 t = original.nextSetBit(t);
			 
			 if(t == -1) {break;} 
			 
			this.readFromB(t, tempTuple);
			if (tempTuple[DD] < DV) {
				left.set(t);
			} else {
				right.set(t);
			}
			i++; 
			t++;
		}
	}

	/**
	 * Calculate the dividing dimension for cl and cr
	 *
	 * @param cl
	 * @param cr
	 * @return the dimension that has the biggest gap between cl and cr
	 */
	protected final short dividingDimension(float[] cl, float[] cr) {
		int res = 0;
		int i = 0;
		double max = Double.MIN_VALUE;
		while (i < pivotsCount) {
			double current = Math.abs(cl[i] - cr[i]);
			if (current > max) {
				max = current;
				res = i;
			}
			i++;
		}
		return (short) res;
	}

	

}
