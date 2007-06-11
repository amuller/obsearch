package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import hep.aida.bin.QuantileBin1D;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.ClusteringFailedException;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.pptree.SpaceTree;
import org.ajmm.obsearch.index.pptree.SpaceTreeLeaf;
import org.ajmm.obsearch.index.pptree.SpaceTreeNode;
import org.apache.log4j.Logger;

import weka.clusterers.SimpleKMeans;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

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
	public AbstractPPTree(final File databaseDirectory, final byte pivots,
			final byte od) throws DatabaseException, IOException {
		super(databaseDirectory, pivots);
		this.od = od;
	}

	/**
	 * ,attrs Calculates the space-tree
	 */
	// TODO: We could override freeze and remove the creation of database B
	// P+Tree needs B so maybe this is not so relevant.
	// recursive is prettier... this method should be recursive. But iterative
	// might
	// be the only way of completing this heavy task in most computers.
	/*protected void calculateIndexParameters() throws DatabaseException,
			IllegalAccessException, InstantiationException,
			OutOfRangeException, OBException {
		long count = super.bDB.count();
		// each median for each dimension will be stored in this array

		Cursor cursor = null;
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		FastVector attrs = new FastVector(pivotsCount);
		// create the attributes that weka will use to do clustering
		// one attribute per each of the pivotsCount dimensions
		int i = 0;
		while (i < pivotsCount) {
			Attribute x = new Attribute("p" + i); // numeric attribute
			attrs.addElement(x);
			i++;
		}

		// hope that memory will be enough, we have to re-implement instances
		Instances data = new Instances("0", attrs, this.databaseSize());
		Random ran = new Random();
		try {
			i = 0;
			cursor = bDB.openCursor(null, null);
			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				assert i == IntegerBinding.entryToInt(foundKey);

				TupleInput in = new TupleInput(foundData.getData());
				Instance ins = createInstance(in);
				data.add(ins);
				i++;
			}
			// pivot count and read # of pivots
			// should be the same
			assert i == count;
			// compact the original data just in case
			data.compactify();

			// now we can start building the space tree, all the data is in
			// memory
			// TODO: Make weka use secondary storage.
			// Extend the class Instances and in the constructor
			// create a new type of fastvector
			// that uses secondary storage and some kind of cache.

			int totalSpaces = (int) Math.pow(2, od);
			Instances[] spaces = new Instances[totalSpaces];
			Instances[] spacesTemp = new Instances[totalSpaces];
			SpaceTree[] treeNodes = new SpaceTree[totalSpaces];
			SpaceTreeNode node = new SpaceTreeNode(); // this will hold the
			// space tree
			treeNodes[0] = node;
			// contains the bouding values for each of the spaces
			float[][][] minMaxes = new float[totalSpaces][pivotsCount][2];
			// this will hold the centers for each of the final subspaces
			Instance[] cs = new Instance[totalSpaces];
			initMinMaxes(minMaxes);
			spaces[0] = data; // we will process space 0 first
			data = null; // hope that we will save some space
			for (int cdt = 0; cdt < od; cdt++) {
				for (int n = 0; n < Math.pow(2, cdt); n++) {
					Instances centers = null;
					Instances spaceN = spaces[n];
					try {

						// initialize clustering algorithm
						SimpleKMeans c = new SimpleKMeans();
						c.setNumClusters(2);
						c.setSeed(ran.nextInt());
						// execute the clustering algorithm
						c.buildClusterer(spaceN);
						// get the centers of the clusters
						centers = c.getClusterCentroids();
						assert centers.numInstances() == 2;

					} catch (Exception e) {
						// wrap weka's Exception so that we don't have to use
						// Exception in our throws clause
						throw new OBException(e);
					}
					// using upercase (breaking coding standards)
					// to be compatible with the format of the original paper
					Instance CL = centers.instance(0);
					Instance CR = centers.instance(1);
					byte DD = dividingDimension(CL, CR);
					float DV = (float) (CR.value(DD) + CL.value(DD)) / 2;

					// Create "children" spaces
					int SNoSL = 2 * n;
					int SNoSR = 2 * n + 1;
					Instances SL = new Instances("" + SNoSL, attrs, this
							.databaseSize());
					Instances SR = new Instances("" + SNoSR, attrs, this
							.databaseSize());
					// update space boundaries
					minMaxes[SNoSL][DD][MAX] = DV;
					minMaxes[SNoSR][DD][MIN] = DV;
					// Divide the elements of the original space
					divideSpace(spaceN, SL, SR, DD, DV);
					spacesTemp[SNoSL] = SL;
					spacesTemp[SNoSR] = SR;
					// insert a nonleaf node
					assert !treeNodes[n].isLeafNode();
					SpaceTreeNode ntemp = (SpaceTreeNode) treeNodes[n];
					SpaceTree leftNode = null;
					SpaceTree rightNode = null;
					if (cdt < (od - 1)) { // if we are before the last
						// iteration (that is before adding
						// the leaves)
						leftNode = new SpaceTreeNode();
						rightNode = new SpaceTreeNode();
					} else {
						leftNode = new SpaceTreeLeaf();
						rightNode = new SpaceTreeLeaf();
						// this is the last iteration...
						// we have to store the cluster centers
						cs[SNoSL] = CL;
						cs[SNoSR] = CR;
					}
					ntemp.setDD(DD);
					ntemp.setDV(DV);
					ntemp.setLeft(leftNode);
					ntemp.setRight(rightNode);
					treeNodes[SNoSL] = leftNode;
					treeNodes[SNoSR] = rightNode;

				} // after cdt is updated SD8 in the paper

				// copy the data in spacesTemp to spaces
				for (int n = 0; n < Math.pow(2, cdt); n++) {
					if (spacesTemp[n] != null) {
						spaces[n] = spacesTemp[n];
						spacesTemp[n] = null;
					}
				}

			}
			// treeNodes should now all be leaves.
			// the ith element of the array is the leaf for the
			// ith space
			// we have to calculate the centers for each space
			// and we have to calculate a[d] b[d] e[d]
			for (int n = 0; n < totalSpaces; n++) {
				assert treeNodes[n] instanceof SpaceTreeLeaf;
				calculateLeaf((SpaceTreeLeaf) treeNodes[n], minMaxes[n], cs[n]);
			}
			// save the space tree
			spaceTree = node;
		} finally {
			cursor.close();
		}

		logger.debug("Space Tree calculated");
	}
*/

//	 TODO: We could override freeze and remove the creation of database B
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

		Cursor cursor = null;
		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		FastVector attrs = new FastVector(pivotsCount);
		// create the attributes that weka will use to do clustering
		// one attribute per each of the pivotsCount dimensions
		logger.info("Calculating Space Tree");
		int i = 0;
		while (i < pivotsCount) {
			Attribute x = new Attribute("p" + i); // numeric attribute
			attrs.addElement(x);
			i++;
		}


		// hope that memory will be enough, we have to re-implement instances
		Instances data = new Instances("0", attrs, this.databaseSize());
		Random ran = new Random();
		try {
			i = 0;
			cursor = bDB.openCursor(null, null);
			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				assert i == IntegerBinding.entryToInt(foundKey);
				if(logger.isDebugEnabled()){
					if(i % 10000 == 0){
						logger.debug("Adding to weka: " + i);
					}
				}
				TupleInput in = new TupleInput(foundData.getData());
				Instance ins = createInstance(in);
				data.add(ins);
				i++;
			}
			// pivot count and read # of pivots
			// should be the same
			logger.debug("Finished adding data to weka");
			assert i == count;
			// compact the original data just in case
			data.compactify();
			SpaceTreeNode node = new SpaceTreeNode(); // this will hold the
			// now we just have to create the space tree
			float[][] minMax = new float[pivotsCount][2];
			initMinMax(minMax);
			int[] SNo = new int[1]; // this is a pointer
			// divide the space
			spaceDivision(node, 0, minMax, data, SNo, ran, attrs, null);
			// now the space-tree has been built.
			// save the space tree
			this.spaceTree = node;
		} finally {
			cursor.close();
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
		float[] a = new float[pivotsCount];
		float[] b = new float[pivotsCount];
		float[] e = new float[pivotsCount];
		while (i < pivotsCount) {
			assert minMax[i][MIN] < center[i] && center[i] < minMax[i][MAX] :
					"MIN: " + minMax[i][MIN] + " CENTER: " + center[i] + " MAX: " + minMax[i][MAX]  ;
			// divisors != 0
			assert (minMax[i][MAX] - minMax[i][MIN]) != 0;
			assert (Math.log(a[i] * center[i]
			          					- b[i]) / Math.log(2)) != 0;

			a[i] = 1 / (minMax[i][MAX] - minMax[i][MIN]);
			b[i] = minMax[i][MIN] / (minMax[i][MAX] - minMax[i][MIN]);
			e[i] = (float) -(1 / (Math.log(a[i] * center[i]
					- b[i]) / Math.log(2)));

			assert center[i] >= 0 && center[i] <= 1;
			assert minMax[i][MIN] >= 0 && minMax[i][MAX] <=1;
			assert minMax[i][MIN] <= center[i] && center[i] <= minMax[i][MAX] : " Center: " + center[i] + " min: " + minMax[i][MIN] + " max: " +  minMax[i][MAX] ;
			i++;
		}
		x.setA(a);
		x.setB(b);
		x.setE(e);
		x.setMinMax(minMax);
	}

	/**
	 * Converts a tuple that has been normalized from 1 to 0 (fist pass) into
	 * one value that is n  * 2 * d pv(norm(tuple))
	 * where: n is the space where the tuple is
	 *             d is the # of pivots of this index
	 *             pv is the pyramid value for a tuple
	 *             norm() is the normalization applied in the given space
	 * @param tuple
	 * @return the P+Tree value
	 */
	protected float ppvalue(final float[] tuple){

		SpaceTreeLeaf n = (SpaceTreeLeaf)this.spaceTree.search(tuple);
		float[] result = new float[pivotsCount];
		n.normalize(tuple, result);
		return n.getSNo() * 2 * pivotsCount + super.pyramidValue(result);
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
	protected void spaceDivision(SpaceTree node, int currentLevel,
			float[][] minMax, Instances data, int[] SNo, Random ran,
			FastVector attrs, float[] center) throws OBException {
		if(logger.isDebugEnabled()){
			logger.debug("Dividing space, level:" + currentLevel);
		}

		try {
			if (currentLevel < od) { // nonleaf node processing
			// initialize clustering algorithm
			SimpleKMeans c = new SimpleKMeans();
			c.setNumClusters(2);
			c.setSeed(ran.nextInt());
			// execute the clustering algorithm
			c.buildClusterer(data);
			// get the centers of the clusters
			Instances centers = c.getClusterCentroids();
			// TODO: develop a way of handling the case when only one
			// cluster is found. Alternative, ask the user to give more data.
			int repetitions = 10;

			while(centers.numInstances() < 2 && repetitions != 0){
				c = new SimpleKMeans();
				c.setNumClusters(2);
				c.setSeed(ran.nextInt());
				c.buildClusterer(data);
				centers = c.getClusterCentroids();
				logger.info("Repeating because centers found:" + centers.numInstances() + " Total instances: " + data.numInstances());
				repetitions--;
			}

			if(centers.numInstances() < 2){
				throw new ClusteringFailedException("Did not find enough data for this cluster. You should add more data!");
			}
			//assert centers.numInstances() == 2 : "Centers found: "  + centers.numInstances();

			Instance CL = centers.instance(0);
			Instance CR = centers.instance(1);
			byte DD = dividingDimension(CL, CR);
			float DV = (float) (CR.value(DD) + CL.value(DD)) / 2;

			// Create "children" spaces
			Instances SL = new Instances("left", attrs, this.databaseSize());
			Instances SR = new Instances("right", attrs, this.databaseSize());
			// update space boundaries
			float[][] minMaxLeft = cloneMinMax(minMax);
			float[][] minMaxRight = cloneMinMax(minMax);
			minMaxLeft[DD][MAX] = DV;
			minMaxRight[DD][MIN] = DV;
			// Divide the elements of the original space
			divideSpace(data, SL, SR, DD, DV);

			SpaceTree leftNode = null;
			SpaceTree rightNode = null;

				SpaceTreeNode ntemp = (SpaceTreeNode)node;
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
					spaceDivision(leftNode, currentLevel + 1, minMaxLeft, SL, SNo, ran, attrs, null);
					spaceDivision(rightNode, currentLevel + 1, minMaxRight, SR, SNo, ran, attrs, null);
				}else{

					spaceDivision(leftNode, currentLevel + 1, minMaxLeft, SL, SNo, ran, attrs, calculateCenter(SL));
					spaceDivision(rightNode, currentLevel + 1, minMaxRight, SR, SNo, ran, attrs, calculateCenter(SR));
				}
			} else { // leaf node processing
				if(logger.isDebugEnabled()){
					logger.debug("Found Space:" + SNo[0]);
				}
				assert node instanceof SpaceTreeLeaf;
				SpaceTreeLeaf n  = (SpaceTreeLeaf)node;
				calculateLeaf(n, minMax, center);
				// increment the index
				n.setSNo(SNo[0]);
				SNo[0] = SNo[0] + 1;
				assert n.pointInside(center): " center: " + Arrays.toString(center) + " minmax: " + Arrays.deepToString(minMax);
				assert verifyData(data,n);
			}

		}
		 catch(OBException e1 ){
			 throw e1;
		 }
		 catch (Exception e) {
			// wrap weka's Exception so that we don't have to use
			// Exception in our throws clause
			if(logger.isDebugEnabled()){
				e.printStackTrace();
			}
			throw new OBException(e);
		}
	}

	/**
	 * Verifies that all the data that is going to be inserted in this leaf
	 * belongs to the given leaf
	 * @param instances
	 * @param n
	 */
	protected boolean verifyData(Instances instances, SpaceTreeLeaf n){
		int i = 0;
		boolean res = true;
		while(i < instances.numInstances() && res){
			res = n.pointInside(this.convertDoubleToFloat(instances.instance(i).toDoubleArray()));
			i++;
		}
		return res;
	}

	/**
	 * Calculates the center of the given data based
	 * on medians (just like the extended pyramid technique)
	 * @param data
	 * @return the center of the given data
	 */
	protected float [] calculateCenter(Instances data){
		  QuantileBin1D[] medianHolder = createMedianHolders(data.numInstances());
		  int i = 0;
		  while(i < data.numInstances()){
			  Instance in = data.instance(i);
			  super.updateMedianHolder(convertDoubleToFloat(in.toDoubleArray()), medianHolder);
			  i++;
		  }
		  // now we just have to get the medians
		 //int
		 i = 0;
		  float[] res = new float[pivotsCount];
		  while(i < pivotsCount){
			  res[i] = (float)medianHolder[i].median();
			  i++;
		  }
		  return res;
	}



	private static float[] convertDoubleToFloat(double[] arr){
		float[] res = new float[arr.length];
		int i = 0;
		while(i < arr.length){
			res[i] = (float) arr[i];
			i++;
		}
		return res;
	}

	private float[][] cloneMinMax(float[][] minMax) {
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
	private void initMinMax(float[][] data) {
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
	protected void divideSpace(Instances original, Instances left,
			Instances right, int DD, double DV) {
		int i = 0;
		while (i < original.numInstances()) {
			Instance j = original.instance(i);
			if (j.value(DD) < DV) {
				left.add(j);
			} else {
				right.add(j);
			}
			i++;
		}
		left.compactify();
		right.compactify();
	}

	/**
	 * Calculate the dividing dimension for cl and cr
	 *
	 * @param cl
	 * @param cr
	 * @return the dimension that has the biggest gap between cl and cr
	 */
	public byte dividingDimension(Instance cl, Instance cr) {
		int res = 0;
		int i = 0;
		double max = Double.MIN_VALUE;
		while (i < pivotsCount) {
			double current = Math.abs(cl.value(i) - cr.value(i));
			if (current > max) {
				max = current;
				res = i;
			}
			i++;
		}
		return (byte) res;
	}

	/**
	 * Generates a weka instance object from the given inputtuple
	 *
	 * @param in
	 *            tuple with the input values
	 * @param attrs
	 *            attribute definitions
	 * @return an instance generated from the given tuple
	 */
	protected Instance createInstance(TupleInput in) throws OutOfRangeException {
		float[] tuple = extractTuple(in);
		int i = 0;
		Instance res = new Instance(pivotsCount);
		while (i < pivotsCount) {
			res.setValue(i, tuple[i]);
			i++;
		}
		return res;
	}


}
