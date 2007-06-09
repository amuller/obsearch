package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Random;

import hep.aida.bin.QuantileBin1D;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
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



	/**,attrs
	 * Calculates the space-tree
	 */
	// TODO: We could override freeze and remove the creation of database B
	// P+Tree needs B so maybe this is not so relevant.
	// recursive is prettier... this method should be recursive.
	@Override
	protected void calculateIndexParameters() throws DatabaseException,
			IllegalAccessException, InstantiationException, OutOfRangeException, OBException{
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
			//  Extend the class Instances and in the constructor
			// create a new type of fastvector
			// that uses secondary storage and some kind of cache.

			int totalSpaces = (int)Math.pow(2, od);
			Instances[] spaces = new Instances[totalSpaces];
			Instances[] spacesTemp = new Instances[totalSpaces];
			SpaceTree[] treeNodes = new SpaceTree[totalSpaces];
			SpaceTreeNode node = new SpaceTreeNode(); // this will hold the space tree
			treeNodes[0] = node;
			// contains the bouding values for each of the spaces
			float [][][] minMaxes = new float [totalSpaces][pivotsCount][2];
			// this will hold the centers for each of the final subspaces
			Instance[] cs = new Instance[totalSpaces];
			initMinMaxes(minMaxes);
			spaces[0] = data; // we will process space 0 first
			for(int cdt = 0; cdt < od; cdt++){
				for(int n = 0; n < Math.pow(2, cdt); n++){
					Instances centers = null;
					Instances spaceN = spaces[n];
					try{

						// initialize clustering algorithm
						SimpleKMeans c = new SimpleKMeans();
						c.setNumClusters(2);
						c.setSeed(ran.nextInt());
						// execute the clustering algorithm
						c.buildClusterer(spaceN);
						// get the centers of the clusters
						centers  = c.getClusterCentroids();
						assert centers.numInstances() == 2;

					}catch(Exception e){
						// wrap weka's Exception so that we don't have to use
						// Exception in our throws clause
						throw new OBException(e);
					}
					// using upercase (breaking coding standards)
					// to be compatible with the format of the original paper
					Instance CL = centers.instance(0);
					Instance CR = centers.instance(1);
					byte DD = dividingDimension(CL, CR);
					float DV = (float)(CR.value(DD) + CL.value(DD)) / 2;

					// Create "children" spaces
					int SNoSL = 2 * n;
					int SNoSR = 2 * n + 1;
					Instances SL = new Instances("" + SNoSL, attrs, this.databaseSize());
					Instances SR = new Instances("" + SNoSR, attrs, this.databaseSize());
					// update space boundaries
					minMaxes[SNoSL][DD][MAX] = DV;
					minMaxes[SNoSR][DD][MIN] = DV;
					// Divide the elements of the original space
					divideSpace(spaceN, SL, SR, DD, DV);
					spacesTemp[SNoSL] = SL;
					spacesTemp[SNoSR] = SR;
					// insert a nonleaf node
					assert ! treeNodes[n].isLeafNode();
					SpaceTreeNode ntemp = (SpaceTreeNode) treeNodes[n];
					SpaceTree leftNode = null;
					SpaceTree rightNode = null;
					if(cdt < (od - 1)){ // if we are before the last iteration (that is before adding the leaves)
						leftNode  = new SpaceTreeNode();
						rightNode = new SpaceTreeNode();
					}else{
						leftNode  = new SpaceTreeLeaf();
						rightNode = new SpaceTreeLeaf();
						// this is the last iteration...
						// we have to store the cluster centers
					}
					ntemp.setDD(DD);
					ntemp.setDV(DV);
					ntemp.setLeft(leftNode);
					ntemp.setRight(rightNode);
					treeNodes[ SNoSL] = leftNode;
					treeNodes[ SNoSR] = rightNode;

				} // after cdt is updated SD8 in the paper

				// copy the data in spacesTemp to spaces
				for(int n = 0; n < Math.pow(2, cdt); n++){
					if(spacesTemp[n] != null){
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
			for(int n = 0; n < Math.pow(2, od); n++){

			}

		} finally {
			cursor.close();
		}
		logger.debug("Space Tree calculated");
	}

	/**
	 * Initializes minMax bouding values
	 * @param data
	 */
	private void initMinMaxes(float[][][] data){
		int i = 0;
		while(i < data.length){
			int cx = 0;
			assert data[i].length == pivotsCount;
			while(cx < data[i].length){
				data[i][cx][MIN] = 0;
				data[i][cx][MAX] = 1;
				cx++;
			}
			i++;
		}
	}
	/**
	 * Divides original space. For each v that belongs to "original" if v_DD < DV then
	 * v belongs to "left". Otherwise v belongs to "right"
	 * @param original
	 * @param left (output argument)
	 * @param right (output argument)
	 * @param DD
	 * @param DV
	 */
	protected void divideSpace(Instances original, Instances left, Instances right, int DD, double DV){
		int i = 0;
		while(i < original.numInstances()){
			Instance j = original.instance(i);
			if(j.value(DD) < DV){
				left.add(j);
			}else{
				right.add(j);
			}
			i++;
		}
		left.compactify();
		right.compactify();
	}

	/**
	 * Calculate the dividing dimension for cl and cr
	 * @param cl
	 * @param cr
	 * @return the dimension that has the biggest gap between cl and cr
	 */
	public byte dividingDimension(Instance cl, Instance cr){
		int res = 0;
		int i  = 0;
		double max = Double.MIN_VALUE;
		while(i < pivotsCount ){
			double current = Math.abs(cl.value(i) - cr.value(i));
			if(current > max){
				max  = current;
				res = i;
			}
			i++;
		}
		return (byte)res;
	}

	/**
	 * Generates a weka instance object from the given inputtuple
	 *
	 * @param in tuple with the input values
	 * @param attrs attribute definitions
	 * @return an instance generated from the given tuple
	 */
	protected Instance createInstance(TupleInput in) throws OutOfRangeException{
		float[] tuple = extractTuple(in);
		int i = 0;
		Instance res = new Instance(pivotsCount);
		while(i < pivotsCount){
			res.setValue(i, tuple[i]);
			i++;
		}
		return res;
	}

	@Override
	protected void insertFromBtoC() throws DatabaseException,
			OutOfRangeException {
		// TODO Auto-generated method stub

	}

	@Override
	protected byte insertFrozen(OB object, int id) throws IllegalIdException,
			OBException, DatabaseException, OBException,
			IllegalAccessException, InstantiationException {
		// TODO Auto-generated method stub
		return 0;
	}

	protected void insertInB(int id, OB object) throws OBException,
			DatabaseException {
		// TODO Auto-generated method stub

	}

}
