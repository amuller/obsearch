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

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

 This program is free software: you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 of the License, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
/**
 * An Abstract P+Tree. Contains common functionality to all the P+trees
 * @author Arnoldo Jose Muller Molina
 * @param <O>
 *            The type of object to be stored in the Index.
 * @version %I%, %G%
 * @since 0.0
 */

public abstract class AbstractPPTree < O extends OB >
        extends AbstractExtendedPyramidIndex < O > {

    /**
     * Logger for the class.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractPPTree.class);

    /**
     * Partitions to be used when generating the space tree.
     */
    private byte od;

    /**
     * Root node of the space tree.
     */
    protected SpaceTree spaceTree;

    /**
     * Hack to catch when k-meanspp is not able to generate centers that
     * converge.
     */
    protected static final int KMEANS_PP_ITERATIONS = 3;

    /**
     * Holds the spaceTree's leaf nodes so we can access them fast.
     */
    protected transient SpaceTreeLeaf[] spaceTreeLeaves;

    /**
     * AbstractPPTree Constructs a P+Tree.
     * @param databaseDirectory
     *            the database directory
     * @param pivots
     *            how many pivots will be used
     * @param od
     *            parameter used to specify the number of divisions. 2 ^ od
     *            divisions will be performed.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws IOException
     *             if the index serialization process fails
     */
    public AbstractPPTree(final File databaseDirectory, final short pivots,
            final byte od) throws DatabaseException, IOException {
        super(databaseDirectory, pivots);
        this.od = od;
    }

    /**
     * Initializes the spaceTreeLeaves array. It assumes that the spaceTree is
     * alreay initialized/loaded
     */
    protected final void initSpaceTreeLeaves() {
        assert spaceTree != null;
        int max = super.totalBoxes();
        spaceTreeLeaves = new SpaceTreeLeaf[max];
        int i = 0;
        while (i < max) {
            spaceTreeLeaves[i] = spaceTree.searchSpace(i);
            i++;
        }
    }

    /**
     * Calculates the space Tree.
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OBException
     *             User generated exception
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws IllegalAccessException
     *             If there is a problem when instantiating objects O
     * @throws InstantiationException
     *             If there is a problem when instantiating objects O
     */
    protected final void calculateIndexParameters() throws DatabaseException,
            IllegalAccessException, InstantiationException,
            OutOfRangeException, OBException {

        Random ran = new Random(System.currentTimeMillis());

        BitSet data = new BitSet(this.databaseSize());
        data.set(0, this.databaseSize());
        SpaceTreeNode node = new SpaceTreeNode(); // this will hold the
        // now we just have to create the space tree
        float[][] minMax = new float[pivotsCount][2];
        initMinMax(minMax);
        int[] sNo = new int[1]; // this is a pointer
        // divide the space
        spaceDivision(node, 0, minMax, data, sNo, ran, null);
        // we created all the spaces.
        assert sNo[0] == Math.pow(2, od);
        // now the space-tree has been built.
        // save the space tree
        this.spaceTree = node;
        if (logger.isDebugEnabled()) {
            logger.debug("Space tree: \n" + spaceTree);
        }
        // add a handy shortcut to access space tree leaves.
        this.initSpaceTreeLeaves();
        logger.debug("Space Tree calculated");

    }

    /**
     * Calculates the parameters for the leaf based on minMax, and the center.
     * @param x
     *            The leaf to be processed
     * @param minMax
     *            a 2 item array.
     * @param center
     *            the center of the space that the leaf is going to represent.
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
     * validates that the properties of function T are preserved in x.
     * @param x
     *            (initialized leaf)
     * @param center
     *            The center of the leaf
     * @return true if the leaf is valid.
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
     * Calculates a P+Tree value for the given tuple. Converts a tuple that has
     * been normalized from 1 to 0 (fist pass) into one value that is n * 2 * d
     * pv(norm(tuple)) where: n is the space where the tuple is d is the # of
     * pivots of this index pv is the pyramid value for a tuple norm() is the
     * normalization applied in the given space.
     * @param tuple
     *            The tuple that will be processed
     * @return the P+Tree value
     */
    protected final float ppvalue(final float[] tuple) {

        SpaceTreeLeaf n = (SpaceTreeLeaf) this.spaceTree.search(tuple);
        float[] result = new float[pivotsCount];
        n.normalize(tuple, result);
        return n.getSNo() * 2 * pivotsCount + super.pyramidValue(result);
    }

    /**
     * Calculate a space number for the given tuple.
     * @param tuple
     *            Tuple to be processed.
     * @return space number for the given tuple.
     */
    protected int spaceNumber(final float[] tuple) {
        SpaceTreeLeaf n = (SpaceTreeLeaf) this.spaceTree.search(tuple);
        return n.getSNo();
    }

    /**
     * A recursive version of the space division algorithm.
     * @param node
     *            Current node of the tree to be processed
     * @param currentLevel
     *            Current depth
     * @param minMax
     *            The current min and maximum values for each of the dimensions
     *            of the current space.
     * @param data
     *            All the data that is going to be processed
     * @param SNo
     *            The current space number (used as an array of 1 elmenet so
     *            that it can be seen by all the other recursion branches)
     * @param ran
     *            A random number generator
     * @param center
     *            Calculated center of the space.
     * @throws OBException
     *             User generated exception
     */
    protected void spaceDivision(SpaceTree node, final int currentLevel,
            final float[][] minMax, final BitSet data, int[] SNo, Random ran,
            final float[] center) throws OBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Dividing space, level:" + currentLevel
                    + " data size: " + data.cardinality());
        }

        try {
            if (currentLevel < od) { // nonleaf node processing
                // initialize clustering algorithm
                float[][] centers = kMeans(data, (byte) 2, ran);

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
                BitSet SL = new BitSet(this.databaseSize());
                BitSet SR = new BitSet(this.databaseSize());

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
     * @param cluster
     *            Each turned bit of the given cluster is an object ID in B
     * @param k
     *            the number of clusters to generate
     * @param ran
     *            Random number generator
     * @return The centroids of the clusters
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws KMeansException
     *             If k-means++ fails to find clusters
     */
    private float[][] kMeans(final BitSet cluster, byte k, Random ran)
            throws DatabaseException, OutOfRangeException, KMeansException {
        return kMeansAux(cluster, k, ran, 0);
    }

    /**
     * Executes k-means, keeps a count of the number of iterations performed...
     * if clustering cannot converge properly, then we execute the randomized
     * initialization procedure.
     * @param cluster
     *            BitSet with the elements of the current data set
     * @param k
     *            Number of clusters to generate
     * @param ran
     *            Random function
     * @param iteration
     *            Number of iterations
     * @return The centers of the cluster.
     * @throws KMeansException
     * @throws DatabaseException
     *             If something goes wrong with the DB
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws KMeansException
     *             If k-means++ fails to find clusters
     */
    private float[][] kMeansAux(final BitSet cluster, byte k, Random ran,
            int iteration) throws DatabaseException, OutOfRangeException,
            KMeansException {

        if (cluster.cardinality() <= 1) {
            throw new KMeansException(
                    "Cannot cluster spaces with one or less elements. Found elements: "
                            + cluster);
        }
        float[][] centroids = new float[k][pivotsCount];
        if (iteration < this.KMEANS_PP_ITERATIONS) {
            initializeKMeansPP(cluster, k, centroids, ran);
        } else {
            initializeKMeans(cluster, k, centroids, ran);
        }
        BitSet selection[] = initSubClusters(cluster, k);

        assert centroids.length == k;
        boolean modified = true;
        float[] tempTuple = new float[pivotsCount];
        while (modified) { // while there have been modifications
            int card = 0;
            int bitIndex = 0;
            modified = false;
            // we will put here all the averages used to calculate the new
            // cluster
            float[][] averages = new float[k][pivotsCount];
            while (card < cluster.cardinality()) {
                // find the closest point
                bitIndex = cluster.nextSetBit(bitIndex);
                // get the tuple
                readFromB(bitIndex, tempTuple);
                // find the closest spot
                byte closest = closest(tempTuple, centroids);
                // check if the closest cluster is still the same
                if (!selection[closest].get(bitIndex)) {
                    modified = true;
                    // set the correct cluster where our item belongs
                    updateClusterInfo(closest, selection, bitIndex);
                }
                updateAveragesInfo(closest, tempTuple, averages);
                card++;
                // if we don't do this we will keep getting the same value over
                // and
                // over again
                bitIndex++;
            }
            // we need to make sure that every centroid has elements, otherwise
            // we have to execute the algorithm again
            if (someoneEmpty(selection)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Repeating k-means: " + cluster.cardinality());
                }
                return kMeansAux(cluster, k, ran, iteration + 1);
            }
            // after finishing recalculating the pivots, we just have to
            // center the clusters
            if (modified) {
                centerClusters(centroids, averages, selection);
            }
        }
        return centroids;
    }

    /**
     * Returns true if any of the given clusters is empty.
     * @param selection
     *            set of clusters to analyze
     * @return true if any of the given clusters is empty
     */
    private boolean someoneEmpty(BitSet[] selection) {
        byte i = 0;
        while (i < selection.length) {
            if (selection[i].cardinality() == 0) {
                return true;
            }
            i++;
        }
        return false;
    }

    /**
     * Find the centroids.
     * @param centroids
     *            Result is left here...
     * @param averages
     *            Average for each dimension
     * @param selection
     *            Current list of clusters
     */
    private void centerClusters(float[][] centroids, float[][] averages,
            BitSet selection[]) {
        byte i = 0;
        assert centroids.length == averages.length
                && centroids.length == selection.length;
        while (i < averages.length) {
            int cx = 0;
            while (cx < pivotsCount) {
                centroids[i][cx] = averages[i][cx] / selection[i].cardinality();
                cx++;
            }
            i++;
        }
    }

    /**
     * Adds the contents of tuple to averages[cluster].
     * @param cluster
     *            Cluster to process
     * @param tuple
     *            Tuple to add
     * @param averages
     *            The result will be stored here.
     */
    private void updateAveragesInfo(byte cluster, float[] tuple,
            float[][] averages) {
        int i = 0;
        while (i < pivotsCount) {
            averages[cluster][i] += tuple[i];
            i++;
        }
    }

    /**
     * Sets the ith element in selection[cluster] and set the ith bit in the
     * other clusters to 0.
     * @param cluster
     *            The cluster that will be set
     * @param element
     *            Elemenet id
     * @param selection
     *            The cluster we will set.
     */
    private void updateClusterInfo(byte cluster, BitSet[] selection, int element) {
        byte i = 0;
        while (i < selection.length) {
            if (i == cluster) {
                selection[i].set(element);
            } else {
                selection[i].clear(element);
            }
            i++;
        }
    }

    /**
     * Finds the centroid which is closest to tuple.
     * @param tuple
     *            The tuple to process
     * @param centroids
     *            A list of centroids
     * @return A byte indicating which is the closest centroid to the given
     *         tuple.
     */
    private byte closest(float[] tuple, float[][] centroids) {
        byte i = 0;
        byte res = 0;
        float value = Float.MAX_VALUE;
        while (i < centroids.length) {
            float temp = euclideanDistance(tuple, centroids[i]);
            if (temp < value) {
                value = temp;
                res = i;
            }
            i++;
        }
        return res;
    }

    /**
     * Computes the euclidean distance for the given tuples.
     * @param a
     *            tuple
     * @param b
     *            tuple
     * @return euclidean distance
     */
    private float euclideanDistance(float[] a, float[] b) {
        int i = 0;
        float res = 0;
        while (i < pivotsCount) {
            float t = a[i] - b[i];
            res += t * t;
            i++;
        }
        return (float) Math.sqrt(res);
    }

    /**
     * Initializes k cluster based on cluster
     * @param cluster
     *            Reference cluster
     * @param k
     *            number of clusters to generate
     * @return An array of clusters with the size of cluster
     */
    private BitSet[] initSubClusters(BitSet cluster, byte k) {
        BitSet[] res = new BitSet[k];
        byte i = 0;
        while (i < k) {
            res[i] = new BitSet(cluster.size());
            i++;
        }
        return res;
    }

    public int totalBoxes() {
        return (int) Math.pow(2, this.od);
    }

    /**
     * Initializes k centroids (Default method).
     * @param cluster
     *            original cluster
     * @param k
     *            number of clusters
     * @param centroids
     *            Centroids that will be generated
     * @param r
     *            A random function
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB.
     */
    private void initializeKMeans(BitSet cluster, byte k, float[][] centroids,
            Random r) throws DatabaseException, OutOfRangeException {
        int total = cluster.cardinality();
        byte i = 0;
        int centroidIds[] = new int[k];
        while (i < k) {
            int t;
            int id;
            do {
                t = r.nextInt(total);
                // we should actually return the tth element instead of this
                id = returnIth(cluster, t);
            } while (id == -1 || contains(id, centroidIds, i));

            centroidIds[i] = id;
            readFromB(id, centroids[i]);
            i++;
        }
    }

    /**
     * Calculates a distance used by kmeans++
     * @param a
     *            A tuple
     * @param b
     *            B tuple
     * @return The distance as calculated by kmeans++. Please refer to the
     *         paper.
     */
    float kMeansPPDistance(float[] a, float[] b) {
        assert a.length == b.length;
        float res = 0;
        int i = 0;
        while (i < a.length) {
            float x = a[i] - b[i];
            res += x * x;
            i++;
        }
        return res;
    }

    /**
     * Initializes k centroids by using k-means++ leaves the result in
     * "centroids" The original paper is here: David Arthur and Sergei
     * Vassilvitskii, "k-means++: The Advantages of Careful Seeding" SODA 2007.
     * This method was inspired from the source code provided by the authors
     * This paper
     * @param cluster
     *            Cluster to initialize
     * @param k
     *            Number of centroids
     * @param centroids
     *            The resulting centroids
     * @param r
     *            A random number generator.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     */
    private void initializeKMeansPP(BitSet cluster, byte k,
            float[][] centroids, Random r) throws DatabaseException,
            OutOfRangeException {

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
        // Random ran0 = new Random(r.nextInt());
        while (i < cluster.cardinality()) {
            t = cluster.nextSetBit(t);
            readFromB(t, tempA);
            closestDistances[i] = kMeansPPDistance(tempA,
                    centroids[currentCenter]);
            potential += closestDistances[i];
            i++;
            t++;
        }

        // Choose the remaining k-1 centers
        int centerCount = 1;
        while (centerCount < k) {

            // Repeat several times
            float bestPotential = -1;
            int bestIndex = -1;
            for (int retry = 0; retry < retries; retry++) {

                // choose the new center
                float probability = r.nextFloat() * potential;
                for (index = 0; index < cluster.cardinality(); index++) {
                    if (contains(index, centroidIds, centerCount)) {
                        continue;
                    }
                    if (probability <= closestDistances[index])
                        break;
                    else
                        probability -= closestDistances[index];
                }

                // Compute the new potential
                float newPotential = 0;
                t = 0;
                readFromB(index, tempB);
                for (i = 0; i < cluster.cardinality(); i++) {
                    t = cluster.nextSetBit(t);
                    readFromB(t, tempA);
                    newPotential += Math.min(kMeansPPDistance(tempA, tempB),
                            closestDistances[i]);
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
            for (i = 0; i < cluster.cardinality(); i++) {
                t = cluster.nextSetBit(t);
                readFromB(t, tempA);
                closestDistances[i] = Math.min(kMeansPPDistance(tempA, tempB),
                        closestDistances[i]);
                t++;
            }
            // make sure that the same center is not found
            assert !contains(bestIndex, centroidIds, centerCount);
            centerCount++;
        }
    }

    /**
     * Returns the ith set bit of the given cluster.
     * @param cluster
     *            the cluster to be processed
     * @param i
     *            the ith set bit
     * @return the ith set bit of the cluster
     */
    private int returnIth(BitSet cluster, int i) {
        int cx = 0;
        int t = 0;
        assert i < cluster.cardinality();
        while (cx < cluster.cardinality()) {
            t = cluster.nextSetBit(t);
            if (cx == i) {
                return t;
            }
            cx++;
            t++;
        }
        assert false;
        return t;
    }

    /**
     * Returns true if id is in the array ids performs the operation up to max
     * (inclusive) if max is 0 this function always returns false.
     * @param id
     *            an identification
     * @param ids
     *            a list of numbers
     * @param max
     *            the maximum point that we will process
     * @return true if id is in the array ids
     */
    private boolean contains(int id, int[] ids, int max) {
        int i = 0;
        if (max == 0) {
            return false;
        }
        while (i < ids.length && i <= max) {
            if (ids[i] == id) {
                return true;
            }
            i++;
        }
        return false;
    }

    /**
     * Read the given tuple from B database and load it into the given tuple
     * @param id
     *            object internal id
     * @param tuple
     *            store the corresponding tuple here.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     */
    protected abstract void readFromB(int id, float[] tuple)
            throws DatabaseException, OutOfRangeException;

    /**
     * Verifies that all the data that is going to be inserted in this leaf
     * belongs to the given leaf.
     * @param instances
     *            Set if data that will be verified
     * @param n
     *            Leaf that will be processed.
     * @throws OutOfRangeException
     *             If the distance of any object to any other object exceeds the
     *             range defined by the user.
     * @throws DatabaseException
     *             If somehing goes wrong with the DB
     * @return if the data is valid
     */
    protected boolean verifyData(BitSet instances, SpaceTreeLeaf n)
            throws OutOfRangeException, DatabaseException {
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
     * extended pyramid technique).
     * @param data
     *            data to be processed
     * @return the center of the given data
     */
    protected final float[] calculateCenter(BitSet data)
            throws DatabaseException, OutOfRangeException {

        QuantileBin1D[] medianHolder = createMedianHolders(data.cardinality());
        int i = 0;
        float[] tempTuple = new float[pivotsCount];
        int t = 0;
        while (i < data.cardinality()) {
            t = data.nextSetBit(t);
            if (t == -1) {
                break;
            }
            this.readFromB(t, tempTuple);
            super.updateMedianHolder(tempTuple, medianHolder);
            i++;
            t++;
        }

        // now we just have to get the medians
        // int
        i = 0;
        float[] res = new float[pivotsCount];
        while (i < pivotsCount) {
            res[i] = (float) medianHolder[i].median();
            // res[i] = (float) data.meanOrMode(i);
            i++;
        }
        return res;
    }

    /**
     * Clone the given float[][] array.
     * @param minMax
     *            Input array
     * @return a clone of minMax
     */
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
     * Initializes minMax bouding values.
     * @param data
     *            float[][] vector that will be initialized.
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
     * @param original
     *            original data set
     * @param left
     *            items to the left of the division (output argument)
     * @param right
     *            items to the right of the division (output argument)
     * @param DD
     *            See the P+tree paper
     * @param DV
     *            See the P+tree paper
     */
    protected final void divideSpace(BitSet original, BitSet left,
            BitSet right, int DD, double DV) throws OutOfRangeException,
            DatabaseException {
        int i = 0;
        float[] tempTuple = new float[pivotsCount];
        int t = 0;
        while (i < original.cardinality()) {
            t = original.nextSetBit(t);

            if (t == -1) {
                break;
            }

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
     * Calculate the dividing dimension for cl and cr.
     * @param cl left center
     * @param cr right center
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
