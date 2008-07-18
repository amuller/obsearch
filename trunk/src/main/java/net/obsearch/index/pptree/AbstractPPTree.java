package net.obsearch.index.pptree;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Random;

import gnu.trove.TIntHashSet;
import gnu.trove.TLongHashSet;
import hep.aida.bin.QuantileBin1D;

import net.obsearch.index.pyramid.AbstractExtendedPyramidIndex;
import net.obsearch.pivots.IncrementalPivotSelector;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.asserts.OBAsserts;
import org.ajmm.obsearch.exception.KMeansException;
import org.ajmm.obsearch.exception.KMeansHungUpException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;

import org.ajmm.obsearch.index.utils.OBRandom;
import org.ajmm.obsearch.storage.OBStoreFactory;
import org.apache.log4j.Logger;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;
import cern.jet.random.engine.MersenneTwister;
import cern.jet.random.engine.RandomSeedGenerator;

import com.sleepycat.je.DatabaseException;

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
 *                The type of object to be stored in the Index.
 * @since 0.7
 */

public abstract class AbstractPPTree < O extends OB >
        extends AbstractExtendedPyramidIndex < O > {

    /**
     * Logger for the class.
     */
    private static final transient Logger logger = Logger
            .getLogger(AbstractPPTree.class);

    /**
     * The minimum number of elements that will be accepted in each space. This
     * setting only applies for the freezing process.
     */
    private int minElementsPerSubspace = 50;

    /**
     * The number of times k-means will be executed. The best clustering will be
     * selected.
     */
    private int kMeansIterations = 3;

    /**
     * The number of times K-Means++ will be executed. The best selection of
     * initial pivots will be chosen.
     */
    private int kMeansPPRetries = 3;

    /**
     * Partitions to be used when generating the space tree.
     */
    private int od;

    /**
     * Root node of the space tree.
     */
    protected SpaceTree spaceTree;

    /**
     * Hack to catch when k-means++ is not able to generate centers that
     * converge.
     */
    private  int kMeansPPTree = 3;

    /**
     * Holds the spaceTree's leaf nodes so we can access them fast.
     */
    protected transient SpaceTreeLeaf[] spaceTreeLeaves;

    /**
     * The total # of boxes used by this P+Tree.
     */
    private int totalBoxes;

    /**
     * AbstractPPTree Constructs a P+Tree.
     * @param databaseDirectory
     *                the database directory
     * @param pivots
     *                how many pivots will be used
     * @param pivotSelector
     *                The pivot selector that will be used by this index.
     * @param od
     *                parameter used to specify the number of divisions. 2 ^ od
     *                divisions will be performed.
     * @param type The class of the object O that will be used.                 
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws IOException
     *                 if the index serialization process fails
     */
    public AbstractPPTree(Class < O > type,
            IncrementalPivotSelector < O > pivotSelector, int pivotCount, int od)
            throws  IOException, OBException {
        super(type, pivotSelector, pivotCount);
        this.od = od;
    }

    /**
     * Initializes the spaceTreeLeaves array. It assumes that the spaceTree is
     * already initialized/loaded
     */
    protected final void initSpaceTreeLeaves() {
        assert spaceTree != null;
        int max = (int)totalBoxes();
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
     *                 If something goes wrong with the DB
     * @throws OBException
     *                 User generated exception
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws IllegalAccessException
     *                 If there is a problem when instantiating objects O
     * @throws InstantiationException
     *                 If there is a problem when instantiating objects O
     */
    @Override
    protected final void calculateIndexParameters() throws 
            IllegalAccessException, InstantiationException,
            OutOfRangeException, OBException {

        Random ran = new Random(System.currentTimeMillis());
        OBAsserts.chkAssert(this.databaseSize() <= Integer.MAX_VALUE, "Too many values for freeze.");
        int maxSize = (int) this.databaseSize();
        LongArrayList data = new LongArrayList(maxSize);
        int i = 0;
        while (i < maxSize) {
            data.add(i);
            i++;
        }
        SpaceTreeNode node = new SpaceTreeNode(null); // this will hold the
        // now we just have to create the space tree
        double[][] minMax = new double[getPivotCount()][2];
        initMinMax(minMax);
        int[] sNo = new int[1]; // this is a pointer for the poor.
        // divide the space
        spaceDivision(node, 0, minMax, data, sNo, ran, null);
        // we created all the spaces.
        assert sNo[0] <= Math.pow(2, od);
        this.totalBoxes = sNo[0];
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
     *                The leaf to be processed
     * @param minMax
     *                a 2 item array.
     * @param center
     *                the center of the space that the leaf is going to
     *                represent.
     */
    protected void calculateLeaf(final SpaceTreeLeaf x, final double[][] minMax,
            final double[] center) {
        int i = 0;
        assert getPivotCount() == minMax.length;
        double[] min = new double[getPivotCount()];
        double[] width = new double[getPivotCount()];
        double[] exp = new double[getPivotCount()];
        while (i < getPivotCount()) {
            assert minMax[i][MIN] <= center[i] && center[i] <= minMax[i][MAX] : "MIN: "
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
        x.setWidth(width);
        x.setExp(exp);
        x.setMinMax(minMax);
        assert validateT(x, center);
    }

    /**
     * validates that the properties of function T are preserved in x.
     * @param x
     *                (initialized leaf)
     * @param center
     *                The center of the leaf
     * @return true if the leaf is valid.
     */
    protected final boolean validateT(final SpaceTreeLeaf x,
            final double[] center) {
        int i = 0;
        boolean res = true;
        while (i < center.length && res) {
            assert Math.abs(x.normalizeAux(center[i], i) - 0.5)< 0.000000000000001 : " c[i]: " + center[i]
                    + " i " + i + " T(c[i] " + x.normalizeAux(center[i], i);
            res = Math.abs(x.normalizeAux(center[i], i) - 0.5)< 0.000000000000001;
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
     *                The tuple that will be processed
     * @return the P+Tree value
     */
    protected final double ppvalue(final double[] tuple) {

        SpaceTreeLeaf n = this.spaceTree.search(tuple);
        double[] result = new double[getPivotCount()];
        n.normalize(tuple, result);
        return n.getSNo() * 2 * getPivotCount() + super.pyramidValue(result);

    }

    /**
     * Calculate a space number for the given tuple.
     * @param tuple
     *                Tuple to be processed.
     * @return space number for the given tuple.
     */
    protected int spaceNumber(final double[] tuple) {
        SpaceTreeLeaf n = this.spaceTree.search(tuple);
        return n.getSNo();
    }

    /**
     * This method returns true if either l is smaller than
     * {@value #minElementsPerSubspace}. This will mean that the Partition will
     * stop and that the amount of subspaces will be less than 2 ^ od
     * @param s
     *                number of elements of the given space
     * @return true if either of the spaces are less than
     *         {@link #minElementsPerSubspace}.
     */
    protected boolean shallWeStop(int s) {
        return s <= minElementsPerSubspace;
    }

    /**
     * A recursive version of the space division algorithm.
     * @param node
     *                Current node of the tree to be processed
     * @param currentLevel
     *                Current depth
     * @param minMax
     *                The current min and maximum values for each of the
     *                dimensions of the current space.
     * @param data
     *                All the data that is going to be processed
     * @param SNo
     *                The current space number (used as an array of 1 elmenet so
     *                that it can be seen by all the other recursion branches)
     * @param ran
     *                A random number generator
     * @param center
     *                Calculated center of the space.
     * @throws OBException
     *                 User generated exception
     */
    protected void spaceDivision(final SpaceTree node, final int currentLevel,
            final double[][] minMax, final LongArrayList data, final int[] SNo,
            final Random ran, final double[] center) throws OBException {
        if (logger.isDebugEnabled()) {
            logger.debug("Dividing space, level:" + currentLevel
                    + " data size: " + data.size());
        }

        try {
            if (!(node instanceof SpaceTreeLeaf)) {
                // initialize clustering algorithm
                assert node instanceof SpaceTreeNode;
                double[][] centers = kMeans(data, (byte) 2);

                // assert centers.numInstances() == 2 : "Centers found: " +
                // centers.numInstances();
                double[] CL = centers[0];
                double[] CR = centers[1];
                short DD = dividingDimension(CL, CR);
                double DV = ((CR[DD] + CL[DD]) / 2);
                assert DV != 0f;
                assert DV != 1f;
                if (logger.isDebugEnabled()) {
                    logger.debug("Details:" + currentLevel + " DD: " + DD
                            + " DV " + DV);
                }

                // Create sub-spaces
                LongArrayList SL = new LongArrayList(data.size());
                LongArrayList SR = new LongArrayList(data.size());

                // update space boundaries
                double[][] minMaxLeft = cloneMinMax(minMax);
                double[][] minMaxRight = cloneMinMax(minMax);

                minMaxLeft[DD][MAX] = DV;
                // assert DV >= minMaxLeft[DD][MIN];
                assert minMaxLeft[DD][MIN] < minMaxLeft[DD][MAX];
                // assert DV >= minMaxRight[DD][MAX];
                minMaxRight[DD][MIN] = DV;

                assert minMaxRight[DD][MIN] < minMaxRight[DD][MAX];
                // assert DV >= minMaxRight[DD][MIN];

                // Divide the elements of the original space
                divideSpace(data, SL, SR, DD, DV);
                assert data.size() == SL.size() + SR.size();

                SpaceTree leftNode = null;
                SpaceTree rightNode = null;

                SpaceTreeNode ntemp = (SpaceTreeNode) node;

                boolean nextIterationIsNotLeafLeft = currentLevel < (od - 1)
                        && !shallWeStop(SL.size());

                boolean nextIterationIsNotLeafRight = currentLevel < (od - 1)
                        && !shallWeStop(SR.size());

                double[] medianCenterLeft = null;
                double[] medianCenterRight = null;

                if (nextIterationIsNotLeafLeft) {
                    leftNode = new SpaceTreeNode(CL);
                } else {
                    leftNode = new SpaceTreeLeaf(CL);
                    medianCenterLeft = calculateCenter(SL, DD, DV, true);
                }

                if (nextIterationIsNotLeafRight) {
                    rightNode = new SpaceTreeNode(CR);
                } else {
                    rightNode = new SpaceTreeLeaf(CR);
                    medianCenterRight = calculateCenter(SR, DD, DV, false);
                }

                ntemp.setDD(DD);
                ntemp.setDV(DV);
                ntemp.setLeft(leftNode);
                ntemp.setRight(rightNode);

                spaceDivision(leftNode, currentLevel + 1, minMaxLeft, SL, SNo,
                        ran, medianCenterLeft);
                spaceDivision(rightNode, currentLevel + 1, minMaxRight, SR,
                        SNo, ran, medianCenterRight);

            } else { // leaf node processing
                if (logger.isDebugEnabled()) {
                    logger.debug("Found Space:" + SNo[0] + " data size: "
                            + data.size());
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
     *                Each turned bit of the given cluster is an object ID in B
     * @param k
     *                the number of clusters to generate
     * @param ran
     *                Random number generator
     * @return The centroids of the clusters
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws KMeansException
     *                 If k-means++ fails to find clusters
     */
   
    private double[][] kMeans(final LongArrayList cluster, final byte k)
            throws DatabaseException, OutOfRangeException, KMeansException, OBException {
        double[] squaredErrorRes = new double[1];
        double[][] res = null;
        double best = Double.MAX_VALUE;
        boolean bestKMeansPP = true;
        OBRandom yay = new OBRandom();
        int i = 0;
        // find the best k=means pair
        while (i < kMeansIterations) {
            int tries = 0;
            boolean kmeansPP = true;
            
            
            try {
                squaredErrorRes[0] = 0;
                double[][] temp = kMeansAux(cluster, k, tries, squaredErrorRes);
                //logger.debug(" squared error: " + squaredErrorRes[0] + " ++?: "
                //        + kmeansPP);
                assert squaredErrorRes[0] < Float.MAX_VALUE : "Size: "
                        + squaredErrorRes[0];
                if (squaredErrorRes[0] < best) {
                    res = temp;
                    best = squaredErrorRes[0];
                    bestKMeansPP = kmeansPP;
                }
                i++;
            } catch (KMeansHungUpException e) {
                // if we could not converge, then we have to
                // retry the clustering again
            }
        }
        logger.debug("Best: " + best + " ++? " + bestKMeansPP);
        if (bestKMeansPP) {
            kmeansPPGood++;
        } else {
            kmeansGood++;
        }
        return res;
    }

    public int kmeansGood = 0;

    public int kmeansPPGood = 0;

    /**
     * Executes k-means, keeps a count of the number of iterations performed...
     * if clustering cannot converge properly, then we execute the randomized
     * initialization procedure.
     * @param cluster
     *                BitSet with the elements of the current data set
     * @param k
     *                Number of clusters to generate
     * @param iteration
     *                Number of iterations
     * @return An arrays of arrays of k+1 elements. The first two elements are
     *         the centers of the k clusters found and the last element is a one
     *         element array that holds the value of the squared error function.
     *         This value can be used to decide which is the best set of
     *         centroids from several iterations.
     * @throws KMeansException
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws KMeansException
     *                 If k-means++ fails to find clusters
     */
    // TODO: improve the way we represent the data. Instead of having
    // two huge vectors with each of the elements, we could have one byte vector
    // whose elements point to the cluster the element belongs to.
    private double[][] kMeansAux(final LongArrayList cluster, final byte k,
            final int iteration, double[] squaredErrorRes)
            throws DatabaseException, OutOfRangeException, KMeansException,
            KMeansHungUpException, OBException {
        if (cluster.size() <= 1) {
            throw new KMeansException(
                    "Cannot cluster spaces with one or less elements. Found elements: "
                            + cluster.size());
        }
        double[][] centroids = new double[k][getPivotCount()];
        if (iteration < kMeansPPTree) {
            initializeKMeansPP(cluster, k, centroids);
        } else {
            initializeKMeans(cluster, k, centroids);
        }
        TLongHashSet selection[] = initSubClusters(cluster, k);

        assert centroids.length == k;
        boolean modified = true;
        double[] tempTuple = new double[getPivotCount()];
        while (modified) { // while there have been modifications
            int card = 0;
            modified = false;
            // we will put here all the averages used to calculate the new
            // cluster
            double[][] averages = new double[k][getPivotCount()];
            while (card < cluster.size()) {
                // find the closest point
                long index = cluster.get(card);
                // get the tuple
                tempTuple = readFromB(index);
                // find the closest spot
                byte closest = closest(tempTuple, centroids);
                // check if the closest cluster is still the same
                if (!selection[closest].contains(index)) {
                    modified = true;
                    // set the correct cluster where our item belongs
                    updateClusterInfo(closest, selection, index);
                }
                updateAveragesInfo(closest, tempTuple, averages);
                card++;
            }

            // after finishing recalculating the pivots, we just have to
            // center the clusters
            if (modified) {
                centerClusters(centroids, averages, selection);
            }
        }
        // calculate the squared error function.
        int card = 0;

        double squaredError = 0;
        while (card < cluster.size()) {

            long index = cluster.get(card);
            // get the tuple
            tempTuple = readFromB(index);
            // find the closest spot
            byte closest = closest(tempTuple, centroids);
            double x = squareDistance(tempTuple, centroids[closest]);
            assert !Double.isNaN(x) : "Calculated: "
                    + Arrays.toString(tempTuple) + " , "
                    + Arrays.toString(centroids[closest]);
            squaredError += x;
            card++;
        }

        squaredErrorRes[0] = squaredError / cluster.size();
        return centroids;
    }

    /**
     * Find the centroids.
     * @param centroids
     *                Result is left here...
     * @param averages
     *                Average for each dimension
     * @param selection
     *                Current list of clusters
     * @throws KMeansException
     *                 if any of the selections have zero elements.
     */
    private void centerClusters(final double[][] centroids,
            final double[][] averages, final TLongHashSet selection[])
            throws KMeansHungUpException {
        byte i = 0;
        assert centroids.length == averages.length
                && centroids.length == selection.length;
        while (i < averages.length) {
            int cx = 0;
            // assert selection[i].size() != 0;
            while (cx < getPivotCount()) {
                if (selection[i].size() == 0) {
                    throw new KMeansHungUpException();
                }
                centroids[i][cx] = averages[i][cx] / selection[i].size();
                cx++;
            }
            i++;
        }
    }

    /**
     * Adds the contents of tuple to averages[cluster].
     * @param cluster
     *                Cluster to process
     * @param tuple
     *                Tuple to add
     * @param averages
     *                The result will be stored here.
     */
    private void updateAveragesInfo(final byte cluster, final double[] tuple,
            final double[][] averages) {
        int i = 0;
        while (i < getPivotCount()) {
            averages[cluster][i] += tuple[i];
            i++;
        }
    }

    /**
     * Sets the ith element in selection[cluster] and set the ith bit in the
     * other clusters to 0.
     * @param cluster
     *                The cluster that will be set
     * @param element
     *                Elemenet id
     * @param selection
     *                The cluster we will set.
     */
    private void updateClusterInfo(final byte cluster,
            final TLongHashSet[] selection, final long element) {
        byte i = 0;
        while (i < selection.length) {
            if (i == cluster) {
                selection[i].add(element);
            } else {
                selection[i].remove(element);
            }
            i++;
        }
    }

    /**
     * Finds the centroid which is closest to tuple.
     * @param tuple
     *                The tuple to process
     * @param centroids
     *                A list of centroids
     * @return A byte indicating which is the closest centroid to the given
     *         tuple.
     */
    private byte closest(final double[] tuple, final double[][] centroids) {
        byte i = 0;
        byte res = 0;
        double value = Float.MAX_VALUE;
        while (i < centroids.length) {
            double temp = squareDistance(tuple, centroids[i]);
            if (temp < value) {
                value = temp;
                res = i;
            }
            i++;
        }
        return res;
    }

    /**
     * Computes the squared distance for the given tuples.
     * @param a
     *                tuple
     * @param b
     *                tuple
     * @return squared distance
     */
    public static final double squareDistance(final double[] a, final double[] b) {
        assert a.length == b.length;
        int i = 0;
        double res = 0;
        while (i < a.length) {
            double t = a[i] - b[i];
            res += t * t;
            i++;
        }
        return res;
    }

    /**
     * Initializes k cluster based on cluster
     * @param cluster
     *                Reference cluster
     * @param k
     *                number of clusters to generate
     * @return An array of clusters with the size of cluster
     */
    private TLongHashSet[] initSubClusters(final LongArrayList cluster,
            final byte k) {
        TLongHashSet[] res = new TLongHashSet[k];
        byte i = 0;
        while (i < k) {
            res[i] = new TLongHashSet(cluster.size());
            i++;
        }
        return res;
    }

    @Override
    public long totalBoxes() {
        return totalBoxes;
    }

    /**
     * Initializes k centroids (Default method).
     * @param cluster
     *                original cluster
     * @param k
     *                number of clusters
     * @param centroids
     *                Centroids that will be generated
     * @param r
     *                A random function
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB.
     */
    private void initializeKMeans(final LongArrayList cluster, final byte k,
            final double[][] centroids) throws DatabaseException,
            OutOfRangeException, OBException {
        int total = cluster.size();
        OBRandom r = new OBRandom();
        byte i = 0;
        long centroidIds[] = new long[k];
        while (i < k) {
            int t;
            long id;
            do {
                t = r.nextInt(total);
                // we should actually return the tth element
                id = cluster.get(t);
            } while (id == -1 || contains(id, centroidIds, i));

            centroidIds[i] = id;
            // TODO: check this statement:
            centroids[i] = readFromB(id);
            i++;
        }
    }

    /**
     * Initializes k centroids by using k-means++ leaves the result in
     * "centroids" The original paper is here: David Arthur and Sergei
     * Vassilvitskii, "k-means++: The Advantages of Careful Seeding" SODA 2007.
     * This method was inspired from the source code provided by the authors
     * @param cluster
     *                Cluster to initialize
     * @param k
     *                Number of centroids
     * @param centroids
     *                The resulting centroids
     * @param r
     *                A random number generator.
     * @throws DatabaseException
     *                 If somehing goes wrong with the DB
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */
    private void initializeKMeansPP(final LongArrayList cluster, final byte k,
            final double[][] centroids) throws DatabaseException,
            OutOfRangeException, OBException {

        OBRandom r = new OBRandom();
        double potential = 0;

        long centroidIds[] = new long[k]; // keep track of the selected centroids
        double[] closestDistances = new double[cluster.size()];
        double[] tempA = new double[getPivotCount()];
        double[] tempB = new double[getPivotCount()];

        // Randomly select one center

        int index = (int)cluster.get(r.nextInt(cluster.size()));
        int currentCenter = 0;
        centroidIds[currentCenter] = index;
        centroids[currentCenter] = readFromB(index);
        int i = 0;
        while (i < cluster.size()) {
            long t = cluster.get(i);
            tempA = readFromB(t);
            closestDistances[i] = squareDistance(tempA,
                    centroids[currentCenter]);
            potential += closestDistances[i];
            i++;
        }

        // Choose the remaining k-1 centers
        int centerCount = 1;
        while (centerCount < k) {

            // Repeat several times
            double bestPotential = -1;
            int bestIndex = -1;
            for (int retry = 0; retry < kMeansPPRetries; retry++) {

                // choose the new center
                double probability = r.nextFloat() * potential;
                for (index = 0; index < cluster.size(); index++) {

                    if (contains(cluster.get(index), centroidIds, centerCount)) {
                        continue;
                    }

                    if (probability <= closestDistances[index])
                        break;
                    else
                        probability -= closestDistances[index];
                }
                // if we did not find any proper index, we assign a random one
                if (index == cluster.size()) {
                    do {
                        index = r.nextInt(cluster.size());
                    } while (contains(cluster.get(index), centroidIds,
                            centerCount));
                }

                // Compute the new potential
                double newPotential = 0;
                tempB = readFromB(cluster.get(index));
                for (i = 0; i < cluster.size(); i++) {
                    long t = cluster.get(i);
                    tempA = readFromB(t);
                    newPotential += Math.min(squareDistance(tempA, tempB),
                            closestDistances[i]);
                }

                // Store the best result
                if (bestPotential < 0 || newPotential < bestPotential) {
                    bestPotential = newPotential;
                    bestIndex = index;
                }
            }

            assert !contains(cluster.get(bestIndex), centroidIds, centerCount) : "The id: "
                    + cluster.get(bestIndex)
                    + " was found here: "
                    + Arrays.toString(centroidIds) + " max: " + centerCount;

            // Add the appropriate center
            centroidIds[centerCount] = bestIndex;
            centroids[centerCount] = readFromB(cluster.get(bestIndex));
            potential = bestPotential;
            tempB = readFromB(cluster.get(bestIndex));
            for (i = 0; i < cluster.size(); i++) {
                long t = cluster.get(i);
                tempA = readFromB(t);
                closestDistances[i] = Math.min(squareDistance(tempA, tempB),
                        closestDistances[i]);
            }
            // make sure that the same center is not found
            centerCount++;
        }
    }

    /**
     * Returns the ith set bit of the given cluster.
     * @param cluster
     *                the cluster to be processed
     * @param i
     *                the ith set bit
     * @return the ith set bit of the cluster
     */
    private int returnIth(final BitSet cluster, final int i) {
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
     *                an identification
     * @param ids
     *                a list of numbers
     * @param max
     *                the maximum point that we will process
     * @return true if id is in the array ids
     */
    private boolean contains(final long id, final long[] ids, final int max) {
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
     *                object internal id
     * @param tuple
     *                store the corresponding tuple here.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     */
    protected abstract double[] readFromB(long id) throws DatabaseException,
            OutOfRangeException, OBException;

    /**
     * Verifies that all the data that is going to be inserted in this leaf
     * belongs to the given leaf.
     * @param instances
     *                Set if data that will be verified
     * @param n
     *                Leaf that will be processed.
     * @throws OutOfRangeException
     *                 If the distance of any object to any other object exceeds
     *                 the range defined by the user.
     * @throws DatabaseException
     *                 If something goes wrong with the DB
     * @return if the data is valid
     */
    protected boolean verifyData(final LongArrayList instances,
            final SpaceTreeLeaf n) throws OutOfRangeException,
            DatabaseException, OBException {
        int i = 0;
        boolean res = true;
        double[] tempTuple = new double[getPivotCount()];
        while (i < instances.size() && res) {
            long t = instances.get(i);
            tempTuple = this.readFromB(t);
            res = n.pointInside(tempTuple);

            assert res : Arrays.toString(tempTuple) + " is not inside: " + n;
             
            i++;
        }
        return res;
    }

    /**
     * Computes the euclidean distance for the given tuples.
     * @param a
     *                tuple
     * @param b
     *                tuple
     * @return euclidean distance
     */
    private double euclideanDistance(final double[] a, final double[] b) {
        int i = 0;
        double res = 0;
        while (i < getPivotCount()) {
            double t = a[i] - b[i];
            res += t * t;
            i++;
        }
        return (double) Math.sqrt(res);
    }

    /**
     * Calculates the center of the given data based on medians (just like the
     * extended pyramid technique).
     * @param data
     *                data to be processed
     * @return the center of the given data
     */
    protected final double[] calculateCenter(final LongArrayList data, int DD,
            double DV, boolean left) throws DatabaseException,
            OutOfRangeException, KMeansException, OBException {

        QuantileBin1D[] medianHolder = createMedianHolders(data.size());
        int i = 0;
        double[] tempTuple = new double[getPivotCount()];
        while (i < data.size()) {
            long t = data.get(i);
            tempTuple = this.readFromB(t);
            assert ((tempTuple[DD] < DV) || !left)
                    && ((tempTuple[DD] >= DV) || left);
            super.updateMedianHolder(tempTuple, medianHolder);
            i++;
        }

        // now we just have to get the medians
        i = 0;
        double[] res = new double[getPivotCount()];
        while (i < getPivotCount()) {
            res[i] = (double) medianHolder[i].median();
            // res[i] = (double) data.meanOrMode(i);
            i++;
        }
        return res;

    }

    /**
     * Clone the given double[][] array.
     * @param minMax
     *                Input array
     * @return a clone of minMax
     */
    private final double[][] cloneMinMax(final double[][] minMax) {
        double[][] res = new double[getPivotCount()][2];
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
     *                double[][] vector that will be initialized.
     */
    private void initMinMax(final double[][] data) {
        int cx = 0;
        assert data.length == getPivotCount();
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
     *                original data set
     * @param left
     *                items to the left of the division (output argument)
     * @param right
     *                items to the right of the division (output argument)
     * @param DD
     *                See the P+tree paper
     * @param DV
     *                See the P+tree paper
     */
    protected final void divideSpace(final LongArrayList original,
            LongArrayList left, LongArrayList right, final int DD, final double DV)
            throws OutOfRangeException, DatabaseException, OBException {
        int i = 0;
        double[] tempTuple = new double[getPivotCount()];
        while (i < original.size()) {
            long t = original.get(i);
            tempTuple = this.readFromB(t);
            if (tempTuple[DD] < DV) {
                left.add(t);
            } else {
                right.add(t);
            }
            i++;
        }
    }

    /**
     * Calculate the dividing dimension for cl and cr.
     * @param cl
     *                left center
     * @param cr
     *                right center
     * @return the dimension that has the biggest gap between cl and cr
     */
    protected final short dividingDimension(final double[] cl, final double[] cr) {
        int res = 0;
        int i = 0;
        double max = Double.MIN_VALUE;
        while (i < getPivotCount()) {
            double current = Math.abs(cl[i] - cr[i]);
            if (current > max) {
                max = current;
                res = i;
            }
            i++;
        }
        return (short) res;
    }

    /**
     * Please see {@link #kMeansPPRetries}.
     */
    public int getKMeansPPRetries() {
        return kMeansPPRetries;
    }

    /**
     * Please see {@link #kMeansPPRetries}.
     */
    public void setKMeansPPRetries(int meansPPRetries) {
        kMeansPPRetries = meansPPRetries;
    }

    /**
     * Please see {@link #minElementsPerSubspace}.
     * @return {@link #minElementsPerSubspace}
     */
    public int getMinElementsPerSubspace() {
        return minElementsPerSubspace;
    }

    /**
     * Please see {@link #minElementsPerSubspace}.
     */
    public void setMinElementsPerSubspace(int minElementsPerSubspace) {
        this.minElementsPerSubspace = minElementsPerSubspace;
    }

    /**
     * Please see {@link #kMeansIterations}.
     * @return {@link #kMeansIterations}
     */
    public int getKMeansIterations() {
        return kMeansIterations;
    }

    /**
     * Please see {@link #kMeansIterations}.
     * @param meansIterations
     */
    public void setKMeansIterations(int meansIterations) {
        kMeansIterations = meansIterations;
    }

}
