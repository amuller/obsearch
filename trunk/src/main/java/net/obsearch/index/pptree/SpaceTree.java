package net.obsearch.index.pptree;

import java.util.List;

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
 * A Space Tree is like a KDB-tree. It is used to partition the space and
 * improve the efficiency of the pyramid technique.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public interface SpaceTree {

    /**
     * Index used when accessing tuples of 2 items.
     */
    int MIN = 0;

    /**
     * Index used when accessing tuples of 2 items.
     */
    int MAX = 1;

    /**
     * Determines if the node is a leaf or not.
     * @return true if the node is a leaf.
     */
    boolean isLeafNode();

    /**
     * Determines the subspace for the given value I know it is funny that the
     * interface specifies a SpaceTreeLeaf... Is this good? Is this bad? Please
     * post your comments. I just felt that it helps to document better... but
     * it is cyclic.
     * @param tuple
     *                The tuple that will be searched
     * @return The leaf where this tuple is stored.
     */
    SpaceTreeLeaf search(double[] tuple);

    /**
     * Returns the center of the underlying space.
     * @return The center of the space.
     */
    double[] getCenter();

    /**
     * Takes a query rectangle and returns all the spaces that intersect with
     * it. The list should be empty the first time you call this method
     * @param query
     *                the query to be searched
     * @param center
     *                Holds the center of the query (original object's
     *                coordinates)
     * @param result
     *                will hold all the spaces that intersect with the query
     */
    void searchRange(double[][] query, double[] center,
            List < SpaceTreeLeaf > result);

    /**
     * Returns the space tree with the number spaceNumber.
     * @see #search(float[])
     * @param spaceNumber
     *                The space number to search.
     * @return The space with number spaceNumber
     */
    SpaceTreeLeaf searchSpace(int spaceNumber);
}
