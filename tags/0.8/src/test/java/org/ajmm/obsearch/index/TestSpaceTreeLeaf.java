package org.ajmm.obsearch.index;

import static org.junit.Assert.*;

import org.ajmm.obsearch.index.pptree.SpaceTreeLeaf;
import org.junit.Before;
import org.junit.Test;

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
 * Test the SpaceTreeLeaf object.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class TestSpaceTreeLeaf {

    /**
     * Leaf that will be tested.
     */
    private SpaceTreeLeaf leaf;

    /**
     * Prepare a space vector to test if a point
     * belongs or not.
     */
    @Before
    public void setUp() throws Exception {
        leaf = new SpaceTreeLeaf();
        float[][] space = { { 0.5f, 1 }, { 0.5f, 1 } };
        leaf.setMinMax(space);
    }

    /**
     * Make sure that intersects works well.
     */
    @Test
    public void testIntersects() {
        float[][] test = { { -1, 0.6f }, { -1, 0.7f } };
        assertTrue(leaf.intersects(test));
    }
    /**
     * Make sure that intersects works well.
     */
    @Test
    public void testNotIntersects() {
        float[][] test = { { -1, 0.4f }, { -1, 0.3f } };
        assertFalse(leaf.intersects(test));
    }

}
