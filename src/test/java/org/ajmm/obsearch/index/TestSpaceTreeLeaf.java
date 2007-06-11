package org.ajmm.obsearch.index;

import static org.junit.Assert.*;

import org.ajmm.obsearch.index.pptree.SpaceTreeLeaf;
import org.junit.Before;
import org.junit.Test;

public class TestSpaceTreeLeaf {

	private SpaceTreeLeaf leaf;

	@Before
	public void setUp() throws Exception {
		leaf = new SpaceTreeLeaf();
		float[][] space = {{0.5f,1},{0.5f,1}};
		leaf.setMinMax(space);
	}

	@Test
	public void testIntersects() {
		float[][] test = {{-1,0.6f},{-1,0.7f}};
		assertTrue(leaf.intersects(test));
	}

	@Test
	public void testNotIntersects() {
		float[][] test = {{-1,0.4f},{-1,0.3f}};
		assertFalse(leaf.intersects(test));
	}

}
