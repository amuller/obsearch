package net.obsearch.pivots.perm;

import hep.aida.bin.StaticBin1D;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.perm.CompactPerm;
import net.obsearch.index.perm.PermProjection;
import net.obsearch.index.perm.impl.PerDouble;
import net.obsearch.pivots.AbstractIncrementalPivotSelector;
import net.obsearch.pivots.PivotResult;
import net.obsearch.pivots.Pivotable;

import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

import cern.colt.list.IntArrayList;
import cern.colt.list.LongArrayList;

/*
 OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
 Copyright (C) 2008 Arnoldo Jose Muller Molina

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
 * IncrementalBustosNavarroChavez implements the pivot selection described here:
 * Pivot Selection Techniques for Proximity Searching in Metric Spaces (2001)
 * Benjamin Bustos, Gonzalo Navarro, Edgar Chavez The idea was also suggested by
 * Zezula et all in their book "Similarity Search: The Metric Space Approach"
 * 
 * @author Arnoldo Jose Muller Molina
 */
public abstract class AbstractIncrementalPerm<O extends OB> extends
		AbstractIncrementalPivotSelector<O> {

	/**
	 * Logger.
	 */
	private static final transient Logger logger = Logger
			.getLogger(AbstractIncrementalPerm.class);

	private int l;

	private int m;

	/**
	 * Receives the object that accepts pivots as possible candidates. Selects l
	 * pairs of objects to compare which set of pivots is better, and selects m
	 * possible pivot candidates from the data set.
	 * 
	 * @param pivotable
	 * @param l
	 * @param m
	 */
	protected AbstractIncrementalPerm(Pivotable<O> pivotable, int l, int m) {
		super(pivotable);
		this.l = l;
		this.m = m;
	}

	private List<PermProjection> initPerms(int dim, int count) {
		List<PermProjection> result = new ArrayList<PermProjection>(count);
		int i = 0;
		while (i < count) {
			result.add(new PermProjection(new CompactPerm(new short[dim]), -1));
			i++;
		}
		return result;
	}

	private List<PermHolderDouble> initPermHolders(int pivotCount, int total) {
		List<PermHolderDouble> res = new ArrayList<PermHolderDouble>(pivotCount);
		int i = 0;
		while (i < total) {
			res.add(new PermHolderDouble(pivotCount));
			i++;
		}
		return res;
	}

	private void updatePerms(O pivot, O[] data, List<PermHolderDouble> perms,
			short pivotIndex) throws OBException {
		int i = 0;
		for (O d : data) {
			double dist = distance(pivot, d);
			perms.get(i).set(pivotIndex, new PerDouble(dist, pivotIndex));
			i++;
		}
	}

	protected abstract double distance(O a, O b) throws OBException;

	@Override
	public PivotResult generatePivots(int pivotCount, LongArrayList elements,
			Index<O> index) throws OBException, IllegalAccessException,
			InstantiationException, OBStorageException,
			PivotsUnavailableException {

		int lLocal = (int) Math.min(l, index.databaseSize());
		int mLocal = (int) Math.min(m, index.databaseSize());
		int max;
		if (elements == null) {
			max = (int) Math.min(index.databaseSize(), Integer.MAX_VALUE);
		} else {
			max = elements.size();
		}
		LongArrayList pivotList = new LongArrayList(pivotCount);
		List<O> pivotListO = new ArrayList<O>(pivotCount);

		Random r = new Random();
		// select m objects from which we will select pivots
		int i = 1;
		O[] data = selectO(lLocal, r, elements, index, null);
		List<PermProjection> projections = initPerms(pivotCount, lLocal);
		List<PermHolderDouble> perms = initPermHolders(pivotCount, lLocal);

		// add the first pivot
		long pivot = select(1, r, elements, index, pivotList)[0];
		pivotList.add(pivot);
		pivotListO.add(index.getObject(pivot));
		// update the perms.
		updatePerms(pivotListO.get(0), data, perms, (short) 0);

		while (i < pivotCount) {

			long[] possiblePivots = select(mLocal, r, elements, index,
					pivotList);
			List<O> pivots = new ArrayList<O>(possiblePivots.length);

			for (long id : possiblePivots) {
				pivots.add(index.getObject(id));
			}
			int cx = 0;
			Score best = null;
			while (cx < pivots.size()) {
				O piv = pivots.get(cx);
				updatePerms(piv, data, perms, (short) i);
				Score score = calculateScore(perms, possiblePivots[cx], pivots
						.get(cx));
				if (best == null || score.isBetter(best)) {
					best = score;
				}
				cx++;
			}
			pivotList.add(best.id);
			pivotListO.add(best.pivot);
			// update the permutations for this level
			updatePerms(best.pivot, data, perms, (short) i);
			logger.info("Best pivot: " + best + " i: " + i + " id: " + best.id);
			i++;
		}

		pivotList.trimToSize();
		long[] result = pivotList.elements();
		return new PivotResult(result);
	}

	private Score calculateScore(List<PermHolderDouble> perms, long id, O pivot) {
		HashSet<PermHolderDouble> set = new HashSet<PermHolderDouble>(perms
				.size());
		for (PermHolderDouble p : perms) {
			set.add(p);
		}
		StaticBin1D stats = new StaticBin1D();
		int i1 = 0;
		while (i1 < (perms.size() - 1)) {
			int i2 = i1 + 1;
			while (i2 < perms.size()) {
				stats.add(perms.get(i1).distance(perms.get(i2)));
				i2++;
			}
			i1++;
		}
		return new Score(set.size(), stats.mean(), id, pivot);
	}

	private class Score {

		private int total;
		private double avgDistance;
		private long id;
		private O pivot;

		public Score(int total, double avgDistance, long id, O pivot) {
			super();
			this.total = total;
			this.avgDistance = avgDistance;
			this.id = id;
			this.pivot = pivot;
		}
		// Sort StaticBin1D
		// 
		public boolean isBetter(Score another) {
			/*if (total == another.total) {
				return avgDistance > another.avgDistance;
			} else {
				return total > another.total;
			}*/
			return avgDistance > another.avgDistance;
		}

		public String toString() {
			return "Tot: " + total + " dis: " + avgDistance;
		}
	}

}
