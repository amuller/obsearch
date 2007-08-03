package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.Iterator;

import hep.aida.bin.QuantileBin1D;

import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.SortedFloatBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
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
 * Class: ExtendedPyramidIndexShort
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */

public class ExtendedPyramidIndexShort<O extends OBShort> extends
		AbstractExtendedPyramidIndex<O> implements IndexShort<O> {

	private short minInput;

	private short maxInput;

	/**
	 * Creates a new ExtendedPyramidIndexShort. Ranges accepted by this pyramid
	 * will be between 0 and Short.MAX_VALUE
	 * 
	 * @param databaseDirectory
	 * @param pivots
	 * @throws DatabaseException
	 * @throws IOException
	 */
	public ExtendedPyramidIndexShort(final File databaseDirectory,
			final short pivots) throws DatabaseException, IOException {

		this(databaseDirectory, pivots, (short) 0, Short.MAX_VALUE);
	}

	/**
	 * Creates a new ExtendedPyramidIndexShort. Ranges accepted by this pyramid
	 * will be defined by the user. We recommend the use of this constructor. We
	 * believe it will give better resolution to the float transformation.
	 * 
	 * @param databaseDirectory
	 * @param pivots
	 * @param minInput
	 * @param maxInput
	 * @throws DatabaseException
	 * @throws IOException
	 */
	public ExtendedPyramidIndexShort(final File databaseDirectory,
			final short pivots, short minInput, short maxInput)
			throws DatabaseException, IOException {
		super(databaseDirectory, pivots);
		this.minInput = minInput;
		this.maxInput = maxInput;
		assert minInput < maxInput;
	}

	@Override
	public String getSerializedName() {
		return "ExtendedPyramidTechniqueShort";
	}

	@Override
	protected float[] extractTuple(TupleInput in) throws OutOfRangeException {
		int i = 0;
		float[] res = new float[pivotsCount];
		while (i < pivotsCount) {
			res[i] = normalize(in.readShort());
			i++;
		}
		return res;
	}

	public boolean intersects(O object, short r, int box)
			throws NotFrozenException, DatabaseException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		short[] t = new short[pivotsCount];
		calculatePivotTuple(object, t); // calculate the pivot for the given
		float[][] q = new float[pivotsCount][2]; // rectangular query
		generateRectangle(t, r, q);
		float[] lowHighResult = new float[2];
		return intersect(q, box, lowHighResult);
	}

	public int [] intersectingBoxes(O object, short r)
			throws NotFrozenException, DatabaseException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		BitSet result = new BitSet(super.totalBoxes());
		short[] t = new short[pivotsCount];
		calculatePivotTuple(object, t); // calculate the pivot for the given
		int pyramidCount = 2 * pivotsCount;
		float[][] qorig = new float[pivotsCount][2]; // rectangular query
		float[][] q = new float[pivotsCount][2]; // rectangular query
		short myr = r;
		float[] lowHighResult = new float[2];
		generateRectangle(t, myr, qorig);
		int i = 0;
		while (i < pyramidCount) {
			copyQuery(qorig, q);
			if (intersect(q, i, lowHighResult)) {
				result.set(i);
			}
			i++;
		}
		int [] res = new int[result.cardinality()];
		i = 0;
		int index = 0;
		while(i < res.length){
			index = result.nextSetBit(index);
			res[i] = index;
			index++;
			i++;
		}
		return res;
	}

	public void searchOB(O object, short r, OBPriorityQueueShort<O> result)
			throws NotFrozenException, DatabaseException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		// check if we are frozen
		assertFrozen();

		short[] t = new short[pivotsCount];
		calculatePivotTuple(object, t); // calculate the pivot for the given
		// object

		int pyramidCount = 2 * pivotsCount;
		float[][] qorig = new float[pivotsCount][2]; // rectangular query
		float[][] q = new float[pivotsCount][2]; // rectangular query
		short myr = r;
		generateRectangle(t, myr, qorig);
		int i = 0;
		float[] lowHighResult = new float[2];
		// TODO: select the pyramids randomly just like quicksort
		while (i < pyramidCount) {
			copyQuery(qorig, q);
			if (intersect(q, i, lowHighResult)) {
				searchBTreeAndUpdate(object, t, myr, i + lowHighResult[HLOW], i
						+ lowHighResult[HHIGH], result);
				short nr = result.updateRange(myr);
				if (nr < myr) {
					myr = nr;
					// regenerate the query with a smaller range
					generateRectangle(t, myr, qorig);
				}
			}
			i++;
		}
	}

	public void searchOB(O object, short r, OBPriorityQueueShort<O> result,
			int [] boxes) throws NotFrozenException, DatabaseException,
			InstantiationException, IllegalIdException, IllegalAccessException,
			OutOfRangeException, OBException {
		// check if we are frozen
		assertFrozen();
		BitSet boxesBit = new BitSet();
		int i = 0;
		while(i < boxes.length){
			boxesBit.set(boxes[i]);
			i++;
		}
		short[] t = new short[pivotsCount];
		calculatePivotTuple(object, t); // calculate the pivot for the given
		// object

		int pyramidCount = 2 * pivotsCount;
		float[][] qorig = new float[pivotsCount][2]; // rectangular query
		float[][] q = new float[pivotsCount][2]; // rectangular query
		short myr = r;
		generateRectangle(t, myr, qorig);
		i = 0;
		float[] lowHighResult = new float[2];
		// TODO: select the pyramids randomly just like quicksort
		while (i < pyramidCount) {
			copyQuery(qorig, q);
			if (intersect(q, i, lowHighResult) && boxesBit.get(i)) {
				searchBTreeAndUpdate(object, t, myr, i + lowHighResult[HLOW], i
						+ lowHighResult[HHIGH], result);
				short nr = result.updateRange(myr);
				if (nr < myr) {
					myr = nr;
					// regenerate the query with a smaller range
					generateRectangle(t, myr, qorig);
				}
			}
			i++;
		}
	}

	/**
	 * This method reads from the B-tree appies l-infinite to discard false
	 * positives Calculates the real distance and updates the result priority
	 * queue It is left public so that junit can perform validations on it
	 * Performance-wise this is one of the most important methods
	 * 
	 * @param object
	 * @param tuple
	 * @param r
	 * @param hlow
	 * @param hhigh
	 * @param result
	 * @throws DatabaseException
	 */
	public void searchBTreeAndUpdate(O object, short[] tuple, short r,
			float hlow, float hhigh, OBPriorityQueueShort<O> result)
			throws DatabaseException, IllegalAccessException,
			InstantiationException, IllegalIdException, OBException {

		Cursor cursor = null;

		try {

			DatabaseEntry keyEntry = new DatabaseEntry();
			DatabaseEntry dataEntry = new DatabaseEntry();
			cursor = cDB.openCursor(null, null);
			SortedFloatBinding.floatToEntry(hlow, keyEntry);
			OperationStatus retVal = cursor.getSearchKeyRange(keyEntry,
					dataEntry, null);

			if (retVal == OperationStatus.NOTFOUND) {
				return;
			}

			if (cursor.count() > 0) {
				float currentPyramidValue = SortedFloatBinding
						.entryToFloat(keyEntry);
				short max = Short.MIN_VALUE;
				short realDistance = Short.MIN_VALUE;
				while (retVal == OperationStatus.SUCCESS
						&& currentPyramidValue <= hhigh) {

					TupleInput in = new TupleInput(dataEntry.getData());

					int i = 0;
					short t;
					max = Short.MIN_VALUE;
					while (i < tuple.length) {
						t = (short) Math.abs(tuple[i] - in.readShort());
						if (t > max) {
							max = t;
							if (t > r) {
								break; // finish this loop this slice won't be
								// matched
								// after all!
							}
						}
						i++;
					}
					if (max <= r && result.isCandidate(max)) {
						// there is a chance it is a possible match
						int id = in.readInt();
						O toCompare = super.getObject(id);
						realDistance = object.distance(toCompare);
						if (realDistance <= r) {
							result.add(id, toCompare, realDistance);
						}
					}

					// read the next record
					retVal = cursor.getNext(keyEntry, dataEntry, null);
					// update the current pyramid value so that we know when to
					// stop
					currentPyramidValue = SortedFloatBinding
							.entryToFloat(keyEntry);
				}
			}
		} finally {
			cursor.close();
		}
	}

	/**
	 * Generates min and max for the given tuple It is actually the query. If
	 * you want non-rectangular queries you have to override this method and
	 * make sure your modification works well with searchOB
	 * 
	 * @param t
	 *            the tuple to be processed
	 * @param r
	 *            the range
	 * @param q
	 *            resulting rectangle query
	 */

	protected void generateRectangle(short[] t, short r, float[][] q)
			throws OutOfRangeException {
		// range
		int i = 0;
		while (i < q.length) { //
			q[i][MIN] = extendedPyramidNormalization(normalize((short) Math
					.max(t[i] - r, minInput)), i);
			q[i][MAX] = extendedPyramidNormalization(normalize((short) Math
					.min(t[i] + r, maxInput)), i);
			i++;
		}
		centerQuery(q);
	}

	/**
	 * Method that takes the values already calculated in B and puts them into C
	 * This is to save some time when rebuilding the index
	 */
	@Override
	protected void insertFromBtoC() throws DatabaseException,
			OutOfRangeException {

		DatabaseEntry foundKey = new DatabaseEntry();
		DatabaseEntry foundData = new DatabaseEntry();
		Cursor cursor = null;
		long count = super.bDB.count();
		short[] t = new short[pivotsCount];
		try {
			int i = 0;
			cursor = bDB.openCursor(null, null);
			MyTupleInput in = new MyTupleInput();
			while (cursor.getNext(foundKey, foundData, LockMode.DEFAULT) == OperationStatus.SUCCESS) {
				assert i == IntegerBinding.entryToInt(foundKey);
				// i contains the actual id of the tuple
				in.setBuffer(foundData.getData());
				int cx = 0;
				while (cx < t.length) {
					t[cx] = in.readShort();
					cx++;
				}
				this.insertFrozenAux(t, i);
				i++;
			}
			// Size reported by the DB and the items
			// we read should be the same
			assert i == count;

		} finally {
			cursor.close();
		}

	}

	/**
	 * Transforms the given tuple into an extended pyramid technique normalized
	 * value that considers the "center" of the dimension
	 * 
	 * @param tuple
	 *            The original tuple in the default dimension
	 * @param result
	 *            the resulting normalized tuple is left here
	 */
	protected float[] extendedPyramidTransform(final short[] tuple)
			throws OutOfRangeException {
		int i = 0;
		float[] result = new float[tuple.length];
		assert tuple.length == result.length;
		while (i < tuple.length) {
			float norm = normalize(tuple[i]);
			float norm2 = extendedPyramidNormalization(norm, i);
			result[i] = norm2;
			i++;
		}
		return result;
	}

	/**
	 * Normalize the given value
	 * 
	 * @param x
	 * @return the normalized value
	 */
	protected float normalize(short x) throws OutOfRangeException {
		if (x < minInput || x > maxInput) {
			throw new OutOfRangeException(minInput + "", maxInput + "", "" + x);
		}
		return ((float) (x - minInput)) / ((float) (maxInput - minInput));
	}

	@Override
	protected byte insertFrozen(O object, int id) throws IllegalIdException,
			OBException, DatabaseException, IllegalAccessException,
			InstantiationException {
		short[] t = new short[pivotsCount];
		calculatePivotTuple(object, t); // calculate the tuple for the new //

		return insertFrozenAux(t, id);
	}

	public int getBox(O object) throws OBException {
		short[] t = new short[pivotsCount];
		calculatePivotTuple(object, t); // calculate the tuple for the new //
		float[] et = extendedPyramidTransform(t);
		return super.pyramidOfPoint(et);
	}

	/**
	 * Inserts the given tuple and id into C
	 * 
	 * @param t
	 * @param id
	 * @return
	 * @throws OutOfRangeException
	 * @throws DatabaseException
	 */
	protected byte insertFrozenAux(short[] t, int id)
			throws OutOfRangeException, DatabaseException {
		float[] et = extendedPyramidTransform(t);
		float pyramidValue = pyramidValue(et);

		DatabaseEntry keyEntry = new DatabaseEntry();
		DatabaseEntry dataEntry = new DatabaseEntry();
		TupleOutput out = new TupleOutput();
		// write the tuple
		for (short d : t) {
			out.writeShort(d);
		}
		// write the object's id
		out.writeInt(id);
		// create the key
		SortedFloatBinding.floatToEntry(pyramidValue, keyEntry);
		dataEntry.setData(out.getBufferBytes());

		// TODO: check the status result of all the operations
		if (cDB.put(null, keyEntry, dataEntry) != OperationStatus.SUCCESS) {
			throw new DatabaseException();
		}
		return 1;
	}

	@Override
	protected void insertInB(int id, O object) throws OBException,
			DatabaseException {
		DatabaseEntry keyEntry = new DatabaseEntry();
		TupleOutput out = new TupleOutput();
		short[] tuple = new short[pivotsCount];
		calculatePivotTuple(object, tuple);
		// write the tuple
		for (short d : tuple) {
			out.writeShort(d);
		}
		// store the ID
		IntegerBinding.intToEntry(id, keyEntry);
		insertInDatabase(out, keyEntry, bDB);
	}

	@Override
	protected Index returnSelf() {
		return this;
	}

	/**
	 * Calculates the tuple vector for the given object
	 * 
	 * @param obj
	 *            object to be processed
	 * @param tuple
	 *            The resulting tuple will be stored here
	 */
	protected void calculatePivotTuple(final O obj, short[] tuple)
			throws OBException {
		assert tuple.length == this.pivotsCount;
		int i = 0;
		while (i < tuple.length) {
			tuple[i] = obj.distance(this.pivots[i]);
			i++;
		}
	}

	public boolean exists(O object) throws DatabaseException, OBException,
			IllegalAccessException, InstantiationException {
		OBPriorityQueueShort<O> result = new OBPriorityQueueShort<O>((byte) 1);
		// perform a search with r==0 and k==1
		// TODO: this short must not be replaced in the template, it is always
		// short
		searchOB(object, (short) 0, result);
		if (result.getSize() == 1) {
			Iterator<OBResultShort<O>> it = result.iterator();
			assert it.hasNext();
			OBResultShort<O> r = it.next();
			return object.equals(r.getObject());
		} else {
			return false;
		}
	}

}
