package org.ajmm.obsearch.index;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.IllegalIdException;
import org.ajmm.obsearch.exception.NotFrozenException;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.exception.OutOfRangeException;
import org.ajmm.obsearch.index.pptree.SpaceTreeLeaf;
import org.ajmm.obsearch.index.utils.MyTupleInput;
import org.ajmm.obsearch.ob.OBShort;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.apache.log4j.Logger;

import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.SortedFloatBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class PPTreeShort<O extends OBShort> extends AbstractPPTree<O> implements
		IndexShort<O> {

	private short minInput;

	private short maxInput;

	private static final transient Logger logger = Logger
	.getLogger(PPTreeShort.class);

	public PPTreeShort(File databaseDirectory, byte pivots, byte od)
			throws DatabaseException, IOException {
		this(databaseDirectory, pivots, od, Short.MIN_VALUE, Short.MAX_VALUE);
	}

	public PPTreeShort(File databaseDirectory, byte pivots, byte od,
			short minInput, short maxInput) throws DatabaseException,
			IOException {
		super(databaseDirectory, pivots, od);
		assert minInput < maxInput;
		this.minInput = minInput;
		this.maxInput = maxInput;
	}

	@Override
	protected float[] extractTuple(TupleInput in) throws OutOfRangeException {
		int i = 0;
		float[] res = new float[pivotsCount];
		while (i < pivotsCount) {
			res[i] = normalizeFirstPassAux(in.readShort());
			i++;
		}
		return res;
	}

	@Override
	public String getSerializedName() {
		// TODO Auto-generated method stub
		return "PPTreeShort";
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
		assert cDB.count() == bDB.count();
	}

	/**
	 * Generates a query rectangle based on the given range and the given tuple.
	 * It normalizes the query first level only
	 *
	 * @param t
	 *            the tuple to be processed
	 * @param r
	 *            the range
	 * @param q
	 *            resulting rectangle query
	 */

	protected void generateRectangleFirstPass(short[] t, short r, float[][] q)
			throws OutOfRangeException {
		// range
		int i = 0;
		while (i < q.length) { //
			q[i][MIN] = normalizeFirstPassAux((short) Math.max(t[i] - r, minInput));
			q[i][MAX] = normalizeFirstPassAux((short) Math.min(t[i] + r, maxInput));
			i++;
		}
		// put the query relative to the center
		// normalizeQuery(q);
	}

	/**
	 * Copies the contents of query src into dest
	 * @param src source
	 * @param dest destination
	 */
	protected void copyQuery(float[][] src, float[][] dest){
		int i = 0;
		while(i < src.length){
			dest[i][MIN] = src[i][MIN];
			dest[i][MAX] = src[i][MAX];
			i++;
		}
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
		float[][] qrect = new float[pivotsCount][2]; // rectangular query
		short myr = r;
		generateRectangleFirstPass(t, myr, qrect);
		// q has the rectangle now,
		// we have to find all the subspacess that collide with the query
		// the space-tree leaves will have the rectangle they cover,
		// in case our range gets reduced during the query, we can skip
		// those rectangles.

		float[] lowHighResult = new float[2];
		// hypercubes that intersect with this query will be stored here
		List<SpaceTreeLeaf> hyperRectangles = new LinkedList<SpaceTreeLeaf>();
		// obtain the hypercubes that have to be matched
		spaceTree.searchRange(qrect, hyperRectangles);
		Iterator<SpaceTreeLeaf> it = hyperRectangles.iterator();
		// this will hold the rectangle for the current hyperrectangle
		float[][] qw = new float[pivotsCount][2];
		float[][] q = new float[pivotsCount][2];
		while (it.hasNext()) {
			SpaceTreeLeaf space = it.next();
			if(! space.intersects(qrect)){
				continue;
			}

			// for each space there are 2d pyramids that have to be browsed
			int i = 0;
			// update the current rectangle, we also have to center it
			space.generateRectangle(qrect, qw);
			centerQuery(qw); // center the rectangle

			while (i < pyramidCount) {
				// intersect destroys q, so we have to copy it
				copyQuery(qw,q);
				if (intersect(q, i, lowHighResult)) {
					int ri = (space.getSNo() * 2 * pivotsCount) + i; // real
																	// index
					searchBTreeAndUpdate(object, t, myr, ri
							+ lowHighResult[HLOW], ri + lowHighResult[HHIGH],
							result);


					 short nr = result.updateRange(myr);
					 // make the range shorter
					 if (nr < myr) {
						 myr = nr; // regenerate the query with a smaller range
						 generateRectangleFirstPass(t, myr, qrect);
						 space.generateRectangle(qrect, qw);
						 centerQuery(qw); // center the rectangle
					  }

				}
				i++;
			}
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

	@Override
	protected byte insertFrozen(O object, int id) throws IllegalIdException,
			OBException, DatabaseException, IllegalAccessException,
			InstantiationException {
		short[] t = new short[pivotsCount];
		calculatePivotTuple(object, t); // calculate the tuple for the new //

		return insertFrozenAux(t, id);
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
		float[] first = normalizeFirstPass(t);
		float ppTreeValue = super.ppvalue(first);

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
		SortedFloatBinding.floatToEntry(ppTreeValue, keyEntry);
		dataEntry.setData(out.getBufferBytes());

		// TODO: check the status result of all the operations
		cDB.put(null, keyEntry, dataEntry);
		return 1;
	}

	protected float[] normalizeFirstPass(short[] t) throws OutOfRangeException {
		assert t.length == pivotsCount;
		float[] res = new float[pivotsCount];

		int i = 0;
		while (i < t.length) {
			res[i] = normalizeFirstPassAux(t[i]);
			i++;
		}
		return res;
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

	/**
	 * Normalize the given value This is a first pass normalization, any value
	 * to [0,1]
	 *
	 * @param x
	 * @return the normalized value
	 */
	protected float normalizeFirstPassAux(short x) throws OutOfRangeException {
		if (x < minInput || x > maxInput) {
			throw new OutOfRangeException(minInput + "", maxInput + "", "" + x);
		}
		return ((float) (x - minInput)) / ((float) (maxInput - minInput));
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

}
