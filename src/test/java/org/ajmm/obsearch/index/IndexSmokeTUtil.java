package org.ajmm.obsearch.index;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.ajmm.obsearch.ParallelIndex;
import org.ajmm.obsearch.TUtils;
import org.ajmm.obsearch.SynchronizableIndex;
import org.ajmm.obsearch.TimeStampResult;
import org.ajmm.obsearch.example.OBSlice;
import org.ajmm.obsearch.index.pivotselection.DummyPivotSelector;
import org.ajmm.obsearch.index.pivotselection.RandomPivotSelector;
import org.ajmm.obsearch.index.utils.Directory;
import org.ajmm.obsearch.result.OBPriorityQueueShort;
import org.ajmm.obsearch.result.OBResultShort;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

public class IndexSmokeTUtil {

    Properties testProperties;

    private static transient final Logger logger = Logger
	    .getLogger(IndexSmokeTUtil.class);

    public IndexSmokeTUtil() throws IOException {
	testProperties = TUtils.getTestProperties();
    }

    public void initIndex(IndexShort<OBSlice> index) throws Exception {
	File query = new File(testProperties.getProperty("test.query.input"));
	File db = new File(testProperties.getProperty("test.db.input"));
	File dbFolder = new File(testProperties.getProperty("test.db.path"));
	logger.debug("query file: " + query);
	logger.debug("db file: " + db);

	logger.info("Adding data");
	BufferedReader r = new BufferedReader(new FileReader(db));
	String re = r.readLine();
	int realIndex = 0;
	while (re != null) {
	    String line = parseLine(re);
	    if (line != null) {
		OBSlice s = new OBSlice(line);
		if (shouldProcessSlice(s)) {
		    assertEquals(realIndex, index.insert(s));
		    realIndex++;
		}
	    }
	    re = r.readLine();

	}
	// logger.info("Inserted elements: " + realIndex);
	// select the pivots
	// TentaclePivotSelectorShort<OBSlice> ps = new
	// TentaclePivotSelectorShort<OBSlice>((short)5);
	// RandomPivotSelector ps = new RandomPivotSelector();
	DummyPivotSelector ps = new DummyPivotSelector();
	if (index instanceof ParallelIndex) {
	    ps.generatePivots((AbstractPivotIndex) ((ParallelIndex) index)
		    .getIndex());
	} else if (index instanceof SynchronizableIndex) {
	    ps
		    .generatePivots((AbstractPivotIndex) ((SynchronizableIndex) index)
			    .getIndex());
	} else {
	    ps.generatePivots((AbstractPivotIndex) index);
	}
	// the pyramid values are created
	logger.info("freezing");
	index.freeze();

	// we should test that the exists method works well

	r = new BufferedReader(new FileReader(db));
	re = r.readLine();

	while (re != null) {
	    String line = parseLine(re);
	    if (line != null) {
		OBSlice s = new OBSlice(line);
		if (shouldProcessSlice(s)) {
		    assertTrue(index.exists(s));
		    // attempt to insert the object again, and get
		    // the -1
		    assertEquals(-1, index.insert(s));
		}
	    }
	    re = r.readLine();
	}
    }

    /**
         * Test method for
         * {@link org.ajmm.obsearch.index.AbstractPivotIndex#insertObjectInDatabase(org.ajmm.obsearch.OB, int, com.sleepycat.je.Database)}.
         * Creates a database, fills it with a bunch of rows performs a query
         * and compares the result with the sequential search.
         */
    protected void tIndex(IndexShort<OBSlice> index) throws Exception {

	File query = new File(testProperties.getProperty("test.query.input"));
	File db = new File(testProperties.getProperty("test.db.input"));
	File dbFolder = new File(testProperties.getProperty("test.db.path"));
	long currentTime = System.currentTimeMillis();
	int cx = 0;
	int querySize = 1642; // amount of elements to read from the query
	String re = null;
	initIndex(index);
	// assertEquals(index.aDB.count(), index.bDB.count());
	// assertEquals(index.aDB.count(), index.bDB.count());
	// index.stats();
	byte k = 3;
	short range = 3; // range
	// it is time to Search
	logger.info("Matching begins...");
	BufferedReader r = new BufferedReader(new FileReader(query));
	List<OBPriorityQueueShort<OBSlice>> result = new LinkedList<OBPriorityQueueShort<OBSlice>>();
	re = r.readLine();
	int i = 0;
	int realIndex = index.databaseSize();

	while (re != null) {
	    String line = parseLine(re);
	    if (line != null) {
		OBPriorityQueueShort<OBSlice> x = new OBPriorityQueueShort<OBSlice>(
			k);
		if (i % 100 == 0) {
		    logger.info("Matching " + i);
		}

		OBSlice s = new OBSlice(line);
		if (shouldProcessSlice(s)) {
		    index.searchOB(s, range, x);
		    result.add(x);
		    i++;
		}
	    }
	    if (i == querySize) {
		logger.warn("Finishing test at i : " + i);
		break;
	    }
	    re = r.readLine();
	}
	if (index instanceof ParallelIndex) {
	    logger.info("Waiting for Queries");
	    ((ParallelIndex) index).waitQueries();
	}
	int maxQuery = i;
	logger.info("Pyramid matching ends... Stats follow:");
	// index.stats();
	// now we compare the results we got with the real thing!
	Iterator<OBPriorityQueueShort<OBSlice>> it = result.iterator();
	r = new BufferedReader(new FileReader(query));
	re = r.readLine();
	i = 0;
	while (re != null) {
	    String line = parseLine(re);
	    if (line != null) {
		if (i % 300 == 0) {
		    logger.info("Matching " + i + " of " + maxQuery);
		}
		OBSlice s = new OBSlice(line);
		if (this.shouldProcessSlice(s)) {
		    OBPriorityQueueShort<OBSlice> x2 = new OBPriorityQueueShort<OBSlice>(
			    k);
		    searchSequential(realIndex, s, x2, index, range);
		    OBPriorityQueueShort<OBSlice> x1 = it.next();
		    assertEquals("Error in query line: " + i + " slice: "
			    + line, x2, x1);

		    // test the other search methods
		    OBPriorityQueueShort<OBSlice> x3 = new OBPriorityQueueShort<OBSlice>(
			    k);
		    int[] inter = index.intersectingBoxes(s, range);
		    index.searchOB(s, range, x3, inter);

		    assertEquals("Error in intersectingBoxes: " + i
			    + " slice: " + line, x2, x3);
		    int box = 0; // this is just an index :)
		    while (box < index.totalBoxes()) {
			if (isIn(box, inter)) {
			    assertTrue(index.intersects(s, range, box));
			} else {
			    assertFalse(index.intersects(s, range, box));
			}
			box++;
		    }

		    i++;
		}
	    }
	    if (i == querySize) {
		logger.warn("Finishing test at i : " + i);
		break;
	    }
	    re = r.readLine();
	}
	logger.info("Finished  matching...");
	assertFalse(it.hasNext());
	// now check that times make sense.

	if (index instanceof SynchronizableIndex) {
	    logger.info("Testing timestamp index");
	    SynchronizableIndex<OBSlice> index2 = (SynchronizableIndex<OBSlice>) index;
	    i = 0;
	    int totalCx = 0;
	    logger.info("Total Boxes: " + index2.totalBoxes());
	    while (i < index2.totalBoxes()) {
		Iterator<TimeStampResult<OBSlice>> it2 = index2
			.elementsNewerThan(i, 0);
		int cx2 = 0;
		while (it2.hasNext()) {
		    TimeStampResult<OBSlice> t = it2.next();
		    OBSlice o = t.getObject();
		    assert o != null;
		    OBPriorityQueueShort<OBSlice> x = new OBPriorityQueueShort<OBSlice>(
			    (byte) 1);
		    // TODO: for r=0 p+tree is failing here but pyramid is
                        // not.
		    // why?
		    index.searchOB(o, (short) 1, x);
		    Iterator<OBResultShort<OBSlice>> it3 = x.iterator();
		    assertTrue(" Size found:" + x.getSize() + " item # " + cx
			    + " : " + o, x.getSize() == 1);
		    while (it3.hasNext()) {
			OBResultShort<OBSlice> j = it3.next();
			assertTrue(j.getObject().equals(o));
			assertTrue(j.getObject().distance(o) == 0);
		    }
		    cx2++;

		}
		assertEquals(cx2, index2.elementsPerBox(i));
		logger.info("Result: box: " + i + " Cx" + cx2);
		totalCx += cx2;
		i++;
	    }
	    assertEquals(realIndex, totalCx);
	    logger.info("CX: " + totalCx);
	    logger.info("Testing completed");
	}

	// now we delete elements from the DB
	i = 0;
	int max = index.databaseSize();
	while (i < max) {
	    OBSlice x = index.getObject(i);
	    assertTrue(index.exists(x));
	    assertEquals(i, index.delete(x));
	    assertTrue(!index.exists(x));
	    i++;
	}
	index.close();
	Directory.deleteDirectory(dbFolder);
    }

    // if x is in j
    public static boolean isIn(int x, int[] j) {
	for (int k : j) {
	    if (k == x) {
		return true;
	    }
	}
	return false;
    }

    public static boolean shouldProcessSlice(OBSlice x) throws Exception {
	return x.size() <= 100;
    }

    public static String parseLine(String line) {
	if (line.startsWith("//") || "".equals(line.trim())
		|| (line.startsWith("#") && !line.startsWith("#("))) {
	    return null;
	} else {
	    String arr[] = line.split("[:]");
	    if (arr.length == 2) {
		return arr[1];
	    } else if (arr.length == 1) {
		return arr[0];
	    } else {
		assert false : "Received line: " + line;
		return null;
	    }
	}
    }

    /**
         * 
         * @param max
         * @param o
         * @param result
         * @param index
         * @param range
         * @throws Exception
         */
    public void searchSequential(int max, OBSlice o,
	    OBPriorityQueueShort<OBSlice> result, IndexShort<OBSlice> index,
	    short range) throws Exception {
	int i = 0;
	while (i < max) {
	    OBSlice obj = index.getObject(i);
	    short res = o.distance(obj);
	    if (res <= range) {
		result.add(i, obj, res);
	    }
	    i++;
	}
    }

}
