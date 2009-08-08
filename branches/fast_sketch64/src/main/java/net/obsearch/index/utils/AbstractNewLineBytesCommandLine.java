package net.obsearch.index.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.Option;

import net.obsearch.Index;
import net.obsearch.OB;
import net.obsearch.ambient.Ambient;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.stats.Statistics;

public abstract class AbstractNewLineBytesCommandLine<O extends OB, I extends Index<O>, A extends Ambient<O, I>>
		extends AbstractNewLineCommandLine<O, I, A> {

	private static Logger logger = Logger
			.getLogger(AbstractNewLineBytesCommandLine.class);

	private InputStreamReader createReader(File toOpen)
			throws FileNotFoundException {

		return new InputStreamReader(new FileInputStream(toOpen), Charset
				.forName("US-ASCII"));
	}

	protected void addObjects(I index, File load) throws IOException,
			OBStorageException, OBException, IllegalAccessException,
			InstantiationException {
		if (bulkMode) {
			logger.info("Using  bulk mode");
		}
		InputStreamReader r = createReader(load);
		byte[] line = new byte[arraySize()];
		int i = 0;
		int res = read(line, r);
		while (res != -1) {
			O o = instantiate(line);
			if (bulkMode) {
				index.insertBulk(o);
			} else {
				index.insert(o);
			}
			res = read(line, r);
			if (i % 100000 == 0) {
				logger.info("Loading: " + i);
				// logger.info(index.getStats().toString());
			}
			i++;
		}
	}

	protected void searchObjects(I index, File load, Statistics other)
			throws IOException, OBException, InstantiationException,
			IllegalAccessException {
		InputStreamReader r = createReader(load);
		byte[] line = new byte[arraySize()];
		int i = 0;
		int res = read(line, r);
		while (res != -1 && i < super.maxQueries) {
			O o = instantiate(line);
			queries++;
			if (i % 100 == 0) {
				logger.info("Searching: " + i);
			}
			searchObject(index, o, other);
			res = read(line, r);
			i++;
		}
	}

	private int read(byte[] buffer, InputStreamReader r) throws IOException {
		int i = 0;
		while (i < buffer.length) {
			int b = r.read();
			if (b == -1 && i != 0) {
				throw new IOException(
						"Reached end of file before we could complete one read");
			} else if (b == -1) {
				return b; // we are done.
			}
			assert b >= 0;
			assert b <= Byte.MAX_VALUE;
			buffer[i] = (byte) b;
			i++;
		}
		// we should have a newline here
		int b = r.read();
		if (b == -1) {
			return b;
		} else if (b != '\n') {
			throw new IOException("Format is incorrect");
		}
		return b;
	}

	/**
	 * Read a byte array with the given size;
	 * 
	 * @return
	 */
	protected abstract int arraySize();

	/**
	 * Instantiate an object from a fixed byte array;
	 * 
	 * @return The object
	 * @throws OBException
	 */
	protected abstract O instantiate(byte[] line) throws OBException;

	/**
	 * Instantiate an object from a string.
	 * 
	 * @return The object
	 * @throws OBException
	 */
	protected O instantiate(String line) throws OBException {
		throw new OBException("This is not used here");
	}

}
