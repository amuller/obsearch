package net.obsearch.example.lev;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.logging.Logger;

import org.kohsuke.args4j.Option;

import net.obsearch.ambient.my.AmbientMy;
import net.obsearch.ambient.tc.AmbientTC;
import net.obsearch.example.vectors.VectorsDemo;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OBStorageException;
import net.obsearch.exception.PivotsUnavailableException;
import net.obsearch.index.ghs.impl.Sketch64Short;
import net.obsearch.index.utils.AbstractNewLineCommandLineShort;
import net.obsearch.index.utils.Directory;
import net.obsearch.ob.OBShort;
import net.obsearch.pivots.AcceptAll;
import net.obsearch.pivots.rf02.RF02PivotSelectorShort;

public abstract class GHCShort<O extends OBShort>
		extends
		AbstractNewLineCommandLineShort<O, Sketch64Short<O>, AmbientTC<O, Sketch64Short<O>>> {
	/**
	 * Logging provided by Java
	 */
	
	@Option(name = "-ep", usage = "CompoundError value")
	protected double ep = 0.00001;
	
	static Logger logger = Logger.getLogger(GHCShort.class.getName());

	protected AmbientTC<O, Sketch64Short<O>> instantiateAmbient(File dbFolder)
			throws OBStorageException, OBException, FileNotFoundException,
			IllegalAccessException, InstantiationException, IOException {
		return new AmbientTC<O, Sketch64Short<O>>(dbFolder);
	}

	protected AmbientTC<O, Sketch64Short<O>> instantiateNewAmbient(File dbFolder)
			throws OBStorageException, OBException, FileNotFoundException,
			IllegalAccessException, InstantiationException, IOException {

		RF02PivotSelectorShort<O> sel = new RF02PivotSelectorShort<O>(
				new AcceptAll<O>());
		// make the bit set as short so that m objects can fit in the buckets.
		Sketch64Short<O> index = new Sketch64Short<O>(obtainClass(), sel, 16, 0);
		index.setExpectedEP(ep);
		index.setSampleSize(100);
		index.setMaxK(new int[] { 1, 3, 10, 50 });

		return new AmbientTC<O, Sketch64Short<O>>(index, dbFolder);
	}
}
