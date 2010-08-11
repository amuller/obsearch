package net.obsearch.index;

import hep.aida.bin.StaticBin1D;

import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import net.obsearch.exception.OBException;
import net.obsearch.ob.OBFloat;
import net.obsearch.query.OBQueryFloat;

public class IndexUtilsFloat {
	protected static Logger logger = Logger.getLogger(IndexUtilsFloat.class
			.getName());
	public static void validateResults(List<OBQueryFloat> queries, IndexFloat index) throws OBException, IllegalAccessException, InstantiationException {
		StaticBin1D ep = new StaticBin1D();
		StaticBin1D rde = new StaticBin1D();
		StaticBin1D precision = new StaticBin1D();
		StaticBin1D compound = new StaticBin1D();
		StaticBin1D tdr = new StaticBin1D();

		Iterator<OBQueryFloat> it1 = queries
				.iterator();		
		StaticBin1D seqTime = new StaticBin1D();
		int i = 0;
		while (it1.hasNext()) {
			OBQueryFloat qu = it1.next();
			OBFloat q = qu.getObject();
			long time = System.currentTimeMillis();
			float[] sortedList = index.fullMatchLite(q, false);
			long el = System.currentTimeMillis() - time;
			seqTime.add(el);
			logger.info("Elapsed: " + el + " " + i);			
			ep.add(qu.ep(sortedList));
			rde.add(qu.rde(sortedList));
			precision.add(qu.precision(sortedList));
			double comp = qu.compound(sortedList);
			tdr.add(qu.tDR(sortedList));
			compound.add(comp);
			i++;
		}
		logger.info("EP");
		logger.info(ep.toString());
		logger.info("RDE");
		logger.info(rde.toString());
		logger.info("TDR");
		logger.info(tdr.toString());		
		logger.info("Precision: " + precision.mean());
		logger.info("Time per seq query: ");
		logger.info(seqTime.toString());

		logger.info("EP: " + ep.mean());
		logger.info("RDE: " + rde.mean());
		logger.info("Precision: " + precision.mean());
		logger.info("Compound: " + compound.mean());
		logger.info("TDR: " + tdr.mean());
		
	}
}
