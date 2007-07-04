package org.ajmm.obsearch.index.pivotselection;

import hep.aida.bin.MightyStaticBin1D;

import java.util.Random;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.index.AbstractPivotIndex;
import org.ajmm.obsearch.ob.OBShort;
import org.apache.log4j.Logger;

import com.sleepycat.je.DatabaseException;

public class TentaclePivotSelectorShort<O extends OBShort> extends AbstractTentaclePivotSelector<O> {
// TODO: fix a bug in which not all the pivots are being selected.
	// from 600 only 88 were selected.
	private short d = Short.MIN_VALUE;
	private short minD;

	/**
	 * Creates a new tentacle selector that will select pivots
	 * with at least minD units
	 * @param minD (minimun accepted number of units)
	 */
	public TentaclePivotSelectorShort(short minD){
		this.minD = minD;
	}
	private static transient final Logger logger = Logger
	.getLogger(TentaclePivotSelectorShort.class);

	@Override
	protected boolean easifyD() {
		// TODO Auto-generated method stub
		if(d > minD){
			d--;
			if(logger.isDebugEnabled()){
				logger.debug("Current d: " + d);
			}
			return true;
		}else{
			return false;
		}
		
	}

	@Override
	protected O obtainD(AbstractPivotIndex<O> x) throws InstantiationException , IllegalAccessException, DatabaseException, OBException{
		Random r = new Random();
		int m = x.getMaxId();
		int id = r.nextInt(m);
		O ob = x.getObject(id);
		int i = 0;
		MightyStaticBin1D data = new  MightyStaticBin1D(true,false,4);
		while(i < m){
			if(id != i){
				short res = ob.distance(x.getObject(i));
				if(logger.isDebugEnabled()){
					if(i % 10000 == 0){
						logger.debug("Finding averages for:" + i);
					}
				}	
				if(res != 0){
					data.add(res);
				}
			}
			i++;
		}
		double mean = data.geometricMean();
		
		d = (short) mean;
		logger.debug("D found by harmonic mean: " + d);
		return ob;
	}

	@Override
	protected boolean withinRange(O a, O b) throws OBException{
		// TODO Auto-generated method stub
		//return Math.abs(a.distance(b) - d) <= minD;
		return Math.abs(a.distance(b) - d) <= minD;
	}

}
