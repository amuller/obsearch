package net.obsearch.example.lev;

import java.io.IOException;

import net.obsearch.example.OBSlice;
import net.obsearch.example.mtd.L2SymMatcher;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.hc.impl.SVMFilterShort;
import net.obsearch.index.utils.AbstractCommandLine;
import net.obsearch.stats.Statistics;
/**
 * Command line matcher for LEV objects
 * @author Arnoldo Jose Muller-Molina
 *
 */
public class L2SymMatcherLEV extends L2SymMatcher<OBString> {

	@Override
	protected Class<OBString> obtainClass() {
		return OBString.class;
	}
	
	@Override
	protected AbstractCommandLine getReference() {
		return this;
	}

	@Override
	protected OBString instantiate(String line) throws OBException {
		return new OBString(line);
	}
	
	public static void main(String args[]){
		L2SymMatcherLEV s = new L2SymMatcherLEV();
		s.processUserCommands(args);
	}
	
	protected boolean isValidObject(OBString object) throws OBException{
		return object.length() <= Short.MAX_VALUE;
	}
	
	

}
