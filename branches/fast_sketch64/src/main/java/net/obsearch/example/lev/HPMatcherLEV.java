package net.obsearch.example.lev;

import java.io.File;
import java.io.IOException;

import net.obsearch.example.OBSlice;
import net.obsearch.exception.IllegalIdException;
import net.obsearch.exception.NotFrozenException;
import net.obsearch.exception.OBException;
import net.obsearch.exception.OutOfRangeException;
import net.obsearch.index.utils.AbstractCommandLine;
import net.obsearch.stats.Statistics;
/**
 * Command line matcher for MTD objects
 * @author Arnoldo Jose Muller-Molina
 *
 */
public class HPMatcherLEV {/*extends HPMatcher<OBString> {

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
		HPMatcherLEV s = new HPMatcherLEV();
		s.processUserCommands(args);
	}
	
	protected boolean isValidObject(OBString object) throws OBException{
		return object.length() <= Short.MAX_VALUE;
	}

	

}*/
}
