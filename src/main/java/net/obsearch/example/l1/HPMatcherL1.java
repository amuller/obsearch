package net.obsearch.example.l1;

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
public class HPMatcherL1  {/*extends HPMatcher<L1> {

	@Override
	protected Class<L1> obtainClass() {
		return L1.class;
	}
	
	@Override
	protected AbstractCommandLine getReference() {
		return this;
	}

	@Override
	protected L1 instantiate(String line) throws OBException {
		return new L1(line);
	}
	
	public static void main(String args[]){
		HPMatcherL1 s = new HPMatcherL1();
		s.processUserCommands(args);
	}
	
	protected boolean isValidObject(L1 object) throws OBException{		
		return true;
	}

	*/

}
