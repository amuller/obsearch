<@pp.dropOutputFile />
<#include "/@inc/ob.ftl">
<#list types as t>
<@type_info t=t/>
<@pp.changeOutputFile name="TestFrameworkApprox${Type}.java" />
/*
		OBSearch: a distributed similarity search engine This project is to
 similarity search what 'bit-torrent' is to downloads. 
    Copyright (C) 2008 Arnoldo Jose Muller Molina

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
	*  TestFrameworkApprox 
	*  
  *  @author      Arnoldo Jose Muller Molina    
  */
package net.obsearch.index;

import hep.aida.bin.StaticBin1D;
import org.apache.log4j.Logger;
import net.obsearch.asserts.OBAsserts;
import net.obsearch.exception.OBStorageException;
import net.obsearch.index.Index${Type};
import net.obsearch.ob.OB${Type};
import net.obsearch.result.OBPriorityQueue${Type};
import net.obsearch.result.OBResult${Type};
import net.obsearch.index.utils.StatsUtil;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
/*
    OBSearch: a distributed similarity search engine
    This project is to similarity search what 'bit-torrent' is to downloads.
    Copyright (C)  2008 Arnoldo Jose Muller Molina

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
*/
/** 
		<class_description> Perform approximate validation of data.
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       0.0
*/


public abstract class TestFrameworkApprox${Type}<O extends OB${Type}>  extends TestFramework${Type}<O> {
	
		private static final Logger logger = Logger.getLogger(TestFrameworkApprox${Type}.class);

		public TestFrameworkApprox${Type}(Class<O> type, int dbSize, int querySize,
																			Index${Type}<O> index) {
				super(type, dbSize, querySize, index);
		}
	
		  	/**
     * Perform all the searches with
     * @param x
     *                the index that will be used
     * @param range
     * @param k
     */
    public void search(Index${Type} < O > index, ${type} range, byte k)
            throws Exception {
        //range = (${type})Math.min(${ClassType}.MAX_VALUE, range);
        index.resetStats();
        // it is time to Search
        
        String re = null;
        logger.info("Matching begins...");
        
        int i = 0;
        long realIndex = index.databaseSize();
        List < OBPriorityQueue${Type} < O >> result = new LinkedList < OBPriorityQueue${Type} < O >>();        
        while (i < this.queries.length) {
                OBPriorityQueue${Type} < O > x = new OBPriorityQueue${Type} < O >(
                        k);
                if (i % 100 == 0) {
                    logger.info("Matching " + i);
                }

                O s = queries[i];                
                    index.searchOB(s, range, x);
                    result.add(x);
                    i++;                                        
        }
        logger.info("Range: " + range + " k " + k + " " + index.getStats().toString());
       
        int maxQuery = i;

        Iterator < OBPriorityQueue${Type} < O >> it = result.iterator();
				StaticBin1D stats = new StaticBin1D();
        i = 0;
				int emptyResults = 0;
        while (i < queries.length) {
        	if (i % 300 == 0) {
                    logger.info("Validating " + i + " of " + maxQuery);
        	}
                O s = queries[i];
                    OBPriorityQueue${Type} < O > x2 = new OBPriorityQueue${Type} < O >(
                            k);
                    searchSequential( s, x2, index, range);
                    OBPriorityQueue${Type} < O > x1 = it.next();
                    
                    
										stats.add(ep(x1,x2,index));
										if(x1.getSize() == 0 && x2.getSize() != 0){
						emptyResults++;
					}
                    i++;
                
                
            }

				logger.info("Finished  EP calculation: ");
				logger.info(StatsUtil.prettyPrintStats("EP", stats));
                   
        logger.info("Finished  matching validation.");
        assertFalse(it.hasNext());

				logger.info("Zero queries: " + emptyResults);
    }


		private double ep(OBPriorityQueue${Type}<O> x1, OBPriorityQueue${Type}<O> x2, Index${Type} < O > index) throws OBStorageException{
		List<OBResult${Type}<O>> query = x1.getSortedElements();
		List<OBResult${Type}<O>> db = x2.getSortedElements();
		int i = 0;
		int result = 0;
		Set<OBResult${Type}<O>> s = new HashSet<OBResult${Type}<O>>();
		for(OBResult${Type}<O> r : query){
			// find the position in the db. 
			int cx = 0;
			for(OBResult${Type}<O> c : db){
				if(s.contains(c)){
					cx++;
					continue;
				}
				if(c.compareTo(r) == 0){
					s.add(c);
					result += cx - i;
					break;
				}
				cx++;
			}
			i++;
		}
		if(query.size() == 0){
			return 0;
		}else{
			double res = ((double)result)/ ((double)(query.size() * index.databaseSize()));
			return res;
		}
	}

}
</#list>