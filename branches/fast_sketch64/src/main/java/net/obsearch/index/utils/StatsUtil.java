package net.obsearch.index.utils;

import hep.aida.bin.StaticBin1D;

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
 * StatsUtil
 * @author Arnoldo Jose Muller Molina
 */

public class StatsUtil {

    /**
     * Reports I/O stats from the given StaticBin1D
     * @param component
     * @param stats
     * @return
     */
    public static String prettyPrintStats(String component, StaticBin1D stats){
        StringBuilder res = new StringBuilder();
        // gloria cantamos gloria.
        res.append("Component: " + component);
				res.append("\n");
        res.append("Mean: " + stats.mean());
								res.append("\n");
        res.append("Std. Dev: " + stats.standardDeviation());
								res.append("\n");
        res.append("Min: " + stats.min());
								res.append("\n");
        res.append("Max: " + stats.max());
								res.append("\n");
        res.append("Total: " + stats.sum());
								res.append("\n");
        res.append("Size: " + stats.size());       
								res.append("\n");
        return res.toString();
    }
    
}
