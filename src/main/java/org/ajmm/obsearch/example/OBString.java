package org.ajmm.obsearch.example;

import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.ob.OBShort;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

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
 * OBString matches strings by using the Levenshtein distance.
 * @author Arnoldo Jose Muller Molina
 */

public class OBString implements OBShort {

    private char[] str;
    
    public OBString(String str){
        this.str = str.toCharArray();
    }
    
    // Levenshtein distance implementation.
    @Override
    public short distance(OBShort object) throws OBException {
        OBString other = (OBString) object;
       
        int m = str.length;
        int n = other.str.length;
        // d is a table with m+1 rows and n+1 columns
        int d[][] = new int[m+1][n+1];
        // intialize
        

        for(int i = 0; i < m ;i++){
            d[i][0] = i;
        }
        for(int j = 0; j<  n; j++){
            d[0][j] = j;
        }
        
        for(int i = 1; i <= m ;i++){
            for(int j = 1; j<=  n; j++){
                
                short cost;
                if( str[i-1] == other.str[j-1]){
                    cost = 0;
                }else{
                    cost = 1;
                }
                               
                d[i][j] = min(
                                     d[i-1][j] + 1,     // deletion
                                     d[i][j-1] + 1,     // insertion
                                     d[i-1][j-1] + cost   // substitution
                                 );
            }
        }
      
        return (short)d[m][n];        
    }
            
    private int min(int a, int b, int c){
        return Math.min(Math.min(a, b),c);
    }

    @Override
    public void load(TupleInput in) throws OBException {
        str= in.readString().toCharArray();        
    }

    @Override
    public void store(TupleOutput out) {
        out.writeString(new String(str));
    }

}
