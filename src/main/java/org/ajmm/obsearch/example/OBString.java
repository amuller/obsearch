package org.ajmm.obsearch.example;

import java.util.Arrays;

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

public final class OBString extends OBEx implements OBShort {

    private String str;
    
    public OBString(){
        
    }
    
    public OBString(String str){
        this.str = str;
    }
    
    public boolean equals(Object obj){
        OBString other = (OBString) obj;
        return str.equals(other.str);
    }
    
    public int hashCode(){
        // TODO cache this.
        return str.hashCode();
    }
    
    // Levenshtein distance implementation.
    @Override
    public short distance(OBShort object) throws OBException {
        count++;
         return LD(str, ((OBString)object).str);
    }
    
  //*****************************
    // Compute Levenshtein distance
    // Taken from: http://www.merriampark.com/ld.htm
    //*****************************

    public short LD (String s, String t) {
    int d[][];
    int n; 
    int m;
    int i; 
    int j; 
    char s_i;
    char t_j;
    int cost; 


      n = s.length ();
      m = t.length ();
      if (n == 0) {
        return (short)m;
      }
      if (m == 0) {
        return (short)n;
      }
      d = new int[n+1][m+1];


      for (i = 0; i <= n; i++) {
        d[i][0] = i;
      }

      for (j = 0; j <= m; j++) {
        d[0][j] = j;
      }


      for (i = 1; i <= n; i++) {

        s_i = s.charAt (i - 1);

        for (j = 1; j <= m; j++) {

          t_j = t.charAt (j - 1);


          if (s_i == t_j) {
            cost = 0;
          }
          else {
            cost = 1;
          }


          d[i][j] = min (d[i-1][j]+1, d[i][j-1]+1, d[i-1][j-1] + cost);

        }

      }


      return (short)d[n][m];

    }

  
            
    private int min(int a, int b, int c){
        return Math.min(Math.min(a, b),c);
    }

    @Override
    public void load(TupleInput in) throws OBException {
        str= in.readString();      
    }

    @Override
    public void store(TupleOutput out) {
        out.writeString(str);
    }
    
    public String toString(){
        return new String(str);
    }

}
