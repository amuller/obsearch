package net.obsearch.index.utils;

import java.util.Comparator;

import net.obsearch.asserts.OBAsserts;

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
 * ByteArrayComparison compares byte arrays.
 * @author Arnoldo Jose Muller Molina
 */

public final class ByteArrayComparator implements Comparator < byte[] > {

    /*
     * (non-Javadoc)
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    /*
     * public int compare(byte[] o1, byte[] o2) { OBAsserts.chkParam(o1.length ==
     * o2.length, "arrays must be equal"); int res = 0; int i = 0; while(i <
     * o1.length){ if(o1[i] < o2[i]){ res = -1; break; }else if(o1[i] > o2[i]){
     * res = 1; break; } i++; } return res; }
     */
    public  int compare(byte[] o1, byte[] o2) {
        for (int i = 0; i < o1.length && i < o2.length; i++) {

            int b1 = 0xFF & o1[i];
            int b2 = 0xFF & o2[i];
            if (b1 < b2)
                return -1;
            else if (b1 > b2)
                return 1;
        }

        if (o1.length < o2.length)
            return -1;
        else if (o1.length > o2.length)
            return 1;
        else
            return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return ByteArrayComparator.class.equals(obj.getClass());
    }

}
