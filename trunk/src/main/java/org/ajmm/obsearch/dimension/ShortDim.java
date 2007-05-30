package org.ajmm.obsearch.dimension;

import org.ajmm.obsearch.Dim;
import org.ajmm.obsearch.exception.OutOfRangeException;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;

/*
    OBSearch: a distributed similarity search engine
    This project is to similarity search what 'bit-torrent' is to downloads.
    Copyright (C)  2007 Arnoldo Jose Muller Molina

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
	  Class: ShortDim
	  
    @author      Arnoldo Jose Muller Molina    
    @version     %I%, %G%
    @since       0.0
*/

public class ShortDim implements Dim {

    
    
    private short value;
    
    public ShortDim(short x){
        value = x;
    }
    /**
     * @see org.ajmm.obsearch.Dim#ge(org.ajmm.obsearch.Dim)
     */
    public boolean ge(Dim x) {
        return value >= ((ShortDim)x).value;
    }

    /**
     * @see org.ajmm.obsearch.Dim#gt(org.ajmm.obsearch.Dim)
     */
    public boolean gt(Dim x) {
        // TODO Auto-generated method stub
        return value > ((ShortDim)x).value;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Dim#lInfiniteOneStep(com.sleepycat.bind.tuple.TupleInput, org.ajmm.obsearch.Dim, org.ajmm.obsearch.Dim)
     */
    public boolean lInfiniteOneStep(TupleInput in, Dim r, Dim max) {
        // TODO Auto-generated method stub
        short t = (short)Math.abs(value - in.readShort());
        if(t > ((ShortDim)r).value){
            return false;
        }
        ShortDim max2 = (ShortDim) max;
        if(t > max2.value){
            max2.value = t;
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Dim#le(org.ajmm.obsearch.Dim)
     */
    public boolean le(Dim x) {
         return value <= ((ShortDim)x).value;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Dim#lt(org.ajmm.obsearch.Dim)
     */
    public boolean lt(Dim x) {
         return value < ((ShortDim)x).value;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Dim#normalize()
     */
    public float normalize(Dim min, Dim max) throws OutOfRangeException{
        // TODO Auto-generated method stub
        ShortDim min2 = (ShortDim)min;
        ShortDim max2 = (ShortDim)max;
        if ( value < min2.value || value > max2.value){
            throw new OutOfRangeException();
        }
        return  ((float)(value - min2.value)) / ((float)(max2.value - min2.value));
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Dim#setToAbsoluteSmall()
     */
    public void setToAbsoluteSmall() {
         value = Short.MIN_VALUE;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Dim#update(org.ajmm.obsearch.Dim)
     */
    public void update(Dim x) {
         value = ((ShortDim)x).value;
    }
    
    public void update(short x){
        value = x;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Dim#updateSmaller(org.ajmm.obsearch.Dim)
     */
    public void updateSmaller(Dim x) {
        value = ((ShortDim)x).value ;        
        value--;
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Storable#load(com.sleepycat.bind.tuple.TupleInput)
     */
    public void load(TupleInput byteInput) {
        value = byteInput.readShort();
    }

    /* (non-Javadoc)
     * @see org.ajmm.obsearch.Storable#store(com.sleepycat.bind.tuple.TupleOutput)
     */
    public void store(TupleOutput out) {
        out.writeShort(value);
    }

}
