package org.ajmm.obsearch.testutils;

import java.io.StringReader;
import java.util.List;
import java.util.ListIterator;

import org.ajmm.obsearch.OB;
import org.ajmm.obsearch.exception.OBException;
import org.ajmm.obsearch.ob.OBShort;

import antlr.RecognitionException;

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
 * Class: OBSlice
 * 
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class OBSlice implements OBShort {

    private String slice;

    private SliceAST tree;

    /**
     * Default constructor must be provided by OB's sons and daughters
     * 
     */
    public OBSlice() {

    }

    /**
     * Creates an slice object
     * 
     * @param slice
     */
    public OBSlice(String slice) {
    	assert slice != null;
        this.slice = slice;
    }

    /**
     * Calculates the distance between two trees When the internal tree is null
     * (first time is loaded or unloaded from memory) The tree is recreated from
     * the string and the original string is deleted to preserve memory
     * 
     * @see org.ajmm.obsearch.OB#distance(org.ajmm.obsearch.OB,
     *      org.ajmm.obsearch.Dim)
     */
    public short distance(OB object) throws OBException {
        // TODO Auto-generated method stub
        OBSlice b = (OBSlice) object;
        updateTree();
        b.updateTree();

        List<SliceAST> aExpanded = this.tree.depthFirst();
        List<SliceAST> bExpanded = b.tree.depthFirst();

        int Na = aExpanded.size();
        int Nb = bExpanded.size();

        ListIterator<SliceAST> ait = aExpanded.listIterator();
        int res = 0;
        while (ait.hasNext()) {
            SliceAST aTree = ait.next();
            ListIterator<SliceAST> bit = bExpanded.listIterator();
            while (bit.hasNext()) {
                SliceAST bTree = bit.next();
                if (aTree.equalsTree(bTree)) {
                    res++;
                    bit.remove();
                    break;
                }
            }
        }
        // return Na - res + Nb - res;
        // return (Na + Nb) - ( 2 * res);
        return (short) (Math.max(Na, Nb) - res);
    }

    protected void updateTree() throws OBException {
        if (tree == null) {
            try {
            	assert slice != null;
                SliceLexer lexer = new SliceLexer(new StringReader(slice));
                SliceParser parser = new SliceParser(lexer);
                parser.setASTNodeClass("org.ajmm.obsearch.testutils.SliceAST");
                parser.slice();
                tree = (SliceAST) parser.getAST();
                synchronized(tree){
                	tree.getSize();
                }
                // TODO: maybe this method should be syncronized
            } catch (Exception e) {
                throw new SliceParseException(slice);
            }
            //slice = null;
        }
    }
    
    public int size() throws OBException{
    	updateTree();
    	return tree.getSize();
    }


    /*
     * (non-Javadoc)
     * 
     * @see org.ajmm.obsearch.Storable#load(com.sleepycat.bind.tuple.TupleInput)
     */
    public void load(TupleInput in) {
        // TODO Auto-generated method stub
        slice = in.readString();
        assert slice !=null: "Slice was null!";
        tree = null; //very important!
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.ajmm.obsearch.Storable#store(com.sleepycat.bind.tuple.TupleOutput)
     */
    public void store(TupleOutput out) {
        // TODO Auto-generated method stub
        out.writeString(slice);
    }

    public boolean equals(Object obj) {
        OBSlice o = (OBSlice) obj;
        return this.tree.equals(o.tree);
    }

}
