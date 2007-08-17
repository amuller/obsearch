package org.ajmm.obsearch.example;

import java.io.StringReader;
import java.util.LinkedList;
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
 * Example Object that can be stored in OBSearch This class reads strings
 * representations of trees and calcualtes the distance between the objects by
 * using a tree distance function.
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 1.0
 */

public class OBSlice implements OBShort {

    /**
     * An String representation of the tree.
     */
    private String slice;

    /**
     * The root node of the tree.
     */
    private SliceAST tree;

    /**
     * Default constructor must be provided by every object that implements the
     * interface OB.
     */
    public OBSlice() {

    }

    /**
     * Creates an slice object.
     * @param slice
     *            A string representation of a tree.
     */
    public OBSlice(final String slice) {
        assert slice != null;
        this.slice = slice;
    }

    /**
     * Calculates the distance between two trees.
     * @param object
     *            The other object to compare
     * @see org.ajmm.obsearch.OB#distance(org.ajmm.obsearch.OB,
     *      org.ajmm.obsearch.Dim)
     * @throws OBException
     *             if something wrong happens.
     * @return A short that indicates how similar or different the trees are.
     */
    public final short distance(final OB object) throws OBException {
        OBSlice b = (OBSlice) object;
        updateTree();
        b.updateTree();

        List < SliceAST > aExpanded = this.tree.depthFirst();
        List < SliceAST > bExpanded = b.tree.depthFirst();
        List < SliceAST > bExpanded2 = new LinkedList < SliceAST >();
        bExpanded2.addAll(bExpanded);
        int Na = aExpanded.size() * 2;
        int Nb = bExpanded.size() * 2;

        ListIterator < SliceAST > ait = aExpanded.listIterator();
        int res = 0;
        while (ait.hasNext()) {
            SliceAST aTree = ait.next();
            ListIterator < SliceAST > bit = bExpanded.listIterator();
            while (bit.hasNext()) {
                SliceAST bTree = bit.next();
                if (aTree.equalsTree(bTree)) {
                    res++;
                    bit.remove();
                    break;
                }
            }
            // do the same for the nodes without children
            bit = bExpanded2.listIterator();
            while (bit.hasNext()) {
                SliceAST bTree = bit.next();
                if (aTree.getText().equals(bTree.getText())) {
                    res++;
                    bit.remove();
                    break;
                }
            }
        }
        // return Na - res + Nb - res;
        // return (Na + Nb) - ( 2 * res);
        return (short) (((Na + Nb) - (2 * res)) / 2);
    }

    /**
     * Internal method that updates the Tree from the String
     * @throws OBException
     */
    protected void updateTree() throws OBException {
        if (tree == null) {
            try {
                synchronized (slice) {
                    assert slice != null;
                    SliceLexer lexer = new SliceLexer(new StringReader(slice));
                    SliceParser parser = new SliceParser(lexer);
                    parser
                            .setASTNodeClass("org.ajmm.obsearch.example.SliceAST");
                    parser.slice();
                    tree = (SliceAST) parser.getAST();
                    tree.getSize();
                }
            } catch (Exception e) {
                throw new SliceParseException(slice, e);
            }
        }
    }

    /**
     * Returns the size (in nodes) of the tree.
     * @return The size of the tree.
     * @throws OBException
     *             If something goes wrong.
     */
    public final int size() throws OBException {
        updateTree();
        return tree.getSize();
    }

    /**
     * @return A String representation of the tree.
     */
    public final String toString() {
        try {
            updateTree();
            return tree.toStringList();
        } catch (Exception e) {
            assert false;
        }
        return ":)";
    }

    /**
     * Re-creates this object from the given byte stream
     * @param in
     *            A byte stream with the data that must be loaded.
     * @see org.ajmm.obsearch.Storable#load(com.sleepycat.bind.tuple.TupleInput)
     */
    public void load(TupleInput in) {
        slice = in.readString();
        assert slice != null : "Slice was null!";
        tree = null; // very important!
    }

    /**
     * Stores this object into the given byte stream.
     * @param out
     *            The byte stream to be used
     * @see org.ajmm.obsearch.Storable#store(com.sleepycat.bind.tuple.TupleOutput)
     */
    public final void store(TupleOutput out) {
        assert slice != null : "Slice was null";
        out.writeString(slice);
    }

    /**
     * Returns true of this.tree.equals(obj.tree). For this distance function
     * this.distance(obj) == 0 implies that this.equals(obj) == true
     * @param obj Object to compare.
     * @return true if this == obj
     */
    public final boolean equals(final Object obj) {
        if (!(obj instanceof OBSlice)) {
            assert false;
            return false;
        }
        OBSlice o = (OBSlice) obj;
        // parse the strings into trees before the equals too!
        try {
            updateTree();
            o.updateTree();
        } catch (Exception e) {
            assert false;
        }
        return tree.equalsTree(o.tree);
    }

    /**
     * A hashCode based on the string representation of the tree.
     * @return a hash code of the string representation of this tree.
     */
    public final int hashCode() {
        assert slice != null;
        return this.slice.hashCode();
    }

}
