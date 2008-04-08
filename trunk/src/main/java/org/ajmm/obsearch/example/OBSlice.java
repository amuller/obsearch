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
 * representations of trees and calculates the distance between the objects by
 * using a tree distance function.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */

public class OBSlice implements OBShort {

    public static long count = 0;

    /**
     * The root node of the tree.
     */
    protected SliceAST tree;
    private int hashCode;

    /**
     * Default constructor must be provided by every object that implements the
     * interface OB.
     */
    public OBSlice() {

    }

    /**
     * Creates an slice object.
     * @param slice
     *                A string representation of a tree.
     */
    public OBSlice(final String slice) throws OBException {
        this.updateTree(slice);
        assert tree.equalsTree( this.parseTree(tree.toFuriaChanTree())) : "This: " + tree.toFuriaChanTree() + " slice: " + slice + " size: " + tree.getSize();
    }

    /**
     * Calculates the distance between two trees. TODO: traverse the smallest
     * tree.
     * @param object
     *                The other object to compare
     * @see org.ajmm.obsearch.OB#distance(org.ajmm.obsearch.OB,
     *      org.ajmm.obsearch.Dim)
     * @throws OBException
     *                 if something wrong happens.
     * @return A short that indicates how similar or different the trees are.
     */
    public final short distance(final OBShort object) throws OBException {
        OBSlice b = (OBSlice) object;
        count++;
        assert this.tree != null;
        assert b.tree != null;
        if (this.tree.getSize() < b.tree.getSize()) {
            return distance(this.tree, b.tree);
        } else {
            return distance(b.tree, this.tree);
        }
    }

    /**
     * Calculates the distance between trees a and b.
     * @param a
     *                The first tree (should be smaller than b)
     * @param b
     *                The second tree
     * @return The distance of the trees.
     */
    private final short distance(SliceAST a, SliceAST b) {

        List < SliceAST > aExpanded = a.depthFirst();
        List < SliceAST > bExpanded = b.depthFirst();
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
        short r = (short) (((Na + Nb) - (2 * res)) / 2);
        assert r >= 0;
        return r;
    }

    /**
     * Internal method that updates the Tree from the String
     * @throws OBException
     */
    protected final void updateTree(String x) throws OBException {
        tree = parseTree(x);
        this.hashCode = tree.toFuriaChanTree().hashCode();
    }

    private final SliceAST parseTree(String x) throws SliceParseException {
        try {
            SliceLexer lexer = new SliceLexer(new StringReader(x));
            SliceParser parser = new SliceParser(lexer);
            parser.setASTNodeClass("org.ajmm.obsearch.example.SliceAST");
            parser.slice();
            SliceAST t = (SliceAST) parser.getAST();
            t.updateDecendantInformation();
            return t;
        } catch (Exception e) {
            throw new SliceParseException(x, e);
        }
    }
    
    

    /**
     * Returns the size (in nodes) of the tree.
     * @return The size of the tree.
     * @throws OBException
     *                 If something goes wrong.
     */
    public final int size() throws OBException {
        return tree.getSize();
    }

    /**
     * @return A String representation of the tree.
     */
    public final String toString() {
        String res = ":)";
        try {
            res = tree.toFuriaChanTree() + "|" + tree.getSize() + "|";
        } catch (Exception e) {
            assert false;
        }
        return res;
    }

    /**
     * Re-creates this object from the given byte stream
     * @param in
     *                A byte stream with the data that must be loaded.
     * @see org.ajmm.obsearch.Storable#load(com.sleepycat.bind.tuple.TupleInput)
     */
    public final void load(TupleInput in) throws OBException {
        short size = in.readShort();
        updateTree(in.readBytes(size));
    }

    /**
     * Stores this object into the given byte stream.
     * @param out
     *                The byte stream to be used
     * @see org.ajmm.obsearch.Storable#store(com.sleepycat.bind.tuple.TupleOutput)
     */
    public final void store(TupleOutput out) {
        String str = tree.toFuriaChanTree();
        out.writeShort(str.length());
        out.writeBytes(str);
    }

    /**
     * Returns true of this.tree.equals(obj.tree). For this distance function
     * this.distance(obj) == 0 implies that this.equals(obj) == true
     * @param obj
     *                Object to compare.
     * @return true if this == obj
     */
    public final boolean equals(final Object obj) {
        if (!(obj instanceof OBSlice)) {
            assert false;
            return false;
        }
        OBSlice o = (OBSlice) obj;
        return tree.equalsTree(o.tree);
    }

    /**
     * A hashCode based on the string representation of the tree.
     * @return a hash code of the string representation of this tree.
     */
    public final int hashCode() {
        return this.hashCode;
    }

}
