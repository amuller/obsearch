package org.ajmm.obsearch.example;

import java.util.LinkedList;
import java.util.List;

import antlr.BaseAST;
import antlr.Token;
import antlr.collections.AST;

/*
 OBSearch: a distributed similarity search engine
 This project is to similarity search what 'bit-torrent' is to downloads.
 Copyright (C)  2007 Arnoldo Jose Muller Molina

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
 * This class provides extra functionality required by tree edit distance
 * algorithms and the like.
 * @author Arnoldo Jose Muller Molina
 * @version %I%, %G%
 * @since 0.0
 */

public class SliceAST
        extends BaseAST {

    /**
     * Serial version of the class.
     */
    private static final long serialVersionUID = -7669115912647058933L;

    /**
     * Number of children this node has.
     */
    protected int decendants = -1;

    /**
     * The text of this node.
     */
    protected String text;

    /**
     * Updates decendant information.
     * @return An integer that represents the number of children of this node.
     */
    public final int updateDecendantInformation() {
        decendants = 0;
        SliceAST n = getLeftmostChild();
        while (n != null) {
            decendants += n.updateDecendantInformation();
            n = (SliceAST) n.getNextSibling();
        }
        return decendants + 1;
    }

    /**
     * Returns the number of decendants of this node.
     * @return The number of children of this node.
     */
    public final int getDescendants() {
        if (decendants == -1) {
            updateDecendantInformation();
        }
        return decendants;
    }

    /**
     * @return The size of the Tree (includes the root node)
     */
    public final int getSize() {
        return getDescendants() + 1;
    }

    /**
     * Get the token text for this node.
     * @return The text of the node.
     */
    @Override
    public final String getText() {
        return text;
    }

    /**
     * Get the token type for this node.
     * @return The type of node
     */
    @Override
    public final int getType() {
        return -1;
    }

    /**
     * Initialize the node.
     * @param t
     *            Node type
     * @param txt
     *            Node tag
     */
    public final void initialize(final int t, final String txt) {
        setType(t);
        setText(txt);
    }

    /**
     * Initialize the node from another node.
     * @param t
     *            Another node.
     */
    public final void initialize(final AST t) {
        setText(t.getText());
        setType(t.getType());
    }

    /**
     * Default constructor.
     */
    public SliceAST() {
    }

    /**
     * Initialize the node.
     * @param t
     *            Node type
     * @param txt
     *            Node text
     */
    public SliceAST(final int t, final String txt) {
        text = txt;
    }

    /**
     * Initialize the node from a token.
     * @param tok
     *            The token to use as initializer.
     */
    public SliceAST(final Token tok) {
        text = tok.getText();
    }

    /**
     * Clone the node with this constructor.
     * @param t
     *            Another SliceAST
     */
    public SliceAST(final SliceAST t) {
        text = t.text;
    }

    /**
     * Initialize from the given token.
     * @param tok
     *            A token.
     */
    @Override
    public final void initialize(final Token tok) {
        setText(tok.getText());
        setType(tok.getType());
    }

    /**
     * Set the token text for this node.
     * @param text_
     *            The text to use.
     */
    @Override
    public final void setText(final String text_) {
        text = text_;
    }

    /**
     * Set the token type for this node. Currently ignored.
     * @param ttype_
     *            Type to use
     */
    @Override
    public final void setType(final int ttype_) {

    }

    /**
     * Get the leftmost child of this node.
     * @return The leftmost child of this node.
     */
    public final SliceAST getLeftmostChild() {
        return (SliceAST) super.getFirstChild();
    }

    /**
     * Print out a child-sibling tree in LISP notation.
     * @return A child-sibling tree in LISP notation
     */
    public final String prettyPrint() {
        final SliceAST t = this;
        String ts = "";
        if (t.getFirstChild() != null)
            ts += " (";
        ts += " " + toString();
        if (t.getFirstChild() != null) {
            ts += ((SliceAST) t.getFirstChild()).prettyPrint();
        }
        if (t.getFirstChild() != null)
            ts += " )";
        if (t.getNextSibling() != null) {
            ts += ((SliceAST) t.getNextSibling()).prettyPrint();
        }
        return ts;
    }

    

    /**
     * Little speed up to the normal equalsTree method.
     * Returns tree if this and t are equal
     * @param t Another tree to compare.
     * @return True if both trees are equal.
     * @see antlr.BaseAST#equalsTree(antlr.collections.AST)
     */
    @Override
    public final boolean equalsTree(final AST t) {
        final SliceAST j = (SliceAST) t;
        if (j.getSize() != getSize()) { // little speed up! ;)
            return false;
        } else {
            return super.equalsTree(t);
        }
    }


    /**
     * @return  A list of the nodes in depth first order
     */
    public final synchronized List < SliceAST > depthFirst() {
        final LinkedList < SliceAST > res = new LinkedList < SliceAST >();
        depthFirstAux(res);
        return res;
    }

    /**
     * Auxiliary function for {@link #depthFirst()}.
     * @param res Where the result will be stored.
     */
    protected final void depthFirstAux(final LinkedList < SliceAST > res) {
        res.add(this);
        final SliceAST down = (SliceAST) getFirstChild();
        if (down != null) {
            down.depthFirstAux(res);
        }
        final SliceAST right = (SliceAST) getNextSibling();
        if (right != null) {
            right.depthFirstAux(res);
        }
    }


}
