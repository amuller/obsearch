package net.obsearch.example.ted;

import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;


import antlr.BaseAST;
import antlr.CommonAST;
import antlr.Token;
import antlr.collections.AST;

/*
 Furia-chan: An Open Source software license violation detector.    
 Copyright (C) 2008 Kyushu Institute of Technology

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
 * SliceAST This class provides extra functionality required by tree edit
 * distance algorithms and the like
 * @author Arnoldo Jose Muller Molina
 */

public class SliceAST
        extends BaseAST {

    protected int decendants = -1;

    protected String text;

    public int updateDecendantInformation() {
        decendants = 0;
        SliceAST n = (SliceAST) this.getLeftmostChild();
        while (n != null) {
            decendants += n.updateDecendantInformation();
            n = (SliceAST) n.getNextSibling();
        }
        return decendants + 1;
    }

    /*
     * public int getDescendants(){ int decendants = 0; SliceAST n =
     * this.getLeftmostChild(); while(n != null){ decendants +=
     * n.getDescendants(); decendants++; n = (SliceAST)n.getNextSibling(); }
     * return decendants; }
     */

    public int getDescendants() {
        if (decendants == -1) {
            this.updateDecendantInformation();
        }
        return decendants;
    }

    public int getSize() {
        return getDescendants() + 1;
    }

    public SliceAST findFirstNodeThatMatches(String label) {
        SliceAST result = null;
        if (this.text.equals(label)) {
            result = this;
        } else {
            SliceAST n = this.getLeftmostChild();
            while (n != null && result == null) {
                result = n.findFirstNodeThatMatches(label);
                n = (SliceAST) n.getNextSibling();
            }
        }
        return result;
    }

    /** Get the token text for this node */
    public String getText() {
        return text;
    }

    /** Get the token type for this node */
    public int getType() {
        return -1;
    }

    public void initialize(int t, String txt) {
        setType(t);
        setText(txt);
    }

    public void initialize(AST t) {
        setText(t.getText());
        setType(t.getType());
    }

    public SliceAST() {
    }

    public SliceAST(int t, String txt) {
        initialize(t, txt);
    }

    public SliceAST(Token tok) {
        initialize(tok);
    }

    public SliceAST(SliceAST t) {
        initialize(-1, t.text);
    }

    public void initialize(Token tok) {
        setText(tok.getText());
        setType(tok.getType());
    }

    /** Set the token text for this node */
    public void setText(String text_) {
        text = text_;
    }

    /** Set the token type for this node */
    public void setType(int ttype_) {

    }

    public SliceAST getLeftmostChild() {
        return (SliceAST) super.getFirstChild();
    }

    /** Print out a child-sibling tree in LISP notation */
    public String prettyPrint() {
        SliceAST t = this;
        String ts = "";
        if (t.getFirstChild() != null)
            ts += " (";
        ts += " " + this.toString();
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

    /** Print out a child-sibling tree in Q notation */
    public String toQ() {
        SliceAST t = this;
        String ts = "";
        ts += this.toString();
        if (t.getFirstChild() != null) {
            ts += "(";
            ts += ((SliceAST) t.getFirstChild()).toQ();
            ts += ")";
        }

        if (t.getNextSibling() != null) {
            ts += ",";
            ts += ((SliceAST) t.getNextSibling()).toQ();
        }

        return ts;
    }

    /*
     * little speed up to the normal equalsTree method
     * @see antlr.BaseAST#equalsTree(antlr.collections.AST)
     */
    public boolean equalsTree(AST t) {
        SliceAST j = (SliceAST) t;
        if (j.getSize() != this.getSize()) { // little speed up! ;)
            return false;
        } else {
            return super.equalsTree(t);
        }
    }

    /*
     * 1 = equal 0 = not equal 2 = equal if we rename the root 3 = children not
     * equal but the same root
     * @see antlr.BaseAST#equalsTree(antlr.collections.AST)
     */
    public int detailedTreeComparison(AST t) {
        SliceAST j = (SliceAST) t;
        if (j.getSize() != this.getSize()) { // little speed up! ;)
            if (this.equals(t)) {
                return 3;
            } else {
                return 0;
            }
        } else {
            return detailedTreeAux(t);
        }
    }

    /**
     * Is tree rooted at 'this' equal to 't'? The siblings of 'this' are
     * ignored.
     */
    protected int detailedTreeAux(AST t) {
        int res = -1;
        // check roots first.

        // if roots match, do full list match test on children.
        if (this.getFirstChild() != null) {
            if (this.getFirstChild().equalsList(t.getFirstChild())) {
                res = 2;
            } else {
                res = 0;
            }
        }
        // sibling has no kids, make sure t doesn't either
        else if (t.getFirstChild() != null) {
            res = 0;
        }

        if (this.equals(t) && (res == 2 || res == -1)) {
            res = 1;
        }
        if (res == -1) {
            res = 0;
        }
        if (this.equals(t) && res == 0) {
            res = 3;
        }

        return res;
    }

    /*
     * get a list of the nodes in depth first order
     */
    public synchronized List < SliceAST > depthFirst() {
        LinkedList < SliceAST > res = new LinkedList < SliceAST >();
        depthFirstAux(res);
        return res;
    }

    protected void depthFirstAux(LinkedList < SliceAST > res) {
        res.add(this);
        SliceAST down = (SliceAST) this.getFirstChild();
        if (down != null) {
            down.depthFirstAux(res);
        }
        SliceAST right = (SliceAST) this.getNextSibling();
        if (right != null) {
            right.depthFirstAux(res);
        }
    }

    // TODO refactor all these things. hack to keep the unit tests working
    // dummy
    /**
     * public int updateDecendantInformation(){ assert false; return -1; }
     */
    // dummy
    public void updateIdInfo() {
        assert false;
    }

    // dummy
    public void updateContains() {
        assert false;
    }

    // dummy
    public int getId() {
        assert false;
        return -1;
    }

    // dummy
    public boolean containsNode(int i) {
        assert false;
        return false;
    }

    public final void toFuriaChanTreeAux(StringBuilder ts) {
        
        AST t = this;
        ts.append(this.toString());
        if (t.getFirstChild() != null) {
            ts.append("(");

            ((SliceAST) t.getFirstChild()).toFuriaChanTreeAux(ts);

            ts.append(")");
        }
        if(t.getNextSibling() != null){
            ts.append(",");
            ((SliceAST)t.getNextSibling()).toFuriaChanTreeAux(ts);
        }
    }
}
