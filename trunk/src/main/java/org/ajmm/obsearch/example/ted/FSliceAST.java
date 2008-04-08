package org.ajmm.obsearch.example.ted;

import org.ajmm.obsearch.index.utils.IntegerHolder;

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
 * FSliceAST A tree that holds an internal id for each unique complete subtree
 * and a hash code computed on the string representation of this complete
 * subtree. Additionally, the number of repetitions is included. This helps to
 * make this algorithm O(n) for equal complete subtrees of two different trees.
 * Once we found that two complete subtrees m,j belonging to different trees T1
 * T2, we can compute their intersection in linear time.
 * @author Arnoldo Jose Muller Molina
 */
public class FSliceAST
        extends SliceAST {

    int hashCode = -1;

    public int id = -1;

    public IntegerHolder repetitions;

    public void update() {
        hashCode = super.toStringTree().hashCode();
    }

    public boolean equals(AST x) {
        return equals((Object) x);
    }

    public int hashCode() {
        return hashCode;
    }

    public boolean equals(Object o) {
        FSliceAST other = (FSliceAST) o;
        boolean res = fequalsTree(other);
        assert res == this.toStringTree().equals(other.toStringTree());
        return res;
    }

    private boolean fequalsTree(FSliceAST other) {
        if (other == null) {
            return false;
        }
        if (this.hashCode != other.hashCode) {
            return false;
        }
        if (!this.text.equals(other.text)) {
            return false;
        }
        if (this.decendants != other.decendants) {
            return false;
        }
        if (this.getLeft() != null) {
            return this.getLeft().fequalsTreeAux(other.getLeft());
        } else if (this.getLeft() == null && other.getLeft() == null) {
            return true;
        } else {
            return false;
        }
    }

    private boolean fequalsTreeAux(FSliceAST other) {
        if (other == null) {
            return false;
        }
        if (!this.text.equals(other.text)) {
            return false;
        }

        boolean res = true;
        FSliceAST left = this.getLeft();
        if (left != null) {
            res = left.fequalsTreeAux(other.getLeft());
        }
        if (res) {
            FSliceAST sib = this.getSibbling();
            if (sib != null) {
                res = sib.fequalsTreeAux(other.getSibbling());
            }
        }
        return res;
    }

    public FSliceAST getLeft() {
        return (FSliceAST) this.getFirstChild();
    }

    public FSliceAST getSibbling() {
        return (FSliceAST) this.getNextSibling();
    }

}
