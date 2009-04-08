package net.obsearch.index.pptree;

import java.util.List;
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
* A Space Tree is like a KDB-tree. It is used to partition the space and
* improve the efficiency of the pyramid technique.
* @author Arnoldo Jose Muller Molina
* @since 0.9
*/
public abstract class AbstractSpaceTreeNode implements SpaceTree {

    /**
     * Holds the center of the current node.
     * It is used to explore first the spaces whose center is closest
     * to our query.
     */
    //TODO: remove this to save some memory
    //private float [] center;
    
    AbstractSpaceTreeNode(double [] center){
       // this.center = center;
    }

    public final double [] getCenter(){
        //return center;
        return null;
    }

}
