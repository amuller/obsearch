<@pp.dropOutputFile />
<#list types as t>
<#assign type = t.name>
<#assign Type = t.name?cap_first>
<@pp.changeOutputFile name="OBQuery"+Type+".java" />
package org.ajmm.obsearch.query;
import org.ajmm.obsearch.ob.OB${Type};
import org.ajmm.obsearch.result.OBResult${Type};
import org.ajmm.obsearch.result.OBPriorityQueue${Type};

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
 * Object used to store a query request.
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */


public class OBQuery${Type}<O extends OB${Type}> extends OBResult${Type}<O> {


    /**
     * Holds the results for the query.
     */
    protected OBPriorityQueue${Type}<O> result;

    /**
     * Constructor.
     */
    public OBQuery${Type}(){
    }

    /**
     * Creates a new OBQuery${Type} object.
     * @param object
     *            The object that will be matched.
     * @param distance
     *            The distance to be used for the match.
     * @param result
     *            The priority queue were the results will be stored.
     */
    public OBQuery${Type}(O object, ${type} distance, OBPriorityQueue${Type}<O> result){

        super(object,-1,distance);
        this.result = result;
    }

    /**
     * @return The current results of the matching.
     */
    public OBPriorityQueue${Type}<O> getResult(){
        return result;
    }

    /**
     * Set the results of the matching to a new object.
     * @param result
     *            The new result.
     */
    public void setResult(OBPriorityQueue${Type}<O> result){
        this.result = result;
    }

}

</#list>
