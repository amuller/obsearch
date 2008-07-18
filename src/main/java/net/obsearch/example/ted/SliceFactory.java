package net.obsearch.example.ted;

import java.io.StringReader;

import org.ajmm.obsearch.example.SliceLexer;
import org.ajmm.obsearch.example.SliceParser;

import antlr.RecognitionException;
import antlr.TokenStreamException;



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
 * SliceFactory Creates SliceAST objects and SliceForest objects from strings.
 * @author Arnoldo Jose Muller Molina
 */
public class SliceFactory {

    /**
     * Creates a tree from a slice string definition
     * @param x
     * @return
     */
    public static SliceASTIds createSliceAST(String x)
            throws RecognitionException, TokenStreamException {
        SliceLexer lexer = new SliceLexer(new StringReader(x));
        SliceParser parser = new SliceParser(lexer);
        parser.setASTNodeClass("furia.slice.SliceASTIds");
        parser.slice();
        SliceASTIds s = (SliceASTIds) parser.getAST();
        s.updateIdInfo();
        s.updateContains();
        return s;
    }

    

    /**
     * creates the base clase of the ast. it is lighter
     * @param x
     * @return
     * @throws RecognitionException
     * @throws TokenStreamException
     */
    public static SliceAST createSliceASTLean(String x)
            throws RecognitionException, TokenStreamException {
        SliceLexer lexer = new SliceLexer(new StringReader(x));
        SliceParser parser = new SliceParser(lexer);
        parser.setASTNodeClass("net.obsearch.example.ted.SliceAST");
        try {
            parser.slice();
        } catch (RecognitionException e) {
            e.fileName = x + " " + e.fileName;
            throw e;
        }
        return (SliceAST) parser.getAST();
    }

    public static SliceForest createSliceForest(String x)
            throws RecognitionException, TokenStreamException {
        // STD was the fastest data structure.
        return new SliceForestStd(SliceFactory.createSliceASTForStandardTed(x));
    }

    public static SliceASTForStandardTed createSliceASTForStandardTed(String x)
            throws RecognitionException, TokenStreamException {
        SliceLexer lexer = new SliceLexer(new StringReader(x));
        SliceParser parser = new SliceParser(lexer);
        parser.setASTNodeClass("net.obsearch.example.ted.SliceASTForStandardTed");
        parser.slice();
        SliceASTForStandardTed s = (SliceASTForStandardTed) parser.getAST();
        s.updateIdInfo();
        s.updateContains();
        return s;
    }

    public static SliceForest createEmptySliceForest() {
        return new SliceForestStd();
    }

}
