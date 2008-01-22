package org.ajmm.obsearch.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.ajmm.obsearch.Index;

import com.thoughtworks.xstream.XStream;

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
 * An index factory creates Indexes from their serialized versions. Currently we
 * use xstream http://xstream.codehaus.org/ to perform the
 * serialization/de-serialization process
 * @author Arnoldo Jose Muller Molina
 * @since 0.7
 */
public final class IndexFactory {

    /**
     * Utility class should not have a public constructor.
     */
    private IndexFactory() {

    }

    /**
     * Creates an index from the given XML. Users are responsible of knowing
     * what type of index is being read. Users are responsible of casting the
     * result.
     * @param xml
     *            String with the serialized index.
     * @return An instantiated index.
     */
    public static Index createFromXML(final String xml) {
        XStream xstream = new XStream();        
        return (Index) xstream.fromXML(xml);
    }
    
    /**
     * Creates an index from the given XML file. Users are responsible of knowing
     * what type of index is being read. Users are responsible of casting the
     * result.
     * @param xmlFileName
     *            File name of the xml file that will be loaded
     * @return An instantiated index.
     * @throws FileNotFoundException if the give file does not exist
     */
    public static Index createFromXML(final File xmlFileName) throws FileNotFoundException {
        XStream xstream = new XStream();  
        FileInputStream fs = new FileInputStream(xmlFileName);
        BufferedInputStream bf = new BufferedInputStream(fs);
        return (Index) xstream.fromXML(bf);
    }

}
