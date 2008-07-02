package org.ajmm.obsearch.index;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.ajmm.obsearch.Index;
import org.ajmm.obsearch.OB;

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
public final class IndexFactory < O extends OB > {

    /**
     * Utility class should not have a public constructor.
     */
    public IndexFactory() {

    }
    
    /**
     * Returns true if the given OB database is frozen, false otherwise.
     * @param OBFolder
     * @return true if the given OB datatabase is frozen, false otherwise.
     */
    public boolean isFrozen(final File OBFolder){
        return new File(OBFolder,
                Index.METADATA_FILENAME).exists();
    }

    /**
     * Creates an index from the given XML. Users are responsible of knowing
     * what type of index is being read. Users are responsible of casting the
     * result.
     * @param xml
     *                String with the serialized index.
     * @return An instantiated index.
     */
    public Index < O > createFromXML(final String xml) {
        XStream xstream = new XStream();
        return (Index < O >) xstream.fromXML(xml);
    }

    /**
     * Creates an index from the given OBSearch database folder. Users are
     * responsible of knowing what type of index is being read and casting the result
     * to the appropriate index.
     * @param xmlFileName
     *                File name of the folder in which the database is found
     * @return An instantiated index.
     * @throws FileNotFoundException
     *                 if the give file does not exist
     */
    public Index < O > createFromOBFolder(final File OBFolder)
            throws FileNotFoundException {
        XStream xstream = new XStream();
        FileInputStream fs = new FileInputStream(new File(OBFolder,
                Index.METADATA_FILENAME));
        BufferedInputStream bf = new BufferedInputStream(fs);
        return (Index<O>) xstream.fromXML(bf);
    }

}
