package org.ajmm.obsearch.index;

import org.ajmm.obsearch.Index;

import com.thoughtworks.xstream.XStream;

/**
 * An index factory creates Indexes from their serialized versions. Currently we
 * use xstream http://xstream.codehaus.org/ to perform the
 * serialization/de-serialization process
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
     * xml.
     * @param xml String with the serialized index.
     * @return An instantiated index.
     */
    public static Index createFromXML(final String xml) {
        XStream xstream = new XStream();
        return (Index) xstream.fromXML(xml);
    }

}
