package org.ajmm.obsearch.index;

import org.ajmm.obsearch.Index;

import com.thoughtworks.xstream.XStream;

/**
 * An index factory creates Indexes from their serialized versions Currently we
 * use xstream http://xstream.codehaus.org/ to perform the
 * serialization/un-serialization process
 */
public class IndexFactory {

    /**
     * Creates an index from the given XML. Users are responsible of knowing
     * what type of index is being read. Users are responsible of casting the
     * xml.
     * @param xml
     * @return
     */
    public static Index createFromXML(String xml) {
        XStream xstream = new XStream();
        return (Index) xstream.fromXML(xml);
    }

}
