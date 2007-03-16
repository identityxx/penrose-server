package org.safehaus.penrose.apacheds;

import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.mapping.EntryMapping;

import javax.naming.directory.SearchResult;
import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EntryTool {

    public static SearchResult createSearchResult(Entry entry) throws Exception {

        //log.debug("Converting "+entry.getDn());

        Attributes attributes = entry.getAttributes();
        javax.naming.directory.Attributes attrs = new javax.naming.directory.BasicAttributes();

        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String name = attribute.getName();
            Collection values = attribute.getValues();

            javax.naming.directory.Attribute attr = new javax.naming.directory.BasicAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();

                //String className = value.getClass().getName();
                //className = className.substring(className.lastIndexOf(".")+1);
                //log.debug(" - "+name+": "+value+" ("+className+")");

                if (value instanceof byte[]) {
                    attr.add(value);

                } else {
                    attr.add(value.toString());
                }
            }

            attrs.put(attr);
        }

        return new javax.naming.directory.SearchResult(entry.getDn().toString(), entry, attrs);
    }
}
