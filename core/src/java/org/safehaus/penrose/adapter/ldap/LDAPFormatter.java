package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.entry.Entry;
import org.safehaus.penrose.util.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class LDAPFormatter {

    public static Logger log = LoggerFactory.getLogger(LDAPFormatter.class);

    public static void printEntry(Entry entry) throws Exception {

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Entry: "+entry.getDn(), 80));

        for (Iterator i=entry.getSourceNames().iterator(); i.hasNext(); ) {
            String sourceName = (String)i.next();
            Attributes attributes = entry.getSourceValues(sourceName);

            for (Iterator j=attributes.getAll().iterator(); j.hasNext(); ) {
                Attribute attribute = (Attribute)j.next();
                String fieldName = sourceName+"."+attribute.getName();

                for (Iterator k=attribute.getValues().iterator(); k.hasNext(); ) {
                    Object value = k.next();
                    String className = value.getClass().getName();
                    className = className.substring(className.lastIndexOf(".")+1);

                    log.debug(Formatter.displayLine(" - "+fieldName+": "+value+" ("+className+")", 80));
                }
            }
        }

        log.debug(Formatter.displaySeparator(80));
    }

}
