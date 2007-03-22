package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.util.Formatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class LDAPFormatter {

    public static Logger log = LoggerFactory.getLogger(LDAPFormatter.class);

    public static void printRecord(DN dn, AttributeValues record) throws Exception {
        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Record: "+dn, 80));
        for (Iterator i=record.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection list = (Collection)record.get(name);

            for (Iterator j=list.iterator(); j.hasNext(); ) {
                Object value = j.next();
                String className = value.getClass().getName();
                className = className.substring(className.lastIndexOf(".")+1);

                log.debug(Formatter.displayLine(" - "+name+": "+value+" ("+className+")", 80));
            }
        }
        log.debug(Formatter.displaySeparator(80));
    }

}
