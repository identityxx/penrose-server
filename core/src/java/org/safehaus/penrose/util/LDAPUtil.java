package org.safehaus.penrose.util;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.DirContext;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;

/**
 * @author Endi S. Dewata
 */
public class LDAPUtil {

    public static Logger log = LoggerFactory.getLogger(LDAPUtil.class);

    public static boolean isBinary(Attribute attribute) throws Exception {

        log.debug("Attribute "+attribute.getID()+" definition:");

        try {
            DirContext ctx = attribute.getAttributeDefinition();
            for (NamingEnumeration ne = ctx.list(""); ne.hasMore(); ) {
                Object o = ne.next();
                log.debug(" - Syntax: "+o+" ("+o.getClass().getName()+")");
            }
        } catch (Exception e) {
            log.debug(" - "+e.getClass().getName()+": "+e.getMessage());
        }

        boolean binary = false;

        try {
            DirContext ctx = attribute.getAttributeSyntaxDefinition();
            for (NamingEnumeration ne = ctx.list(""); ne.hasMore(); ) {
                Object o = ne.next();
                log.debug(" - Syntax: "+o+" ("+o.getClass().getName()+")");
            }
        } catch (Exception e) {
            log.debug(" - "+e.getClass().getName()+": "+e.getMessage());
            binary = "SyntaxDefinition/1.3.6.1.4.1.1466.115.121.1.40".equals(e.getMessage());
        }

        return binary;
    }
}
