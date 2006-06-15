package org.safehaus.penrose.util;

import org.apache.log4j.Logger;
import org.safehaus.penrose.session.PenroseSearchControls;

import javax.naming.directory.DirContext;
import javax.naming.directory.Attribute;
import javax.naming.NamingEnumeration;

/**
 * @author Endi S. Dewata
 */
public class LDAPUtil {

    public static Logger log = Logger.getLogger(LDAPUtil.class);

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

    public static String getScope(PenroseSearchControls sc) {
        return getScope(sc.getScope());
    }

    public static String getScope(int scope) {

        switch (scope) {
            case PenroseSearchControls.SCOPE_BASE:
                return "base";

            case PenroseSearchControls.SCOPE_ONE:
                return "one level";

            case PenroseSearchControls.SCOPE_SUB:
                return "subtree";
        }

        return null;
    }

    public static String getDereference(PenroseSearchControls sc) {
        return getDereference(sc.getDereference());
    }

    public static String getDereference(int deref) {

        switch (deref) {
            case PenroseSearchControls.DEREF_NEVER:
                return "never";

            case PenroseSearchControls.DEREF_SEARCHING:
                return "searching";

            case PenroseSearchControls.DEREF_FINDING:
                return "finding";

            case PenroseSearchControls.DEREF_ALWAYS:
                return "always";
        }

        return null;
    }
}
