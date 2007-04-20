/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.util;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.Modification;

import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class LDAPUtil {

    public static Logger log = LoggerFactory.getLogger(LDAPUtil.class);

    public static boolean isBinary(javax.naming.directory.Attribute attribute) throws Exception {

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

    public static SearchRequest convert(SearchControls sc) {
        SearchRequest request = new SearchRequest();
        request.setScope(sc.getSearchScope());
        request.setSizeLimit(sc.getCountLimit());
        request.setTimeLimit(sc.getTimeLimit());
        request.setAttributes(sc.getReturningAttributes());
        request.setTypesOnly(sc.getReturningObjFlag());
        return request;
    }

    public static String getScope(int scope) {

        switch (scope) {
            case SearchRequest.SCOPE_BASE:
                return "base";

            case SearchRequest.SCOPE_ONE:
                return "one level";

            case SearchRequest.SCOPE_SUB:
                return "subtree";
        }

        return null;
    }

    public static String getDereference(SearchRequest sc) {
        return getDereference(sc.getDereference());
    }

    public static String getDereference(int deref) {

        switch (deref) {
            case SearchRequest.DEREF_NEVER:
                return "never";

            case SearchRequest.DEREF_SEARCHING:
                return "searching";

            case SearchRequest.DEREF_FINDING:
                return "finding";

            case SearchRequest.DEREF_ALWAYS:
                return "always";
        }

        return null;
    }

    public static String getModificationOperations(int op) {

        switch (op) {
            case DirContext.ADD_ATTRIBUTE:
                return "add";

            case DirContext.REMOVE_ATTRIBUTE:
                return "delete";

            case DirContext.REPLACE_ATTRIBUTE:
                return "replace";
        }

        return null;
    }

    public static Collection<Modification> createModifications(
            Attributes oldAttributes,
            Attributes newAttributes
    ) {

        Collection<Modification> modifications = new ArrayList<Modification>();

        Collection<String> oldNames = oldAttributes.getNames();
        Collection<String> newNames = newAttributes.getNames();

        Collection<String> adds = new ArrayList<String>();
        adds.addAll(newNames);
        adds.removeAll(oldNames);

        for (Iterator i=adds.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Attribute attribute = newAttributes.get(name);
            modifications.add(new Modification(Modification.ADD, attribute));
        }

        Collection<String> deletes = new ArrayList<String>();
        deletes.addAll(oldNames);
        deletes.removeAll(newNames);

        for (Iterator i=deletes.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Attribute attribute = oldAttributes.get(name);
            modifications.add(new Modification(Modification.DELETE, attribute));
        }

        Collection<String> modifies = new ArrayList<String>();
        modifies.addAll(oldNames);
        modifies.retainAll(newNames);

        for (Iterator i=modifies.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Attribute attribute = newAttributes.get(name);
            modifications.add(new Modification(Modification.REPLACE, attribute));
        }

        return modifications;
    }
}
