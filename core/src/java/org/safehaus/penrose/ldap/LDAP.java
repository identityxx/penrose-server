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
package org.safehaus.penrose.ldap;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.NamingEnumeration;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class LDAP {

    public static Logger log = LoggerFactory.getLogger(LDAP.class);

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

        for (String name : adds) {
            Attribute attribute = newAttributes.get(name);
            modifications.add(new Modification(Modification.ADD, attribute));
        }

        Collection<String> deletes = new ArrayList<String>();
        deletes.addAll(oldNames);
        deletes.removeAll(newNames);

        for (String name : deletes) {
            Attribute attribute = oldAttributes.get(name);
            modifications.add(new Modification(Modification.DELETE, attribute));
        }

        Collection<String> modifies = new ArrayList<String>();
        modifies.addAll(oldNames);
        modifies.retainAll(newNames);

        for (String name : modifies) {
            Attribute attribute = newAttributes.get(name);
            modifications.add(new Modification(Modification.REPLACE, attribute));
        }

        return modifications;
    }

    public static String escape(String value) {

        StringBuilder sb = new StringBuilder();
        char chars[] = value.toCharArray();

        boolean quote = chars[0] == ' ' || chars[chars.length-1] == ' ';
        boolean space = false;

        for (char c : chars) {
            // checking special characters
            if (c == ',' || c == '=' || c == '+'
                    || c == '<' || c == '>'
                    || c == '#' || c == ';'
                    || c == '\\' || c == '"') {
                sb.append('\\');
            }

            if (c == '\n') {
                quote = true;
            }

            // checking double space
            if (c == ' ') {
                if (space) {
                    quote = true;
                } else {
                    space = true;
                }
            } else {
                space = false;
            }

            sb.append(c);
        }

        if (quote) {
            sb.insert(0, '"');
            sb.append('"');
        }

        return sb.toString();
    }

    public static String unescape(String value) {
        char chars[] = value.toCharArray();

        if (chars[0] == '"' && chars[chars.length-1] == '"') {
            return value.substring(1, chars.length-1);
        }

        StringBuilder sb = new StringBuilder();

        for (int i=0; i<chars.length; i++) {
            char c = chars[i];
            if (c == '\\') {
                c = chars[++i];
            }
            sb.append(c);
        }

        return sb.toString();
    }
}
