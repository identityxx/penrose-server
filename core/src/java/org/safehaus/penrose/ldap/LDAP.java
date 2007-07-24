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
import org.ietf.ldap.LDAPException;

import javax.naming.directory.*;
import javax.naming.*;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class LDAP {

    public static Logger log = LoggerFactory.getLogger(LDAP.class);

    public static final int SUCCESS = 0;
    public static final int OPERATIONS_ERROR = 1;
    public static final int PROTOCOL_ERROR = 2;
    public static final int TIME_LIMIT_EXCEEDED = 3;
    public static final int SIZE_LIMIT_EXCEEDED = 4;
    public static final int COMPARE_FALSE = 5;
    public static final int COMPARE_TRUE = 6;
    public static final int AUTH_METHOD_NOT_SUPPORTED = 7;
    public static final int STRONG_AUTH_REQUIRED = 8;
    public static final int REFERRAL = 10;
    public static final int ADMIN_LIMIT_EXCEEDED = 11;
    public static final int UNAVAILABLE_CRITICAL_EXTENSION = 12;
    public static final int CONFIDENTIALITY_REQUIRED = 13;
    public static final int SASL_BIND_IN_PROGRESS = 14;
    public static final int NO_SUCH_ATTRIBUTE = 16;
    public static final int UNDEFINED_ATTRIBUTE_TYPE = 17;
    public static final int INAPPROPRIATE_MATCHING = 18;
    public static final int CONSTRAINT_VIOLATION = 19;
    public static final int ATTRIBUTE_OR_VALUE_EXISTS = 20;
    public static final int INVALID_ATTRIBUTE_SYNTAX = 21;
    public static final int NO_SUCH_OBJECT = 32;
    public static final int ALIAS_PROBLEM = 33;
    public static final int INVALID_DN_SYNTAX = 34;
    public static final int IS_LEAF = 35;
    public static final int ALIAS_DEREFERENCING_PROBLEM = 36;
    public static final int INAPPROPRIATE_AUTHENTICATION = 48;
    public static final int INVALID_CREDENTIALS = 49;
    public static final int INSUFFICIENT_ACCESS_RIGHTS = 50;
    public static final int BUSY = 51;
    public static final int UNAVAILABLE = 52;
    public static final int UNWILLING_TO_PERFORM = 53;
    public static final int LOOP_DETECT = 54;
    public static final int NAMING_VIOLATION = 64;
    public static final int OBJECT_CLASS_VIOLATION = 65;
    public static final int NOT_ALLOWED_ON_NONLEAF = 66;
    public static final int NOT_ALLOWED_ON_RDN = 67;
    public static final int ENTRY_ALREADY_EXISTS = 68;
    public static final int OBJECT_CLASS_MODS_PROHIBITED = 69;
    public static final int AFFECTS_MULTIPLE_DSAS = 71;
    public static final int OTHER = 80;
    public static final int SERVER_DOWN = 81;
    public static final int LOCAL_ERROR = 82;
    public static final int ENCODING_ERROR = 83;
    public static final int DECODING_ERROR = 84;
    public static final int LDAP_TIMEOUT = 85;
    public static final int AUTH_UNKNOWN = 86;
    public static final int FILTER_ERROR = 87;
    public static final int USER_CANCELLED = 88;
    public static final int NO_MEMORY = 90;
    public static final int CONNECT_ERROR = 91;
    public static final int LDAP_NOT_SUPPORTED = 92;
    public static final int CONTROL_NOT_FOUND = 93;
    public static final int NO_RESULTS_RETURNED = 94;
    public static final int MORE_RESULTS_TO_RETURN = 95;
    public static final int CLIENT_LOOP = 96;
    public static final int REFERRAL_LIMIT_EXCEEDED = 97;
    public static final int INVALID_RESPONSE = 100;
    public static final int AMBIGUOUS_RESPONSE = 101;
    public static final int TLS_NOT_SUPPORTED = 112;

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

    public static int getReturnCode(Throwable t) {

        if (t instanceof LDAPException) return ((LDAPException)t).getResultCode();

        if (t instanceof CommunicationException) return PROTOCOL_ERROR;
        if (t instanceof TimeLimitExceededException) return TIME_LIMIT_EXCEEDED;
        if (t instanceof SizeLimitExceededException) return SIZE_LIMIT_EXCEEDED;
        if (t instanceof AuthenticationException) return INVALID_CREDENTIALS;
        if (t instanceof NoPermissionException) return INSUFFICIENT_ACCESS_RIGHTS;
        if (t instanceof NoSuchAttributeException) return NO_SUCH_ATTRIBUTE;
        if (t instanceof InvalidAttributeIdentifierException) return UNDEFINED_ATTRIBUTE_TYPE;
        if (t instanceof InvalidSearchFilterException) return INAPPROPRIATE_MATCHING;
        if (t instanceof AttributeInUseException) return ATTRIBUTE_OR_VALUE_EXISTS;
        if (t instanceof NameNotFoundException) return NO_SUCH_OBJECT;
        if (t instanceof NameAlreadyBoundException) return ENTRY_ALREADY_EXISTS;
        if (t instanceof ContextNotEmptyException) return NOT_ALLOWED_ON_NONLEAF;

        return OPERATIONS_ERROR;
    }

    public static String getMessage(int rc) {
        return LDAPException.resultCodeToString(rc);
    }

    public static LDAPException createException(Exception e) {
        if (e instanceof LDAPException) return (LDAPException)e;

        int rc = getReturnCode(e);
        return createException(rc, e.getMessage(), e);
    }

    public static LDAPException createException(int rc) {
        return createException(rc, getMessage(rc), null);
    }

    public static LDAPException createException(int rc, Exception e) {
        return createException(rc, e.getMessage(), e);
    }

    public static LDAPException createException(int rc, String message) {
        return createException(rc, message, null);
    }

    public static LDAPException createException(int rc, String message, Exception exception) {
        return new LDAPException(getMessage(rc), rc, message, exception);
    }
}
