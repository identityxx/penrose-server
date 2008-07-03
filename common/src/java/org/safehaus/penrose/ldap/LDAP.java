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

    public final static DN ROOT_DSE_DN = new DN("");
    public final static DN SCHEMA_DN   = new DN("cn=Subschema");

    public final static int SUCCESS = 0;
    public final static int OPERATIONS_ERROR = 1;
    public final static int PROTOCOL_ERROR = 2;
    public final static int TIME_LIMIT_EXCEEDED = 3;
    public final static int SIZE_LIMIT_EXCEEDED = 4;
    public final static int COMPARE_FALSE = 5;
    public final static int COMPARE_TRUE = 6;
    public final static int AUTH_METHOD_NOT_SUPPORTED = 7;
    public final static int STRONG_AUTH_REQUIRED = 8;
    public final static int REFERRAL = 10;
    public final static int ADMIN_LIMIT_EXCEEDED = 11;
    public final static int UNAVAILABLE_CRITICAL_EXTENSION = 12;
    public final static int CONFIDENTIALITY_REQUIRED = 13;
    public final static int SASL_BIND_IN_PROGRESS = 14;
    public final static int NO_SUCH_ATTRIBUTE = 16;
    public final static int UNDEFINED_ATTRIBUTE_TYPE = 17;
    public final static int INAPPROPRIATE_MATCHING = 18;
    public final static int CONSTRAINT_VIOLATION = 19;
    public final static int ATTRIBUTE_OR_VALUE_EXISTS = 20;
    public final static int INVALID_ATTRIBUTE_SYNTAX = 21;
    public final static int NO_SUCH_OBJECT = 32;
    public final static int ALIAS_PROBLEM = 33;
    public final static int INVALID_DN_SYNTAX = 34;
    public final static int IS_LEAF = 35;
    public final static int ALIAS_DEREFERENCING_PROBLEM = 36;
    public final static int INAPPROPRIATE_AUTHENTICATION = 48;
    public final static int INVALID_CREDENTIALS = 49;
    public final static int INSUFFICIENT_ACCESS_RIGHTS = 50;
    public final static int BUSY = 51;
    public final static int UNAVAILABLE = 52;
    public final static int UNWILLING_TO_PERFORM = 53;
    public final static int LOOP_DETECT = 54;
    public final static int NAMING_VIOLATION = 64;
    public final static int OBJECT_CLASS_VIOLATION = 65;
    public final static int NOT_ALLOWED_ON_NONLEAF = 66;
    public final static int NOT_ALLOWED_ON_RDN = 67;
    public final static int ENTRY_ALREADY_EXISTS = 68;
    public final static int OBJECT_CLASS_MODS_PROHIBITED = 69;
    public final static int AFFECTS_MULTIPLE_DSAS = 71;
    public final static int OTHER = 80;
    public final static int SERVER_DOWN = 81;
    public final static int LOCAL_ERROR = 82;
    public final static int ENCODING_ERROR = 83;
    public final static int DECODING_ERROR = 84;
    public final static int LDAP_TIMEOUT = 85;
    public final static int AUTH_UNKNOWN = 86;
    public final static int FILTER_ERROR = 87;
    public final static int USER_CANCELLED = 88;
    public final static int NO_MEMORY = 90;
    public final static int CONNECT_ERROR = 91;
    public final static int LDAP_NOT_SUPPORTED = 92;
    public final static int CONTROL_NOT_FOUND = 93;
    public final static int NO_RESULTS_RETURNED = 94;
    public final static int MORE_RESULTS_TO_RETURN = 95;
    public final static int CLIENT_LOOP = 96;
    public final static int REFERRAL_LIMIT_EXCEEDED = 97;
    public final static int INVALID_RESPONSE = 100;
    public final static int AMBIGUOUS_RESPONSE = 101;
    public final static int TLS_NOT_SUPPORTED = 112;

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

    public static String getModificationOperation(int op) {

        switch (op) {
            case Modification.ADD:
                return "add";

            case Modification.DELETE:
                return "delete";

            case Modification.REPLACE:
                return "replace";
        }

        return null;
    }

    public static int getModificationOperation(String op) {

        if ("add".equals(op)) {
            return Modification.ADD;

        } else if ("delete".equals(op)) {
            return Modification.DELETE;

        } else if ("replace".equals(op)) {
            return Modification.REPLACE;
        }

        return 0;
    }

    public static Collection<Modification> createModifications(
            Attributes oldAttributes,
            Attributes newAttributes
    ) throws Exception {

        Collection<Modification> modifications = new ArrayList<Modification>();

        Collection<String> oldNames = oldAttributes.getNormalizedNames();
        Collection<String> newNames = newAttributes.getNormalizedNames();

        Collection<String> deletes = new ArrayList<String>();
        deletes.addAll(oldNames);
        deletes.removeAll(newNames);

        for (String name : deletes) {
            Attribute attribute = oldAttributes.get(name);
            modifications.add(new Modification(Modification.DELETE, attribute));
        }

        Collection<String> adds = new ArrayList<String>();
        adds.addAll(newNames);
        adds.removeAll(oldNames);

        for (String name : adds) {
            Attribute attribute = newAttributes.get(name);
            if (attribute.isEmpty()) continue;
            modifications.add(new Modification(Modification.ADD, attribute));
        }

        Collection<String> modifies = new ArrayList<String>();
        modifies.addAll(oldNames);
        modifies.retainAll(newNames);

        for (String name : modifies) {
            Attribute oldAttribute = oldAttributes.get(name);
            Attribute newAttribute = newAttributes.get(name);

            Collection<Modification> mods = createModifications(
                    oldAttribute,
                    newAttribute
            );

            if (mods.isEmpty()) continue;

            modifications.addAll(mods);
        }

        return modifications;
    }

    public static Collection<Modification> createModifications(
            Attribute oldAttribute,
            Attribute newAttribute
    ) throws Exception {

        Collection<Modification> modifications = new ArrayList<Modification>();

        boolean objectClass = oldAttribute.getName().equalsIgnoreCase("objectClass");

        Attribute attributeToDelete = (Attribute)oldAttribute.clone();
        attributeToDelete.removeValues(newAttribute.getValues());
        if (objectClass) attributeToDelete.removeValue("top");

        if (!attributeToDelete.isEmpty()) {
            modifications.add(new Modification(Modification.DELETE, attributeToDelete));
        }

        Attribute attributeToAdd = (Attribute)newAttribute.clone();
        attributeToAdd.removeValues(oldAttribute.getValues());
        if (objectClass) attributeToAdd.removeValue("top");

        if (!attributeToAdd.isEmpty()) {
            modifications.add(new Modification(Modification.ADD, attributeToAdd));
        }

        return modifications;
    }

    public static String escape(String value) throws Exception {

        StringBuilder sb = new StringBuilder();
        char chars[] = value.toCharArray();

        boolean quote = chars[0] == ' ' || chars[chars.length-1] == ' ';
        boolean space = false;

        for (char c : chars) {
            //if (c < 0x20 || c > 0x7E) {
            if (Character.isISOControl(c) || !Character.isDefined(c)) {
                sb.append('\\').append(hex(c));
                continue;
            }

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

    private static String hex(int b) {
        String hex = Integer.toHexString(b);
        if (hex.length() % 2 == 1) {
            hex = "0" + hex;
        }
        return hex;
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
