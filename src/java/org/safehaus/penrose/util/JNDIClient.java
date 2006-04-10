/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import java.io.StringReader;
import java.util.*;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import javax.naming.ldap.*;

import org.apache.log4j.Logger;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaParser;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.ietf.ldap.*;

public class JNDIClient {

	Logger log = Logger.getLogger(getClass());

    public String[] BINARY_ATTRIBUTES = new String[] {
        "photo", "personalSignature", "audio", "jpegPhoto", "javaSerializedData",
        "thumbnailPhoto", "thumbnailLogo", "userPassword", "userCertificate",
        "cACertificate", "authorityRevocationList", "certificateRevocationList",
        "crossCertificatePair", "x500UniqueIdentifier"
    };

    public Hashtable parameters;
    public Collection binaryAttributes;

    private String suffix;
    private String url;

    private LdapContext context;

    private LDAPEntry rootDSE;
    private Schema schema;

    public JNDIClient(JNDIClient client, Map parameters) throws Exception {
        init(parameters);

        this.rootDSE = client.rootDSE;
        this.schema = client.schema;
    }

    public JNDIClient(Map parameters) throws Exception {
        init(parameters);

        getRootDSE();
        getSchema();
    }

    public void init(Map parameters) throws Exception {

        this.parameters = new Hashtable();
        this.parameters.putAll(parameters);

        String providerUrl = (String)parameters.get(Context.PROVIDER_URL);

        int index = providerUrl.indexOf("://");
        index = providerUrl.indexOf("/", index+3);

        if (index >= 0) {
            suffix = providerUrl.substring(index+1);
            url = providerUrl.substring(0, index);
        } else {
            suffix = "";
            url = providerUrl;
        }

        this.parameters.put(Context.PROVIDER_URL, url);

        binaryAttributes = new HashSet();
        for (int i=0; i<BINARY_ATTRIBUTES.length; i++) {
            binaryAttributes.add(BINARY_ATTRIBUTES[i].toLowerCase());
        }

        String s = (String)parameters.get("java.naming.ldap.attributes.binary");
        //log.debug("java.naming.ldap.attributes.binary: "+s);

        if (s != null) {
            StringTokenizer st = new StringTokenizer(s);
            while (st.hasMoreTokens()) {
                String attribute = st.nextToken();
                binaryAttributes.add(attribute.toLowerCase());
            }
        }

        context = new InitialLdapContext(this.parameters, null);
    }

    public void close() throws Exception {
        context.close();
    }

    public DirContext bind(String dn, String password) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        Hashtable env = new Hashtable(parameters);
        env.put(Context.SECURITY_PRINCIPAL, dn);
        env.put(Context.SECURITY_CREDENTIALS, password);
        return new InitialLdapContext(env, null);
    }

    public int add(String dn, LDAPAttributeSet attributes) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        log.debug("Adding "+dn);

        Attributes attrs = new BasicAttributes();

        for (Iterator i=attributes.iterator(); i.hasNext(); ) {
            LDAPAttribute attribute = (LDAPAttribute)i.next();
            String name = attribute.getName();

            Attribute attr = new BasicAttribute(name);

            if ("unicodePwd".equalsIgnoreCase(name)) { // need to encode unicodePwd
                String[] values = attribute.getStringValueArray();
                for (int j=0; j<values.length; j++) {
                    attr.add(PasswordUtil.toUnicodePassword(values[j]));
                    log.debug(" - "+name+": (binary)");
                }

            } else if (isBinaryAttribute(name)) {
                byte[][] bytes = attribute.getByteValueArray();
                for (int j=0; j<bytes.length; j++) {
                    attr.add(bytes[j]);
                    log.debug(" - "+name+": (binary)");
                }

            } else {
                String[] values = attribute.getStringValueArray();
                for (int j=0; j<values.length; j++) {
                    String value = values[j];
                    attr.add(value);
                    log.debug(" - "+name+": "+value);
                }
            }

            attrs.put(attr);
        }

        try {
            context.createSubcontext(dn, attrs);

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            throw e;
        }

        return LDAPException.SUCCESS;
    }

    public int delete(String dn) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        log.debug("Deleting "+dn);

        try {
            context.destroySubcontext(dn);

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            throw e;
        }

        return LDAPException.SUCCESS;
    }

    public int modify(String dn, Collection modifications) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        log.debug("Modifying "+dn);

        Collection list = new ArrayList();

        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            LDAPModification modification = (LDAPModification)i.next();

            LDAPAttribute attribute = modification.getAttribute();
            String name = attribute.getName();

            Attribute attr = new BasicAttribute(name);

            switch (modification.getOp()) {
                case LDAPModification.ADD:
                    log.debug(" - add: "+name);
                    list.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attr));
                    break;

                case LDAPModification.REPLACE:
                    list.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attr));
                    break;

                case LDAPModification.DELETE:
                    list.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, attr));
                    break;
            }

            if ("unicodePwd".equalsIgnoreCase(name)) { // need to encode unicodePwd
                String[] values = attribute.getStringValueArray();
                for (int j=0; j<values.length; j++) {
                    attr.add(PasswordUtil.toUnicodePassword(values[j]));
                    log.debug(" - "+name+": (binary)");
                }

            } else if (isBinaryAttribute(name)) {
                byte[][] bytes = attribute.getByteValueArray();
                for (int j=0; j<bytes.length; j++) {
                    attr.add(bytes[j]);
                    log.debug(" - "+name+": (binary)");
                }

            } else {
                String[] values = attribute.getStringValueArray();
                for (int j=0; j<values.length; j++) {
                    String value = values[j];
                    attr.add(value);
                    log.debug(" - "+name+": "+value);
                }
            }
        }

        ModificationItem mods[] = (ModificationItem[])list.toArray(new ModificationItem[list.size()]);

        try {
            context.modifyAttributes(dn, mods);

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            throw e;
        }

        return LDAPException.SUCCESS;
    }

    public int modrdn(String dn, String newRdn) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        try {
            context.rename(dn, newRdn);

        } catch (Exception e) {
            log.debug(e.getMessage(), e);
            throw e;
        }

        return LDAPException.SUCCESS;
    }

    public boolean isBinaryAttribute(String name) throws Exception {

        if (binaryAttributes.contains(name.toLowerCase())) return true;

        getSchema();

        AttributeType at = schema.getAttributeType(name);
        if (at == null) return false;

        String syntax = at.getSyntax();
        //log.debug("Checking syntax for "+name+": "+syntax);

        if ("2.5.5.7".equals(syntax) // binary
                || "2.5.5.10".equals(syntax) // octet string
                //|| "2.5.5.12".equals(syntax) // unicode string
                || "2.5.5.17".equals(syntax) // sid
                || "1.3.6.1.4.1.1466.115.121.1.40".equals(syntax)
        ) {
            return true;
        }

        return false;
        //return binaryAttributes.contains(name.toLowerCase());
    }

    public LDAPEntry getRootDSE() throws Exception {

        if (rootDSE != null) return rootDSE;

        log.debug("Searching Root DSE ...");

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.OBJECT_SCOPE);
        ctls.setReturningAttributes(new String[] { "*", "+" });

        NamingEnumeration results = context.search("", "(objectClass=*)", ctls);
        SearchResult sr = (SearchResult)results.next();
        results.close();

        rootDSE = createLDAPEntry("", sr);

        return rootDSE;
    }

    public Collection getNamingContexts() throws Exception {
        getRootDSE();
        LDAPAttribute namingContexts = rootDSE.getAttribute("namingContexts");
        return Arrays.asList(namingContexts.getStringValueArray());
    }

    public Schema getSchema() throws Exception {

        if (schema != null) return schema;

        getRootDSE();

        log.debug("Searching Schema ...");

        try {
            LDAPAttribute schemaNamingContext = rootDSE.getAttribute("schemaNamingContext");
            LDAPAttribute subschemaSubentry = rootDSE.getAttribute("subschemaSubentry");

            String schemaDn;

            if (schemaNamingContext != null) {
                schemaDn = (String)schemaNamingContext.getStringValues().nextElement();
                log.debug("Active Directory Schema: "+schemaDn);
                schema = getActiveDirectorySchema(schemaDn);

            } else if (subschemaSubentry != null) {
                schemaDn = (String)subschemaSubentry.getStringValues().nextElement();
                log.debug("Standard LDAP Schema: "+schemaDn);
                schema = getLDAPSchema(schemaDn);

            } else {
                schemaDn = "cn=schema";
                log.debug("Default Schema: "+schemaDn);
                schema = getLDAPSchema(schemaDn);
            }
            
        } catch (Exception e) {
            log.debug(e.getMessage(), e);
        }

        return schema;
    }

    public Schema getActiveDirectorySchema(String schemaDn) throws Exception {

        log.debug("Searching "+schemaDn+" ...");

        Schema schema = new Schema();

        getActiveDirectoryAttributeTypes(schema, schemaDn);
        getActiveDirectoryObjectClasses(schema, schemaDn);

        return schema;
    }

    public void getActiveDirectoryAttributeTypes(Schema schema, String schemaDn) throws Exception {

        //log.debug("Attribute Types:");

        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
/*
        byte[] cookie = null;

        do {
            Control[] controls = new Control[]{
                new PagedResultsControl(100, cookie, Control.CRITICAL)
            };

            context.setRequestControls(controls);
*/
            NamingEnumeration results = context.search(schemaDn, "(objectClass=attributeSchema)", searchControls);

            while (results.hasMore()) {
                SearchResult sr = (SearchResult)results.next();
                Attributes attributes = sr.getAttributes();

                Attribute attribute = attributes.get("lDAPDisplayName");
                String atName = (String)attribute.get();
                //log.debug(" - "+atName);

                AttributeType at = new AttributeType();
                at.setName(atName);
                at.setOid((String)attributes.get("attributeID").get());
                at.setDescription((String)attributes.get("adminDescription").get());
                at.setSyntax((String)attributes.get("attributeSyntax").get());
                at.setSingleValued(Boolean.valueOf((String)attributes.get("isSingleValued").get()).booleanValue());

                schema.addAttributeType(at);
            }
/*
            controls = context.getResponseControls();

            if (controls != null) {
                for (int i = 0; i < controls.length; i++) {
                    if (controls[i] instanceof PagedResultsResponseControl) {
                        PagedResultsResponseControl prrc =
                            (PagedResultsResponseControl)controls[i];
                        cookie = prrc.getCookie();
                    }
                }
            }
*/
            results.close();
/*
        } while (cookie != null);

        context.setRequestControls(null);
*/
    }

    public void getActiveDirectoryObjectClasses(Schema schema, String schemaDn) throws Exception {

        //log.debug("Object Classes:");

        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.ONELEVEL_SCOPE);
/*
        byte[] cookie = null;

        do {
            Control[] controls = new Control[]{
                new PagedResultsControl(100, cookie, Control.CRITICAL)
            };

            context.setRequestControls(controls);
*/
            NamingEnumeration results = context.search(schemaDn, "(objectClass=classSchema)", searchControls);

            while (results.hasMore()) {
                SearchResult sr = (SearchResult)results.next();
                Attributes attributes = sr.getAttributes();

                Attribute attribute = attributes.get("lDAPDisplayName");
                String ocName = (String)attribute.get();
                //log.debug(" - "+ocName);

                ObjectClass oc = new ObjectClass();
                oc.setName(ocName);
                oc.setOid((String)attributes.get("governsID").get());
                oc.setDescription((String)attributes.get("adminDescription").get());

                attribute = attributes.get("mustContain");
                if (attribute != null) {
                    NamingEnumeration ne = attribute.getAll();
                    while (ne.hasMore()) {
                        String requiredAttribute = (String)ne.next();
                        oc.addRequiredAttribute(requiredAttribute);
                    }
                }

                attribute = attributes.get("systemMustContain");
                if (attribute != null) {
                    NamingEnumeration ne = attribute.getAll();
                    while (ne.hasMore()) {
                        String requiredAttribute = (String)ne.next();
                        oc.addRequiredAttribute(requiredAttribute);
                    }
                }

                attribute = attributes.get("mayContain");
                if (attribute != null) {
                    NamingEnumeration ne = attribute.getAll();
                    while (ne.hasMore()) {
                        String optionalAttribute = (String)ne.next();
                        oc.addOptionalAttribute(optionalAttribute);
                    }
                }

                attribute = attributes.get("systemMayContain");
                if (attribute != null) {
                    NamingEnumeration ne = attribute.getAll();
                    while (ne.hasMore()) {
                        String optionalAttribute = (String)ne.next();
                        oc.addOptionalAttribute(optionalAttribute);
                    }
                }

                schema.addObjectClass(oc);
            }
/*
            controls = context.getResponseControls();

            if (controls != null) {
                for (int i = 0; i < controls.length; i++) {
                    if (controls[i] instanceof PagedResultsResponseControl) {
                        PagedResultsResponseControl prrc =
                            (PagedResultsResponseControl)controls[i];
                        cookie = prrc.getCookie();
                    }
                }
            }
*/
            results.close();
/*
        } while (cookie != null);

        context.setRequestControls(null);
*/
    }

    public Schema getLDAPSchema(String schemaDn) throws Exception {

        Schema schema = new Schema();

        log.debug("Searching "+schemaDn+" ...");

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.OBJECT_SCOPE);
        ctls.setReturningAttributes(new String[] { "attributeTypes", "objectClasses" });

        NamingEnumeration results = context.search(schemaDn, "(objectClass=*)", ctls);
        SearchResult sr = (SearchResult)results.next();

        Attributes attributes = sr.getAttributes();

        //log.debug("Object Classes:");
        Attribute attribute = attributes.get("objectClasses");

        NamingEnumeration values = attribute.getAll();
        while (values.hasMore()) {
            String value = (String)values.next();
            //System.out.println("objectClass "+value);
            ObjectClass oc = parseObjectClass(value);
            if (oc == null) continue;

            //log.debug(" - "+oc.getName());
            schema.addObjectClass(oc);
        }

        //log.debug("Attribute Types:");
        attribute = attributes.get("attributeTypes");

        values = attribute.getAll();
        while (values.hasMore()) {
            String value = (String)values.next();
            //System.out.println("attributeTypes "+value);
            AttributeType at = parseAttributeType(value);
            if (at == null) continue;

            //log.debug(" - "+at.getName());
            schema.addAttributeType(at);
        }

        results.close();

        return schema;
    }

    public AttributeType parseAttributeType(String line) throws Exception {
        try {
            line = "attributetype "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Collection schema = parser.parse();
            //System.out.println("Parsed: "+schema);
            return (AttributeType)schema.iterator().next();

        } catch (Exception e) {
            System.out.println("Error parsing "+line);
            log.debug(e.getMessage(), e);
            return null;
        }
    }

    public ObjectClass parseObjectClass(String line) throws Exception {
        try {
            line = "objectclass "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Collection schema = parser.parse();
            //System.out.println("Parsed: "+schema);
            return (ObjectClass)schema.iterator().next();

        } catch (Exception e) {
            System.out.println("Error parsing "+line);
            log.debug(e.getMessage(), e);
            return null;
        }
    }

    public void search(
            String baseDn,
            int scope,
            String filter,
            Collection attributeNames,
            PenroseSearchResults results
            ) throws Exception {

        String ldapBase = EntryUtil.append(baseDn, suffix);

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(scope);

        DirContext ctx = null;
        try {
            NamingEnumeration ne = context.search(ldapBase, filter, ctls);

            log.debug("Search \""+ldapBase+"\" with "+filter+":");

            while (ne.hasMore()) {
                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();

                //String dn = "".equals(sr.getName()) ? baseDn : sr.getName()+","+baseDn;
                String dn = EntryUtil.append(sr.getName(), baseDn);
                //String dn = sr.getName();
                //log.debug(" - "+dn);

                LDAPEntry entry = createLDAPEntry(dn, sr);

                results.add(entry);
            }

        } finally {
            results.close();
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
    }
    
    public LDAPEntry getEntry(String dn) throws Exception {

        String searchBase = EntryUtil.append(dn, suffix);

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

        NamingEnumeration entries = context.search(searchBase, "(objectClass=*)", ctls);
        if (!entries.hasMore()) return null;

        SearchResult sr = (SearchResult)entries.next();

        return createLDAPEntry(dn, sr);
    }

    /**
     * @return Collection of LDAPEntry
     */
    public Collection getChildren(String baseDn) throws Exception {

        Collection results = new ArrayList();

        String searchBase = EntryUtil.append(baseDn, suffix);

        if ("".equals(searchBase)) {
            log.debug("Searching Root DSE:");

            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

            NamingEnumeration entries = context.search(searchBase, "(objectClass=*)", ctls);
            SearchResult rootDse = (SearchResult)entries.next();

            Attributes attributes = rootDse.getAttributes();
            Attribute attribute = attributes.get("namingContexts");

            NamingEnumeration values = attribute.getAll();

            while (values.hasMore()) {
                String dn = (String)values.next();
                log.debug(" - "+dn);

                LDAPEntry entry = getEntry(dn);
                results.add(entry);
            }

        } else {
            log.debug("Searching "+searchBase+":");

            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

            NamingEnumeration entries = context.search(searchBase, "(objectClass=*)", ctls);
            try {
                while (entries.hasMore()) {
                    SearchResult sr = (SearchResult)entries.next();
                    String rdn = sr.getName();
                    String dn = EntryUtil.append(rdn, baseDn);
                    log.debug(" - "+dn);
                    LDAPEntry entry = createLDAPEntry(dn, sr);
                    results.add(entry);
                }
            } catch (Exception e) {
                log.debug(e.getMessage(), e);
            }
        }

        return results;
    }

    public DirContext getContext() {
        return context;
    }

    public void setContext(LdapContext context) {
        this.context = context;
    }

    public static String[] parseURL(String s) {
        System.out.println("Parsing "+s);

        String[] result = new String[4]; // 0 = ldap/ldaps, 1 = hostname, 2 = port, 3 = suffix

        int i = s.indexOf("://");
        if (i < 0) return null;

        result[0] = s.substring(0, i);

        int j = s.indexOf("/", i+3);
        String hostPort;
        if (j > 0) {
            hostPort = s.substring(i+3, j);
            result[3] = s.substring(j+1);
        } else {
            hostPort = s.substring(i+3);
        }

        int k = hostPort.indexOf(":");
        if (k < 0) {
            result[1] = hostPort;
            if ("ldap".equals(result[0])) {
                result[2] = "389";
            } else if ("ldaps".equals(result[0])) {
                result[2] = "636";
            } else {
                return null;
            }
        } else {
            result[1] = hostPort.substring(0, k);
            result[2] = hostPort.substring(k+1);
        }

        return result;
    }

    public String getSuffix() {
        return suffix;
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public LDAPEntry createLDAPEntry(String dn, SearchResult sr) throws Exception {
        return createLDAPEntry(dn, sr.getAttributes());
    }

    public LDAPEntry createLDAPEntry(String dn, Attributes attributes) throws Exception {

        LDAPAttributeSet attributeSet = new LDAPAttributeSet();

        //log.debug("Creating "+EntryUtil.append(dn, suffix)+":");
        for (Enumeration en = attributes.getAll(); en.hasMoreElements(); ) {
            Attribute attribute = (Attribute)en.nextElement();
            String name = attribute.getID();

            LDAPAttribute attr = new LDAPAttribute(name);

            Collection values = getAttributeValues(attribute);
            for (Iterator i = values.iterator(); i.hasNext(); ) {
                Object value = i.next();
                if (value instanceof byte[]) {
                    attr.addValue((byte[])value);
                    //log.debug(" - "+name+": (binary)");
                } else {
                    attr.addValue((String)value);
                    //log.debug(" - "+name+": "+value);
                }
            }

            attributeSet.add(attr);
        }

        return new LDAPEntry(dn, attributeSet);
    }

    public Collection getAttributeValues(Attribute attribute) throws Exception {

        Collection values = new ArrayList();

        for (NamingEnumeration ne = attribute.getAll(); ne.hasMore(); ) {
            Object value = ne.next();
            values.add(value);
        }

        return values;
    }

    public void setRootDSE(LDAPEntry rootDSE) {
        this.rootDSE = rootDSE;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
