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

import java.io.StringReader;
import java.util.*;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.ReferralException;
import javax.naming.directory.*;
import javax.naming.ldap.*;

import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaParser;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.util.PasswordUtil;
import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class LDAPClient {

    Logger log = LoggerFactory.getLogger(getClass());

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

    LDAPConnection connection = null;

    private SearchResult rootDSE;
    private Schema schema;

    public LDAPClient(LDAPClient client, Map parameters) throws Exception {
        init(parameters);

        this.rootDSE = client.rootDSE;
        this.schema = client.schema;
    }

    public LDAPClient(Map parameters) throws Exception {
        init(parameters);

        //getRootDSE();
        //getSchema();
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
/*
        LDAPUrl url = new LDAPUrl(providerUrl);

        String server = url.getHost();
        int port = url.getPort();

        log.debug("Connecting to "+server+":"+port);
        connection = new LDAPConnection();
        connection.connect(server, port);

        String bindDn = (String)parameters.get(Context.SECURITY_PRINCIPAL);
        String bindPassword = (String)parameters.get(Context.SECURITY_CREDENTIALS);

        if (bindDn != null && !"".equals(bindDn)) {
            connection.bind(3, bindDn, bindPassword.getBytes());
        }
*/
    }

    public LdapContext getContext() throws Exception {
        log.debug("Creating InitialLdapContext:");
        for (Iterator i=parameters.keySet().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = parameters.get(name);
            log.debug(" - "+name+": "+value);
        }
        return new InitialLdapContext(parameters, null);
    }

    public void close() throws Exception {
        if (connection != null) connection.disconnect();
    }

    public int bind(String dn, String password) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        parameters.put(Context.SECURITY_PRINCIPAL, dn);
        parameters.put(Context.SECURITY_CREDENTIALS, password);

        LdapContext context = null;

        try {
            context = getContext();

        } catch (Exception e) {
            log.debug(e.getMessage());
            return LDAPException.INVALID_CREDENTIALS;

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int add(String dn, Attributes attributes) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        log.debug("Adding "+dn);

        Attributes attrs = new BasicAttributes();

        for (NamingEnumeration i=attributes.getAll(); i.hasMore(); ) {
            Attribute attribute = (Attribute)i.next();
            String name = attribute.getID();

            Attribute attr = new BasicAttribute(name);

            if ("unicodePwd".equalsIgnoreCase(name)) { // need to encode unicodePwd
                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    attr.add(PasswordUtil.toUnicodePassword(value));
                    log.debug(" - "+name+": (binary)");
                }

            } else {
                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    attr.add(value);
                    log.debug(" - "+name+": "+value);
                }
            }

            attrs.put(attr);
        }

        LdapContext context = null;

        try {
            context = getContext();
            context.createSubcontext(dn, attrs);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int delete(String dn) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        log.debug("Deleting "+dn);

        LdapContext context = null;

        try {
            context = getContext();
            context.destroySubcontext(dn);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int modify(String dn, Collection modifications) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        log.debug("Modifying "+dn);

        Collection list = new ArrayList();

        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            ModificationItem modification = (ModificationItem)i.next();

            Attribute attribute = modification.getAttribute();
            String name = attribute.getID();

            Attribute attr = new BasicAttribute(name);

            switch (modification.getModificationOp()) {
                case DirContext.ADD_ATTRIBUTE:
                    list.add(new ModificationItem(DirContext.ADD_ATTRIBUTE, attr));
                    break;

                case DirContext.REPLACE_ATTRIBUTE:
                    list.add(new ModificationItem(DirContext.REPLACE_ATTRIBUTE, attr));
                    break;

                case DirContext.REMOVE_ATTRIBUTE:
                    list.add(new ModificationItem(DirContext.REMOVE_ATTRIBUTE, attr));
                    break;
            }

            if ("unicodePwd".equalsIgnoreCase(name)) { // need to encode unicodePwd
                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    attr.add(PasswordUtil.toUnicodePassword(value));
                    log.debug(" - "+name+": (binary)");
                }

            } else {
                for (NamingEnumeration j=attribute.getAll(); j.hasMore(); ) {
                    Object value = j.next();
                    attr.add(value);
                    log.debug(" - "+name+": "+value);
                }
            }
        }

        ModificationItem mods[] = (ModificationItem[])list.toArray(new ModificationItem[list.size()]);

        LdapContext context = null;

        try {
            context = getContext();
            context.modifyAttributes(dn, mods);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }

        return LDAPException.SUCCESS;
    }

    public int modrdn(String dn, String newRdn) throws Exception {

        dn = EntryUtil.append(dn, suffix);

        LdapContext context = null;

        try {
            context = getContext();
            context.rename(dn, newRdn);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
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

    public SearchResult getRootDSE() throws Exception {

        if (rootDSE != null) return rootDSE;

        log.debug("Searching Root DSE ...");

        LDAPConnection connection = null;

        try {
            LDAPUrl ldapUrl = new LDAPUrl(url);

            connection = new LDAPConnection();
            connection.connect(ldapUrl.getHost(), ldapUrl.getPort());

            String bindDn = (String)parameters.get(Context.SECURITY_PRINCIPAL);
            String password = (String)parameters.get(Context.SECURITY_CREDENTIALS);

            if (bindDn != null && !"".equals(bindDn) && password != null && !"".equals(password)) {
                connection.bind(3, bindDn, password.getBytes());
            }

            LDAPSearchResults sr = connection.search("", LDAPConnection.SCOPE_BASE, "(objectClass=*)", new String[] { "*", "+" }, false);
            LDAPEntry entry = sr.next();

            rootDSE = EntryUtil.toSearchResult(entry);

        } finally {
            if (connection != null) try { connection.disconnect(); } catch (Exception e) {}
        }

        return rootDSE;
    }

    public Collection getNamingContexts() throws Exception {
        getRootDSE();
        Attribute namingContexts = rootDSE.getAttributes().get("namingContexts");

        Collection list = new ArrayList();
        for (NamingEnumeration i=namingContexts.getAll(); i.hasMore(); ) {
            String namingContext = (String)i.next();
            list.add(namingContext);
        }

        return list;
    }

    public Schema getSchema() throws Exception {

        if (schema != null) return schema;

        getRootDSE();

        log.debug("Searching Schema ...");

        try {
            Attribute schemaNamingContext = rootDSE.getAttributes().get("schemaNamingContext");
            Attribute subschemaSubentry = rootDSE.getAttributes().get("subschemaSubentry");

            String schemaDn;

            if (schemaNamingContext != null) {
                schemaDn = (String)schemaNamingContext.get();
                log.debug("Active Directory Schema: "+schemaDn);
                schema = getActiveDirectorySchema(schemaDn);

            } else if (subschemaSubentry != null) {
                schemaDn = (String)subschemaSubentry.get();
                log.debug("Standard LDAP Schema: "+schemaDn);
                schema = getLDAPSchema(schemaDn);

            } else {
                schemaDn = "cn=schema";
                log.debug("Default Schema: "+schemaDn);
                schema = getLDAPSchema(schemaDn);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
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

        LdapContext context = null;

        try {
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
                context = getContext();
                NamingEnumeration results = context.search(schemaDn, "(objectClass=attributeSchema)", searchControls);

                while (results.hasMore()) {
                    SearchResult sr = (SearchResult)results.next();
                    Attributes attributes = sr.getAttributes();

                    Attribute lDAPDisplayName = attributes.get("lDAPDisplayName");
                    String atName = (String)lDAPDisplayName.get();
                    //log.debug(" - "+atName);

                    AttributeType at = new AttributeType();
                    at.setName(atName);

                    Attribute attributeID = attributes.get("attributeID");
                    if (attributeID != null) at.setOid(attributeID.get().toString());

                    Attribute adminDescription = attributes.get("adminDescription");
                    if (adminDescription != null) at.setDescription(adminDescription.get().toString());

                    Attribute attributeSyntax = attributes.get("attributeSyntax");
                    if (attributeSyntax != null) at.setSyntax(attributeSyntax.get().toString());

                    Attribute isSingleValued = attributes.get("isSingleValued");
                    if (isSingleValued != null) at.setSingleValued(Boolean.valueOf(isSingleValued.get().toString()).booleanValue());

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
        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }
    }

    public void getActiveDirectoryObjectClasses(Schema schema, String schemaDn) throws Exception {

        //log.debug("Object Classes:");

        LdapContext context = null;

        try {
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
                context = getContext();
                NamingEnumeration results = context.search(schemaDn, "(objectClass=classSchema)", searchControls);

                while (results.hasMore()) {
                    SearchResult sr = (SearchResult)results.next();
                    Attributes attributes = sr.getAttributes();

                    Attribute lDAPDisplayName = attributes.get("lDAPDisplayName");
                    String ocName = (String)lDAPDisplayName.get();
                    //log.debug(" - "+ocName);

                    ObjectClass oc = new ObjectClass();
                    oc.setName(ocName);

                    Attribute governsID = attributes.get("governsID");
                    if (governsID != null) oc.setOid(governsID.get().toString());

                    Attribute adminDescription = attributes.get("adminDescription");
                    if (adminDescription != null) oc.setDescription(adminDescription.get().toString());

                    Attribute mustContain = attributes.get("mustContain");
                    if (mustContain != null) {
                        NamingEnumeration ne = mustContain.getAll();
                        while (ne.hasMore()) {
                            String requiredAttribute = (String)ne.next();
                            oc.addRequiredAttribute(requiredAttribute);
                        }
                    }

                    Attribute systemMustContain = attributes.get("systemMustContain");
                    if (systemMustContain != null) {
                        NamingEnumeration ne = systemMustContain.getAll();
                        while (ne.hasMore()) {
                            String requiredAttribute = (String)ne.next();
                            oc.addRequiredAttribute(requiredAttribute);
                        }
                    }

                    Attribute mayContain = attributes.get("mayContain");
                    if (mayContain != null) {
                        NamingEnumeration ne = mayContain.getAll();
                        while (ne.hasMore()) {
                            String optionalAttribute = (String)ne.next();
                            oc.addOptionalAttribute(optionalAttribute);
                        }
                    }

                    Attribute systemMayContain = attributes.get("systemMayContain");
                    if (systemMayContain != null) {
                        NamingEnumeration ne = systemMayContain.getAll();
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

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }
    }

    public Schema getLDAPSchema(String schemaDn) throws Exception {

        Schema schema = new Schema();

        log.debug("Searching "+schemaDn+" ...");

        LdapContext context = null;

        try {
            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.OBJECT_SCOPE);
            ctls.setReturningAttributes(new String[] { "attributeTypes", "objectClasses" });

            context = getContext();
            NamingEnumeration results = context.search(schemaDn, "(objectClass=*)", ctls);
            SearchResult sr = (SearchResult)results.next();

            Attributes attributes = sr.getAttributes();

            //log.debug("Object Classes:");
            Attribute objectClasses = attributes.get("objectClasses");

            NamingEnumeration values = objectClasses.getAll();
            while (values.hasMore()) {
                String value = (String)values.next();
                //System.out.println("objectClass "+value);
                ObjectClass oc = parseObjectClass(value);
                if (oc == null) continue;

                //log.debug(" - "+oc.getName());
                schema.addObjectClass(oc);
            }

            //log.debug("Attribute Types:");
            Attribute attributeTypes = attributes.get("attributeTypes");

            values = attributeTypes.getAll();
            while (values.hasMore()) {
                String value = (String)values.next();
                //System.out.println("attributeTypes "+value);
                AttributeType at = parseAttributeType(value);
                if (at == null) continue;

                //log.debug(" - "+at.getName());
                schema.addAttributeType(at);
            }

            results.close();

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }

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
            log.error(e.getMessage(), e);
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
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public void search(
            String baseDn,
            String filter,
            PenroseSearchControls searchControls,
            PenroseSearchResults results
            ) throws Exception {

        String ldapBase = EntryUtil.append(baseDn, suffix);
        log.debug("Search \""+ldapBase+"\" with filter="+filter+" scope="+searchControls.getScope()+" attrs="+searchControls.getAttributes()+":");

        String attributes[] = (String[])searchControls.getAttributes().toArray(new String[searchControls.getAttributes().size()]);

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(searchControls.getScope());
        ctls.setReturningAttributes(searchControls.getAttributes().isEmpty() ? null : attributes);

        LdapContext context = null;
        NamingEnumeration ne = null;

        try {
/*
            LDAPSearchResults searchResults = connection.search(ldapBase, searchControls.getScope(), filter, attributes, searchControls.isTypesOnly());
            while (searchResults.hasMore()) {
                try {
                    LDAPEntry entry = searchResults.next();
                    log.debug("Received entry "+entry.getDN());
                    SearchResult sr = EntryUtil.toSearchResult(entry);
                    results.add(sr);

                } catch (LDAPReferralException e) {
                    log.debug("Referrals:");
                    String referrals[] = e.getReferrals();
                    for (int i=0; i<referrals.length; i++) {
                        String referral = referrals[i];
                        log.debug(" - "+referral);

                        LDAPUrl url = new LDAPUrl(referral);

                        Attributes attrs = new BasicAttributes();
                        attrs.put("ref", referral);
                        attrs.put("objectClass", "referral");

                        SearchResult sr = new SearchResult(url.getDN(), null, attrs);
                        results.add(sr);
                        //results.addReferral(referral);
                    }
                }
            }
*/
            context = getContext();

            boolean moreReferrals = true;

            while (moreReferrals) {
                try {
                    ne = context.search(ldapBase, filter, ctls);

                    while (ne.hasMore()) {
                        SearchResult sr = (SearchResult)ne.next();
                        String dn = sr.getName();
                        log.debug("SearchResult: ["+dn+"]");

                        if (dn.startsWith("ldap://")) {
                            LDAPUrl url = new LDAPUrl(dn);
                            dn = LDAPUrl.decode(url.getDN());
                        } else {
                            dn = EntryUtil.append(dn, baseDn);
                        }
                        
                        sr.setName(dn);

                        results.add(sr);
                    }

                    moreReferrals = false;

                } catch (ReferralException e) {
                    String referral = e.getReferralInfo().toString();
                    log.debug("Referral: "+referral);

                    LDAPUrl url = new LDAPUrl(referral);

                    Attributes attrs = new BasicAttributes();
                    attrs.put("ref", referral);
                    attrs.put("objectClass", "referral");

                    SearchResult sr = new SearchResult(url.getDN(), null, attrs);
                    results.add(sr);
                    //results.addReferral(referral);

                    moreReferrals = e.skipReferral();

                    if (moreReferrals) {
                        context = (LdapContext)e.getReferralContext();
                    }
                }
            }

        } finally {
            if (ne != null) try { ne.close(); } catch (Exception e) {}
            if (context != null) try { context.close(); } catch (Exception e) {}
            //if (connection != null) try { connection.disconnect(); } catch (Exception e) {}
            results.close();
        }
    }

    public SearchResult getEntry(String dn) throws Exception {

        String searchBase = EntryUtil.append(dn, suffix);

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

        LdapContext context = null;

        try {
            if ("".equals(searchBase)) {
                return getRootDSE();
                
            } else {
                context = getContext();
                NamingEnumeration entries = context.search(searchBase, "(objectClass=*)", ctls);
                if (!entries.hasMore()) return null;

                SearchResult sr = (SearchResult)entries.next();
                sr.setName(dn);

                return sr;
            }

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }
    }

    /**
     * @return Collection of LDAPEntry
     */
    public Collection getChildren(String baseDn) throws Exception {

        Collection results = new ArrayList();

        LdapContext context = null;

        try {
            String searchBase = EntryUtil.append(baseDn, suffix);

            if ("".equals(searchBase)) {
                SearchResult rootDse = getRootDSE();
/*
                log.debug("Searching Root DSE:");

                SearchControls ctls = new SearchControls();
                ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

                context = getContext();
                NamingEnumeration entries = context.search(searchBase, "(objectClass=*)", ctls);
                SearchResult rootDse = (SearchResult)entries.next();
*/
                Attributes attributes = rootDse.getAttributes();
                Attribute attribute = attributes.get("namingContexts");

                NamingEnumeration values = attribute.getAll();

                while (values.hasMore()) {
                    String dn = (String)values.next();
                    log.debug(" - "+dn);

                    SearchResult entry = getEntry(dn);
                    results.add(entry);
                }

            } else {
                log.debug("Searching "+searchBase+":");

                SearchControls ctls = new SearchControls();
                ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

                context = getContext();
                NamingEnumeration entries = context.search(searchBase, "(objectClass=*)", ctls);
                try {
                    while (entries.hasMore()) {
                        SearchResult sr = (SearchResult)entries.next();
                        String rdn = sr.getName();
                        String dn = EntryUtil.append(rdn, baseDn);
                        log.debug(" - "+dn);
                        sr.setName(dn);
                        results.add(sr);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) {}
        }

        return results;
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

    public Collection getAttributeValues(Attribute attribute) throws Exception {

        Collection values = new ArrayList();

        for (NamingEnumeration ne = attribute.getAll(); ne.hasMore(); ) {
            Object value = ne.next();
            values.add(value);
        }

        return values;
    }

    public void setRootDSE(SearchResult rootDSE) {
        this.rootDSE = rootDSE;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }
}
