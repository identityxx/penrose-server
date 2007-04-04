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
import javax.naming.ldap.Control;

import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaParser;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.DNBuilder;
import org.safehaus.penrose.entry.RDN;

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

    private javax.naming.directory.SearchResult rootDSE;
    private Schema schema;

    private int pageSize = 1000;

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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN targetDn = request.getDn();
        Attributes attributes = convertAttributes(request.getAttributes());

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        db.append(suffix);
        DN dn = db.toDn();

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
            context.createSubcontext(dn.toString(), attrs);

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN bindDn = request.getDn();
        String password = request.getPassword();

        DNBuilder db = new DNBuilder();
        db.set(bindDn);
        db.append(suffix);

        parameters.put(Context.SECURITY_PRINCIPAL, db.toString());
        parameters.put(Context.SECURITY_CREDENTIALS, password);

        LdapContext context = null;

        try {
            context = getContext();

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN targetDn = request.getDn();

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        db.append(suffix);
        DN dn = db.toDn();

        log.debug("Deleting "+dn);

        LdapContext context = null;

        try {
            context = getContext();
            context.destroySubcontext(dn.toString());

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN targetDn = request.getDn();
        Collection<Modification> modifications = request.getModifications();

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        db.append(suffix);
        DN dn = db.toDn();

        log.debug("Modifying "+dn);

        Collection list = new ArrayList();

        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            Modification modification = (Modification)i.next();

            int type = modification.getType();
            org.safehaus.penrose.entry.Attribute attribute = modification.getAttribute();
            String name = attribute.getName();

            javax.naming.directory.Attribute attr = new javax.naming.directory.BasicAttribute(name);
            list.add(new ModificationItem(type, attr));

            if ("unicodePwd".equalsIgnoreCase(name)) { // need to encode unicodePwd
                for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                    Object value = j.next();
                    attr.add(PasswordUtil.toUnicodePassword(value));
                    log.debug(" - "+name+": (binary)");
                }

            } else {
                for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
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
            context.modifyAttributes(dn.toString(), mods);

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN targetDn = request.getDn();
        RDN newRdn = request.getNewRdn();

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        db.append(suffix);
        DN dn = db.toDn();

        LdapContext context = null;

        try {
            context = getContext();
            context.rename(dn.toString(), newRdn.toString());

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DNBuilder db = new DNBuilder();
        db.set(request.getDn());
        db.append(suffix);
        DN baseDn = db.toDn();

        String filter = request.getFilter() == null ? "(objectClass=*)" : request.getFilter().toString();

        log.debug("Search \""+ baseDn +"\" with filter="+filter+" scope="+ request.getScope()+" attrs="+ request.getAttributes()+":");

        String attributes[] = (String[]) request.getAttributes().toArray(new String[request.getAttributes().size()]);

        SearchControls sc = new SearchControls();
        sc.setSearchScope(request.getScope());
        sc.setReturningAttributes(request.getAttributes().isEmpty() ? null : attributes);
        sc.setCountLimit(request.getSizeLimit());
        sc.setTimeLimit((int) request.getTimeLimit());

        LdapContext context = null;
        NamingEnumeration ne = null;

        try {
/*
            LDAPSearchResults searchResults = connection.search(baseDn, request.getScope(), filter, attributes, request.isTypesOnly());
            while (searchResults.hasMore()) {
                try {
                    LDAPEntry entry = searchResults.next();
                    log.debug("Received entry "+entry.getDN());
                    SearchResult sr = EntryUtil.toSearchResult(entry);
                    response.add(sr);

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
                        response.add(sr);
                        //response.addReferral(referral);
                    }
                }
            }
*/
            context = getContext();

            Collection<Control> origControls = convertControls(request.getControls());

            Collection<Control> list = new ArrayList<Control>();
            list.addAll(origControls);
            list.add(new PagedResultsControl(pageSize, Control.NONCRITICAL));

            Control[] controls = (Control[])list.toArray(new Control[list.size()]);
            context.setRequestControls(controls);

            int page = 0;
            byte[] cookie = null;

            do {
                boolean moreReferrals = true;

                while (moreReferrals) {
                    try {
                        if (debug) log.debug("Searching page #"+page);
                        ne = context.search(baseDn.toString(), filter, sc);

                        while (ne.hasMore()) {
                            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                            String s = sr.getName();
                            log.debug("SearchResult: ["+s+"]");

                            if (s.startsWith("ldap://")) {
                                LDAPUrl url = new LDAPUrl(s);
                                db.set(LDAPUrl.decode(url.getDN()));
                            } else {
                                db.set(s);
                                db.append(request.getDn());
                            }

                            sr.setName(db.toString());

                            response.add(sr);
                        }

                        moreReferrals = false;

                    } catch (ReferralException e) {
                        String referral = e.getReferralInfo().toString();
                        log.debug("Referral: "+referral);

                        LDAPUrl url = new LDAPUrl(referral);

                        Attributes attrs = new BasicAttributes();
                        attrs.put("ref", referral);
                        attrs.put("objectClass", "referral");

                        javax.naming.directory.SearchResult sr = new javax.naming.directory.SearchResult(url.getDN(), null, attrs);
                        response.add(sr);
                        //response.addReferral(referral);

                        moreReferrals = e.skipReferral();

                        if (moreReferrals) {
                            context = (LdapContext)e.getReferralContext();
                        }
                    }
                }

                // get cookie returned by server
                controls = context.getResponseControls();
                if (controls != null) {
                    for (int i = 0; i < controls.length; i++) {
                        if (controls[i] instanceof PagedResultsResponseControl) {
                            PagedResultsResponseControl prrc = (PagedResultsResponseControl)controls[i];
                            cookie = prrc.getCookie();
                        }
                    }
                }

                // pass cookie back to server for the next page
                list = new ArrayList<Control>();
                list.addAll(origControls);
                list.add(new PagedResultsControl(pageSize, cookie, Control.CRITICAL));

                controls = (Control[])list.toArray(new Control[list.size()]);
                context.setRequestControls(controls);

                page++;

            } while (cookie != null && cookie.length != 0);

        } finally {
            if (ne != null) try { ne.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            //if (connection != null) try { connection.disconnect(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            response.close();
        }
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

    public javax.naming.directory.SearchResult getRootDSE() throws Exception {

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

            rootDSE = createSearchResult(entry);

        } finally {
            if (connection != null) try { connection.disconnect(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }

        return rootDSE;
    }

    public javax.naming.directory.SearchResult createSearchResult(LDAPEntry entry) {

        //log.debug("Converting attributes for "+entry.getDN());

        LDAPAttributeSet attributeSet = entry.getAttributeSet();
        javax.naming.directory.Attributes attributes = new javax.naming.directory.BasicAttributes();

        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute ldapAttribute = (LDAPAttribute)i.next();
            //log.debug(" - "+ldapAttribute.getName()+": "+Arrays.asList(ldapAttribute.getSubtypes()));
            javax.naming.directory.Attribute attribute = new javax.naming.directory.BasicAttribute(ldapAttribute.getName());

            for (Enumeration j=ldapAttribute.getStringValues(); j.hasMoreElements(); ) {
                String value = (String)j.nextElement();
                //log.debug("   - "+value);
                attribute.add(value);
            }

            attributes.put(attribute);
        }

        return new javax.naming.directory.SearchResult(entry.getDN(), entry, attributes);
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
                    javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)results.next();
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
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
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
                    javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)results.next();
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
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
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
            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)results.next();

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
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
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

    public Collection<Control> convertControls(Collection<org.safehaus.penrose.control.Control> controls) throws Exception {
        Collection<Control> list = new ArrayList<Control>();
        for (Iterator i=controls.iterator(); i.hasNext(); ) {
            org.safehaus.penrose.control.Control control = (org.safehaus.penrose.control.Control)i.next();

            String oid = control.getOid();
            boolean critical = control.isCritical();
            byte[] value = control.getValue();

            list.add(new BasicControl(oid, critical, value));
        }

        return list;
    }

    public javax.naming.directory.SearchResult getEntry(String dn) throws Exception {

        DNBuilder db = new DNBuilder();
        db.set(dn);
        db.append(suffix);
        DN searchBase = db.toDn();

        SearchControls ctls = new SearchControls();
        ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

        LdapContext context = null;

        try {
            if ("".equals(searchBase)) {
                return getRootDSE();
                
            } else {
                context = getContext();
                NamingEnumeration entries = context.search(searchBase.toString(), "(objectClass=*)", ctls);
                if (!entries.hasMore()) return null;

                javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)entries.next();
                sr.setName(dn);

                return sr;
            }

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    /**
     * @return Collection of LDAPEntry
     */
    public Collection getChildren(String baseDn) throws Exception {

        Collection results = new ArrayList();

        LdapContext context = null;

        try {
            DNBuilder db = new DNBuilder();
            db.set(baseDn);
            db.append(suffix);
            DN searchBase = db.toDn();

            if ("".equals(searchBase)) {
                javax.naming.directory.SearchResult rootDse = getRootDSE();
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

                    javax.naming.directory.SearchResult entry = getEntry(dn);
                    results.add(entry);
                }

            } else {
                log.debug("Searching "+searchBase+":");

                SearchControls ctls = new SearchControls();
                ctls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

                context = getContext();
                NamingEnumeration entries = context.search(searchBase.toString(), "(objectClass=*)", ctls);
                try {
                    while (entries.hasMore()) {
                        javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)entries.next();
                        db.set(sr.getName());
                        db.append(baseDn);
                        DN dn = db.toDn();

                        log.debug(" - "+dn);
                        sr.setName(dn.toString());
                        results.add(sr);
                    }
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        } finally {
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
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

    public void setRootDSE(javax.naming.directory.SearchResult rootDSE) {
        this.rootDSE = rootDSE;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Attributes convertAttributes(org.safehaus.penrose.entry.Attributes attributes) throws Exception {

        Attributes attrs = new javax.naming.directory.BasicAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            org.safehaus.penrose.entry.Attribute attribute = (org.safehaus.penrose.entry.Attribute)i.next();

            Attribute attr = new BasicAttribute(attribute.getName());
            for (Iterator j=attribute.getValues().iterator(); j.hasNext(); ) {
                Object value = j.next();
                attr.add(value);
            }
        }

        return attrs;
    }
}
