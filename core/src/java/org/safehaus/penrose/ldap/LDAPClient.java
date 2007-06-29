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
import javax.naming.PartialResultException;
import javax.naming.directory.*;
import javax.naming.ldap.Control;
import javax.naming.ldap.BasicControl;
import javax.naming.ldap.PagedResultsResponseControl;

import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaParser;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.util.BinaryUtil;

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

    public Hashtable<String,Object> parameters = new Hashtable<String,Object>();
    public Collection<String> binaryAttributes;

    private DN suffix;
    private String url;

    LDAPConnection connection = null;

    private javax.naming.directory.SearchResult rootDSE;
    private Schema schema;

    private int pageSize = 100;

    public LDAPClient(LDAPClient client, Map<String,String> parameters) throws Exception {
        init(parameters);

        this.rootDSE = client.rootDSE;
        this.schema = client.schema;
    }

    public LDAPClient(Map<String,String> parameters) throws Exception {
        init(parameters);

        //getRootDSE();
        //getSchema();
    }

    public void init(Map<String,String> parameters) throws Exception {

        this.parameters.putAll(parameters);

        String providerUrl = parameters.get(Context.PROVIDER_URL);

        int index = providerUrl.indexOf("://");
        index = providerUrl.indexOf("/", index+3);

        if (index >= 0) {
            suffix = new DN(providerUrl.substring(index+1));
            url = providerUrl.substring(0, index);
        } else {
            suffix = new DN();
            url = providerUrl;
        }

        this.parameters.put(Context.PROVIDER_URL, url);

        binaryAttributes = new HashSet<String>();
        for (String name : BINARY_ATTRIBUTES) {
            binaryAttributes.add(name.toLowerCase());
        }

        String s = parameters.get("java.naming.ldap.attributes.binary");
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
            connection.bind(3, bindDn, bindPassword.getPassword());
        }
*/
    }

    public javax.naming.ldap.LdapContext open() throws Exception {
        return open(parameters);
    }

    public javax.naming.ldap.LdapContext open(Hashtable parameters) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug("Creating InitialLdapContext:");

            for (Object name : parameters.keySet()) {
                Object value = parameters.get(name);

                if (Context.SECURITY_CREDENTIALS.equals(name) && value instanceof byte[]) {
                    log.debug(" - " + name + ": " + new String((byte[])value));
                } else {
                    log.debug(" - " + name + ": " + value);
                }
            }
        }

        return new javax.naming.ldap.InitialLdapContext(parameters, null);
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
        Attributes attributes = request.getAttributes();

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        db.append(suffix);
        DN dn = db.toDn();

        log.debug("Adding "+dn);

        javax.naming.directory.Attributes attrs = convertAttributes(attributes);

        javax.naming.ldap.LdapContext context = null;

        try {
            context = open();
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
        byte[] password = request.getPassword();

        DNBuilder db = new DNBuilder();
        db.set(bindDn);
        db.append(suffix);

        parameters.put(Context.SECURITY_PRINCIPAL, db.toString());
        parameters.put(Context.SECURITY_CREDENTIALS, password);

        javax.naming.ldap.LdapContext context = null;

        try {
            context = open();

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

        javax.naming.ldap.LdapContext context = null;

        try {
            context = open();
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

        boolean debug = log.isDebugEnabled();

        DN targetDn = request.getDn();
        Collection<Modification> modifications = request.getModifications();

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        db.append(suffix);
        DN dn = db.toDn();

        if (debug) log.debug("Modifying "+dn);

        Collection<javax.naming.directory.ModificationItem> list = new ArrayList<javax.naming.directory.ModificationItem>();

        for (Modification modification : modifications) {

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();
            if (debug) log.debug(" - "+LDAPUtil.getModificationOperations(type)+": "+attribute.getName());

            javax.naming.directory.Attribute attr = convertAttribute(attribute);
            list.add(new javax.naming.directory.ModificationItem(type, attr));
        }

        javax.naming.directory.ModificationItem mods[] = list.toArray(new javax.naming.directory.ModificationItem[list.size()]);

        javax.naming.ldap.LdapContext context = null;

        try {
            context = open();
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

        javax.naming.ldap.LdapContext context = null;

        try {
            context = open();
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
            SearchResponse<SearchResult> response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        DNBuilder db = new DNBuilder();
        db.set(request.getDn());
        db.append(suffix);
        DN baseDn = db.toDn();

        String filter = request.getFilter() == null ? "(objectClass=*)" : request.getFilter().toString();

        if (debug) log.debug("Search \""+ baseDn +"\" with filter "+filter+" with scope "+ LDAPUtil.getScope(request.getScope()));

        String attributeNames[] = request.getAttributes().toArray(new String[request.getAttributes().size()]);

        javax.naming.directory.SearchControls sc = new javax.naming.directory.SearchControls();
        sc.setSearchScope(request.getScope());
        sc.setReturningAttributes(request.getAttributes().isEmpty() ? null : attributeNames);
        sc.setCountLimit(request.getSizeLimit());
        sc.setTimeLimit((int) request.getTimeLimit());

        javax.naming.ldap.LdapContext context = null;
        NamingEnumeration ne = null;

        try {
            Collection<Control> origControls = convertControls(request.getControls());
            Collection<Control> requestControls = new ArrayList<Control>();

            if (pageSize > 0) {
                requestControls.add(new javax.naming.ldap.PagedResultsControl(pageSize, Control.NONCRITICAL));
            }

            //String referral = "follow";
            String referral = "throw";

            for (Control control : origControls) {
                if (control.getID().equals("2.16.840.1.113730.3.4.2")) {
                    referral = "ignore";
                    continue;
                }
                requestControls.add(control);
            }

            Hashtable<String,Object> env = new Hashtable<String,Object>();
            env.putAll(parameters);
            env.put(Context.REFERRAL, referral);

            context = open(env);

            boolean moreReferrals = true;

            while (moreReferrals) {
                try {
                    int page = 0;
                    byte[] cookie;

                    do {
                        if (debug) {
                            log.debug("Request Controls:");
                            for (Control control : requestControls) {
                                log.debug(" - "+control.getID());
                            }
                        }

                        context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));

                        if (debug) log.debug("Searching page #"+page);
                        ne = context.search(baseDn.toString(), filter, sc);

                        while (ne.hasMore()) {
                            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                            response.add(createSearchResult(request, sr));
                        }

                        // get cookie returned by server
                        Control[] responseControls = context.getResponseControls();
                        cookie = null;

                        if (responseControls != null) {
                            log.debug("Response Controls:");
                            for (Control control : responseControls) {
                                log.debug(" - "+control.getID());
                                if (control instanceof PagedResultsResponseControl) {
                                    PagedResultsResponseControl prrc = (PagedResultsResponseControl) control;
                                    cookie = prrc.getCookie();
                                }
                            }
                        }

                        // pass cookie back to server for the next page
                        requestControls = new ArrayList<Control>();
                        requestControls.addAll(origControls);

                        if (pageSize > 0 && cookie != null) {
                            requestControls.add(new javax.naming.ldap.PagedResultsControl(pageSize, cookie, Control.CRITICAL));
                        }

                        page++;

                    } while (cookie != null && cookie.length != 0);

                    moreReferrals = false;

                } catch (PartialResultException e) {
                    log.debug(e.getMessage(), e);
                    moreReferrals = false;

                } catch (ReferralException e) {
                    String ref = e.getReferralInfo().toString();
                    log.debug("Referral: "+ ref);

                    LDAPUrl url = new LDAPUrl(ref);
                    DN dn = new DN(url.getDN());

                    Attributes attributes = new Attributes();
                    attributes.setValue("ref", ref);
                    attributes.setValue("objectClass", "referral");

                    SearchResult result = new SearchResult(dn, attributes);
                    response.add(result);
                    //response.addReferral(ref);

                    moreReferrals = e.skipReferral();

                    if (moreReferrals) {
                        context = (javax.naming.ldap.LdapContext)e.getReferralContext();
                    }
                }
            }

        } finally {
            if (ne != null) try { ne.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            //if (connection != null) try { connection.disconnect(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            response.close();
        }
    }

    public SearchResult createSearchResult(
            SearchRequest request,
            javax.naming.directory.SearchResult sr
    ) throws Exception {

        String s = sr.getName();
        log.debug("SearchResult: ["+s+"]");

        DNBuilder db = new DNBuilder();
        if (s.startsWith("ldap://")) {
            LDAPUrl url = new LDAPUrl(s);
            db.set(LDAPUrl.decode(url.getDN()));
        } else {
            db.set(s);
        }

        db.append(request.getDn());

        DN dn = db.toDn();

        Attributes attributes = new Attributes();
        for (NamingEnumeration e = sr.getAttributes().getAll(); e.hasMore(); ) {
            javax.naming.directory.Attribute attr = (javax.naming.directory.Attribute)e.next();
            String name = attr.getID();

            for (NamingEnumeration ne = attr.getAll(); ne.hasMore(); ) {
                Object value = ne.next();
                attributes.addValue(name, value);
            }
        }

        return new SearchResult(dn, attributes);
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
            Object password = parameters.get(Context.SECURITY_CREDENTIALS);

            if (bindDn != null && !"".equals(bindDn) && password != null) {
                byte[] bytes;
                if (password instanceof byte[]) {
                    bytes = (byte[])password;
                } else {
                    bytes = password.toString().getBytes();
                }
                connection.bind(3, bindDn, bytes);
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
        javax.naming.directory.Attribute namingContexts = rootDSE.getAttributes().get("namingContexts");

        Collection<String> list = new ArrayList<String>();
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
            javax.naming.directory.Attribute schemaNamingContext = rootDSE.getAttributes().get("schemaNamingContext");
            javax.naming.directory.Attribute subschemaSubentry = rootDSE.getAttributes().get("subschemaSubentry");

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

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        javax.naming.directory.SearchControls searchControls = new javax.naming.directory.SearchControls();
        searchControls.setSearchScope(javax.naming.directory.SearchControls.ONELEVEL_SCOPE);

        javax.naming.ldap.LdapContext context = null;
        NamingEnumeration ne = null;

        try {
            Collection<Control> requestControls = new ArrayList<Control>();
            requestControls.add(new javax.naming.ldap.PagedResultsControl(100, Control.NONCRITICAL));

            context = open();

            int page = 0;
            byte[] cookie;

            do {
                if (debug) {
                    log.debug("Request Controls:");
                    for (Control control : requestControls) {
                        log.debug(" - "+control.getID());
                    }
                }

                context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));

                if (debug) log.debug("Searching page #"+page);
                ne = context.search(schemaDn, "(objectClass=attributeSchema)", searchControls);

                while (ne.hasMore()) {
                    javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                    javax.naming.directory.Attributes attributes = sr.getAttributes();

                    javax.naming.directory.Attribute lDAPDisplayName = attributes.get("lDAPDisplayName");
                    String atName = (String)lDAPDisplayName.get();
                    //log.debug(" - "+atName);

                    AttributeType at = new AttributeType();
                    at.setName(atName);

                    javax.naming.directory.Attribute attributeID = attributes.get("attributeID");
                    if (attributeID != null) at.setOid(attributeID.get().toString());

                    javax.naming.directory.Attribute adminDescription = attributes.get("adminDescription");
                    if (adminDescription != null) at.setDescription(adminDescription.get().toString());

                    javax.naming.directory.Attribute attributeSyntax = attributes.get("attributeSyntax");
                    if (attributeSyntax != null) at.setSyntax(attributeSyntax.get().toString());

                    javax.naming.directory.Attribute isSingleValued = attributes.get("isSingleValued");
                    if (isSingleValued != null) at.setSingleValued(Boolean.valueOf(isSingleValued.get().toString()));

                    schema.addAttributeType(at);
                }

                // get cookie returned by server
                Control[] responseControls = context.getResponseControls();
                cookie = null;

                if (responseControls != null) {
                    log.debug("Response Controls:");
                    for (Control control : responseControls) {
                        log.debug(" - "+control.getID());
                        if (control instanceof PagedResultsResponseControl) {
                            PagedResultsResponseControl prrc = (PagedResultsResponseControl) control;
                            cookie = prrc.getCookie();
                        }
                    }
                }

                // pass cookie back to server for the next page
                requestControls = new ArrayList<Control>();

                if (cookie != null) {
                    requestControls.add(new javax.naming.ldap.PagedResultsControl(100, cookie, Control.CRITICAL));
                }

                page++;

            } while (cookie != null && cookie.length != 0);

        } finally {
            if (ne != null) try { ne.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public void getActiveDirectoryObjectClasses(Schema schema, String schemaDn) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        javax.naming.directory.SearchControls searchControls = new javax.naming.directory.SearchControls();
        searchControls.setSearchScope(javax.naming.directory.SearchControls.ONELEVEL_SCOPE);

        javax.naming.ldap.LdapContext context = null;
        NamingEnumeration ne = null;

        try {
            Collection<Control> requestControls = new ArrayList<Control>();
            requestControls.add(new javax.naming.ldap.PagedResultsControl(100, Control.NONCRITICAL));

            context = open();

            int page = 0;
            byte[] cookie;

            do {
                if (debug) {
                    log.debug("Request Controls:");
                    for (Control control : requestControls) {
                        log.debug(" - "+control.getID());
                    }
                }

                context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));

                if (debug) log.debug("Searching page #"+page);
                ne = context.search(schemaDn, "(objectClass=classSchema)", searchControls);

                while (ne.hasMore()) {
                    javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                    javax.naming.directory.Attributes attributes = sr.getAttributes();

                    javax.naming.directory.Attribute lDAPDisplayName = attributes.get("lDAPDisplayName");
                    String ocName = (String)lDAPDisplayName.get();
                    //log.debug(" - "+ocName);

                    ObjectClass oc = new ObjectClass();
                    oc.setName(ocName);

                    javax.naming.directory.Attribute governsID = attributes.get("governsID");
                    if (governsID != null) oc.setOid(governsID.get().toString());

                    javax.naming.directory.Attribute adminDescription = attributes.get("adminDescription");
                    if (adminDescription != null) oc.setDescription(adminDescription.get().toString());

                    javax.naming.directory.Attribute mustContain = attributes.get("mustContain");
                    if (mustContain != null) {
                        NamingEnumeration ne2 = mustContain.getAll();
                        while (ne2.hasMore()) {
                            String requiredAttribute = (String)ne2.next();
                            oc.addRequiredAttribute(requiredAttribute);
                        }
                    }

                    javax.naming.directory.Attribute systemMustContain = attributes.get("systemMustContain");
                    if (systemMustContain != null) {
                        NamingEnumeration ne2 = systemMustContain.getAll();
                        while (ne2.hasMore()) {
                            String requiredAttribute = (String)ne2.next();
                            oc.addRequiredAttribute(requiredAttribute);
                        }
                    }

                    javax.naming.directory.Attribute mayContain = attributes.get("mayContain");
                    if (mayContain != null) {
                        NamingEnumeration ne2 = mayContain.getAll();
                        while (ne2.hasMore()) {
                            String optionalAttribute = (String)ne2.next();
                            oc.addOptionalAttribute(optionalAttribute);
                        }
                    }

                    javax.naming.directory.Attribute systemMayContain = attributes.get("systemMayContain");
                    if (systemMayContain != null) {
                        NamingEnumeration ne2 = systemMayContain.getAll();
                        while (ne2.hasMore()) {
                            String optionalAttribute = (String)ne2.next();
                            oc.addOptionalAttribute(optionalAttribute);
                        }
                    }

                    schema.addObjectClass(oc);
                }

                // get cookie returned by server
                Control[] responseControls = context.getResponseControls();
                cookie = null;

                if (responseControls != null) {
                    log.debug("Response Controls:");
                    for (Control control : responseControls) {
                        log.debug(" - "+control.getID());
                        if (control instanceof PagedResultsResponseControl) {
                            PagedResultsResponseControl prrc = (PagedResultsResponseControl) control;
                            cookie = prrc.getCookie();
                        }
                    }
                }

                // pass cookie back to server for the next page
                requestControls = new ArrayList<Control>();

                if (cookie != null) {
                    requestControls.add(new javax.naming.ldap.PagedResultsControl(100, cookie, Control.CRITICAL));
                }

                page++;

            } while (cookie != null && cookie.length != 0);

        } finally {
            if (ne != null) try { ne.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
            if (context != null) try { context.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public Schema getLDAPSchema(String schemaDn) throws Exception {

        Schema schema = new Schema();

        log.debug("Searching "+schemaDn+" ...");

        javax.naming.ldap.LdapContext context = null;

        try {
            javax.naming.directory.SearchControls ctls = new javax.naming.directory.SearchControls();
            ctls.setSearchScope(javax.naming.directory.SearchControls.OBJECT_SCOPE);
            ctls.setReturningAttributes(new String[] { "attributeTypes", "objectClasses" });

            context = open();
            NamingEnumeration results = context.search(schemaDn, "(objectClass=*)", ctls);
            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)results.next();

            javax.naming.directory.Attributes attributes = sr.getAttributes();

            //log.debug("Object Classes:");
            javax.naming.directory.Attribute objectClasses = attributes.get("objectClasses");

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
            javax.naming.directory.Attribute attributeTypes = attributes.get("attributeTypes");

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
        for (org.safehaus.penrose.control.Control control : controls) {

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

        javax.naming.directory.SearchControls ctls = new javax.naming.directory.SearchControls();
        ctls.setSearchScope(javax.naming.directory.SearchControls.OBJECT_SCOPE);

        javax.naming.ldap.LdapContext context = null;

        try {
            if ("".equals(searchBase)) {
                return getRootDSE();
                
            } else {
                context = open();
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

    public Collection<javax.naming.directory.SearchResult> getChildren(String baseDn) throws Exception {

        Collection<javax.naming.directory.SearchResult> results = new ArrayList<javax.naming.directory.SearchResult>();

        javax.naming.ldap.LdapContext context = null;

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

                context = open();
                NamingEnumeration entries = context.search(searchBase, "(objectClass=*)", ctls);
                SearchResult rootDse = (SearchResult)entries.next();
*/
                javax.naming.directory.Attributes attributes = rootDse.getAttributes();
                javax.naming.directory.Attribute attribute = attributes.get("namingContexts");

                NamingEnumeration values = attribute.getAll();

                while (values.hasMore()) {
                    String dn = (String)values.next();
                    log.debug(" - "+dn);

                    javax.naming.directory.SearchResult entry = getEntry(dn);
                    results.add(entry);
                }

            } else {
                log.debug("Searching "+searchBase+":");

                javax.naming.directory.SearchControls ctls = new javax.naming.directory.SearchControls();
                ctls.setSearchScope(javax.naming.directory.SearchControls.ONELEVEL_SCOPE);

                context = open();
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

    public DN getSuffix() {
        return suffix;
    }

    public void setSuffix(DN suffix) {
        this.suffix = suffix;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Collection<Object> getAttributeValues(javax.naming.directory.Attribute attribute) throws Exception {

        Collection<Object> values = new ArrayList<Object>();

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

    public javax.naming.directory.Attributes convertAttributes(Attributes attributes) throws Exception {

        javax.naming.directory.Attributes attrs = new javax.naming.directory.BasicAttributes();
        for (Attribute attribute : attributes.getAll()) {
            attrs.put(convertAttribute(attribute));
        }

        return attrs;
    }

    public javax.naming.directory.Attribute convertAttribute(Attribute attribute) throws Exception {

        boolean debug = log.isDebugEnabled();

        String name = attribute.getName();
        javax.naming.directory.Attribute attr = new BasicAttribute(name);

        for (Object value : attribute.getValues()) {
            attr.add(value);
            if (debug) {
                if (value instanceof byte[]) {
                    log.debug(" - "+name+": "+BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
                } else {
                    log.debug(" - "+name+": "+value);
                }
            }
        }

        return attr;
    }
}
