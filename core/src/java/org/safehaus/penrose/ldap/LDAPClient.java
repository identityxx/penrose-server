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

import javax.naming.*;
import javax.naming.directory.*;
import javax.naming.ldap.*;

import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaParser;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.util.BinaryUtil;

import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class LDAPClient implements Cloneable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean warn = log.isWarnEnabled();
    public boolean debug = log.isDebugEnabled();

    public Collection<String> DEFAULT_BINARY_ATTRIBUTES = Arrays.asList(
        "photo", "personalSignature", "audio", "jpegPhoto", "javaSerializedData",
        "thumbnailPhoto", "thumbnailLogo", "userPassword", "userCertificate",
        "cACertificate", "authorityRevocationList", "certificateRevocationList",
        "crossCertificatePair", "x500UniqueIdentifier"
    );

    public Hashtable<String,Object> parameters = new Hashtable<String,Object>();

    public Collection<String> binaryAttributes = new ArrayList<String>();

    public DN suffix;
    public String url;

    public String bindDn;
    public Object password;

    public SearchResult rootDSE;
    public Schema schema;

    public int pageSize = 100;

    public LdapContext context;

    public LDAPClient(LdapContext context, Map<String,?> parameters) throws Exception {
        this.context = context;

        parseParameters(parameters);
    }

    public LDAPClient(Map<String,?> parameters) throws Exception {
        this(parameters, true);
    }

    public LDAPClient(Map<String,?> parameters, boolean connect) throws Exception {

        parseParameters(parameters);

        if (connect) {
            try {
                init();

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void parseParameters(Map<String,?> parameters) throws Exception {
        this.parameters.putAll(parameters);

        this.parameters.put(Context.REFERRAL, "ignore");

        String providerUrl = (String)parameters.get(Context.PROVIDER_URL);

        //int index = providerUrl.indexOf("://");
        //if (index < 0) throw new Exception("Invalid URL: "+providerUrl);

        //index = providerUrl.indexOf("/", index+3);

        //if (index >= 0) {
            //suffix = new DN(providerUrl.substring(index+1));
            //url = providerUrl.substring(0, index);
        //} else {
            suffix = new DN();
            url = providerUrl;
        //}

        //this.parameters.put(Context.PROVIDER_URL, url);

        bindDn = (String)parameters.get(Context.SECURITY_PRINCIPAL);
        password = parameters.get(Context.SECURITY_CREDENTIALS);

        binaryAttributes.addAll(DEFAULT_BINARY_ATTRIBUTES);

        String s = (String)parameters.get("java.naming.ldap.attributes.binary");
        //log.debug("java.naming.ldap.attributes.binary: "+s);

        if (s != null) {
            StringTokenizer st = new StringTokenizer(s);
            while (st.hasMoreTokens()) {
                String attribute = st.nextToken();
                binaryAttributes.add(attribute.toLowerCase());
            }
        }

        this.parameters.put("com.sun.jndi.ldap.connect.pool", "true");

        if (!this.parameters.containsKey("com.sun.jndi.ldap.connect.timeout")) {
            this.parameters.put("com.sun.jndi.ldap.connect.timeout", "30000"); // 30 seconds
        }
    }

    public void init() throws Exception {
        if (context != null) return;
        connect();
    }

    public void reconnect() throws Exception {

        if (context == null) {
            connect();
            return;
        }

        try {
            context.reconnect(null);
        } catch (CommunicationException e) {
            connect();
        }
    }

    public void close() throws Exception {
        if (context != null) context.close();
    }

    public void connect() throws Exception {
        context = open(parameters);
    }

    public LdapContext open(Hashtable<String,Object> parameters) throws Exception {

        if (debug) {
            log.debug("Creating InitialLdapContext:");

            for (String name : parameters.keySet()) {
                Object value = parameters.get(name);

                if (Context.SECURITY_CREDENTIALS.equals(name) && value instanceof byte[]) {
                    log.debug(" - " + name + ": " + new String((byte[])value));
                } else {
                    log.debug(" - " + name + ": " + value);
                }
            }
        }

        LdapContext context = new InitialLdapContext(parameters, null);
        if (debug) log.debug("Connected to "+context.getEnvironment().get(Context.PROVIDER_URL));

        return context;
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
        //db.append(suffix);
        DN dn = db.toDn();

        javax.naming.directory.Attributes attrs = convertAttributes(attributes);

        if (warn) log.warn("Adding "+dn+".");

        if (debug) {
            log.debug("Attributes:");
            NamingEnumeration ne = attrs.getAll();
            while (ne.hasMore()) {
                javax.naming.directory.Attribute attribute = (javax.naming.directory.Attribute)ne.next();
                String name = attribute.getID();

                NamingEnumeration ne2 = attribute.getAll();
                while (ne2.hasMore()) {
                    Object value = ne2.next();
                    if (value instanceof byte[]) {
                        log.debug(" - "+name+": "+BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
                    } else {
                        log.debug(" - "+name+": "+value);
                    }
                }
                ne2.close();
            }
            ne.close();
        }

        init();

        try {
            context.createSubcontext(escape(dn), attrs);

        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
            reconnect();
            context.createSubcontext(escape(dn), attrs);
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
        //db.append(suffix);
        DN dn = db.toDn();

        this.bindDn = escape(dn);
        this.password = password;
        
        parameters.put(Context.SECURITY_PRINCIPAL, this.bindDn);
        parameters.put(Context.SECURITY_CREDENTIALS, this.password);

        if (warn) log.warn("Binding as "+dn+".");
        
        if (debug) {
            log.debug("Password: "+new String(password));
        }

        close();
        //context = connect();

        LDAPUrl ldapUrl = new LDAPUrl(url);

        String server = ldapUrl.getHost();
        int port = ldapUrl.getPort();

        LDAPConnection connection = new LDAPConnection();
        connection.connect(server, port);
        connection.bind(3, dn.toString(), password);
        connection.disconnect();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN targetDn = request.getDn();

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        //db.append(suffix);
        DN dn = db.toDn();

        String filter = "("+request.getAttributeName()+"={0})";
        Object args[] = new Object[] { request.getAttributeValue() };

        javax.naming.directory.SearchControls sc = new javax.naming.directory.SearchControls();
        sc.setReturningAttributes(new String[] {});
        sc.setSearchScope(SearchControls.OBJECT_SCOPE);

        if (warn) log.warn("Comparing "+dn+".");

        if (debug) {
            Object value = request.getAttributeValue();
            if (value instanceof byte[]) {
                log.debug(" - "+request.getAttributeName()+": "+BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                log.debug(" - "+request.getAttributeName()+": "+value);
            }
        }

        init();

        NamingEnumeration ne;

        try {
            ne = context.search(escape(dn), filter, args, sc);

        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
            reconnect();
            ne = context.search(escape(dn), filter, args, sc);
        }

        boolean b = ne.hasMore();

        ne.close();

        return b;
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
        //db.append(suffix);
        DN dn = db.toDn();

        if (warn) log.warn("Deleting "+dn+".");

        init();

        try {
            context.destroySubcontext(escape(dn));

        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
            reconnect();
            context.destroySubcontext(escape(dn));
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

        DNBuilder db = new DNBuilder();
        db.set(targetDn);
        //db.append(suffix);
        DN dn = db.toDn();

        Collection<javax.naming.directory.ModificationItem> list = new ArrayList<javax.naming.directory.ModificationItem>();

        for (Modification modification : request.getModifications()) {
            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            javax.naming.directory.Attribute attr = convertAttribute(attribute);
            list.add(new javax.naming.directory.ModificationItem(type, attr));
        }

        javax.naming.directory.ModificationItem mods[] = list.toArray(new javax.naming.directory.ModificationItem[list.size()]);

        if (warn) log.warn("Modifying "+dn+".");

        if (debug) {
            for (javax.naming.directory.ModificationItem mi : mods) {
                javax.naming.directory.Attribute attribute = mi.getAttribute();
                int type = mi.getModificationOp();
                String name = attribute.getID();
                log.debug(" - "+LDAP.getModificationOperation(type)+": "+name);

                NamingEnumeration ne2 = attribute.getAll();
                while (ne2.hasMore()) {
                    Object value = ne2.next();
                    if (value instanceof byte[]) {
                        log.debug("   - "+name+": "+BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
                    } else {
                        log.debug("   - "+name+": "+value);
                    }
                }
                ne2.close();
            }
        }

        init();

        try {
            context.modifyAttributes(escape(dn), mods);

        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
            reconnect();
            context.modifyAttributes(escape(dn), mods);
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
        //db.append(suffix);
        DN dn = db.toDn();

        if (warn) log.warn("Renaming "+dn+" to "+newRdn+".");

        init();

        try {
            context.rename(escape(dn), escape(newRdn));

        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
            reconnect();
            context.rename(escape(dn), escape(newRdn));
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        if (log.isDebugEnabled()) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("LDAP SEARCH", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        DNBuilder db = new DNBuilder();
        db.set(request.getDn());
        //db.append(suffix);
        DN baseDn = db.toDn();

        Collection<Object> list = new ArrayList<Object>();
        String filter = request.getFilter() == null ? "(objectClass=*)" : request.getFilter().toString(list);
        Object values[] = list.toArray();

        int scope = request.getScope();

        Collection<String> attributes = request.getAttributes();
        String attributeNames[] = attributes.toArray(new String[attributes.size()]);

        long sizeLimit = request.getSizeLimit();
        long timeLimit = request.getTimeLimit();
        boolean typesOnly = request.isTypesOnly();

        //if (warn) log.warn("Searching "+baseDn+".");

        if (debug) {
            log.debug("Base       : "+baseDn+".");
            log.debug("Filter     : "+filter);
            for (Object value : values) {
                String s;
                if (value instanceof byte[]) {
                    s = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])value);
                } else {
                    s = value.toString();
                }
                log.debug(" - "+s+" ("+value.getClass().getSimpleName()+")");
            }
            log.debug("Scope      : "+ LDAP.getScope(scope));
            log.debug("Attributes : "+attributes);
            log.debug("Size Limit : "+sizeLimit);
            log.debug("Time Limit : "+timeLimit);
        }

        javax.naming.directory.SearchControls sc = new javax.naming.directory.SearchControls();
        sc.setSearchScope(scope);
        sc.setReturningAttributes(attributes.isEmpty() ? null : attributeNames);
        sc.setCountLimit(sizeLimit);
        sc.setTimeLimit((int)timeLimit);
        sc.setReturningObjFlag(!typesOnly);


        try {
            Collection<Control> origControls = convertControls(request.getControls());
            Collection<Control> requestControls = new ArrayList<Control>();

            if (pageSize > 0) {
                requestControls.add(new PagedResultsControl(pageSize, Control.NONCRITICAL));
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

            //Hashtable<String,Object> env = new Hashtable<String,Object>();
            //env.putAll(parameters);
            //env.put(Context.REFERRAL, referral);

            //context = open(env);

            init();

            boolean moreReferrals = true;

            while (moreReferrals) {
                try {
                    int page = 0;
                    byte[] cookie;

                    do {
                        if (debug) {
                            log.debug("Searching page #"+page);

                            log.debug("Request Controls:");
                            for (Control control : requestControls) {
                                log.debug(" - "+control.getID());
                            }
                        }

                        NamingEnumeration ne;

                        try {
                            context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));
                            ne = context.search(escape(baseDn), filter, values, sc);

                        } catch (CommunicationException e) {
                            log.error(e.getMessage(), e);
                            reconnect();
                            context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));
                            ne = context.search(escape(baseDn), filter, values, sc);
                        }

                        while (ne.hasMore()) {
                            if (response.isClosed()) return;
                            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                            response.add(createSearchResult(request, sr));
                        }

                        ne.close();

                        // get cookie returned by server
                        Control[] responseControls = context.getResponseControls();
                        cookie = null;

                        if (responseControls != null) {
                            if (debug) log.debug("Response Controls:");
                            for (Control control : responseControls) {
                                if (debug) log.debug(" - "+control.getID());
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
                            requestControls.add(new PagedResultsControl(pageSize, cookie, Control.CRITICAL));
                        }

                        page++;

                    } while (cookie != null && cookie.length != 0);

                    moreReferrals = false;

                } catch (PartialResultException e) {
                    log.error(e.getMessage(), e);
                    moreReferrals = false;

                } catch (ReferralException e) {
                    String ref = e.getReferralInfo().toString();
                    if (debug) log.debug("Referral: "+ ref);

                    LDAPUrl url = new LDAPUrl(ref);
                    DN dn = new DN(url.getDN());

                    Attributes attrs = new Attributes();
                    attrs.setValue("ref", ref);
                    attrs.setValue("objectClass", "referral");

                    SearchResult result = new SearchResult(dn, attrs);
                    response.add(result);
                    //response.addReferral(ref);

                    moreReferrals = e.skipReferral();

                    if (moreReferrals) {
                        context = (LdapContext)e.getReferralContext();
                    }
                }
            }

        } finally {
            response.close();
        }
    }

    public SearchResult createSearchResult(
            SearchRequest request,
            javax.naming.directory.SearchResult sr
    ) throws Exception {

        String s = sr.getName();
        if (debug) log.debug("SearchResult: ["+s+"]");

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

        NamingEnumeration ne = sr.getAttributes().getAll();

        while (ne.hasMore()) {
            javax.naming.directory.Attribute attr = (javax.naming.directory.Attribute)ne.next();
            String name = attr.getID();

            NamingEnumeration ne2 = attr.getAll();
            while (ne2.hasMore()) {
                Object value = ne2.next();
                attributes.addValue(name, value);
            }
            ne2.close();
        }

        ne.close();

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

    public SearchResult getRootDSE() throws Exception {

        if (rootDSE != null) return rootDSE;

        if (debug) log.debug("Searching Root DSE ...");

        SearchRequest request = new SearchRequest();
        request.setScope(SearchRequest.SCOPE_BASE);
        request.setAttributes(new String[] { "*", "+" });

        SearchResponse response = new SearchResponse();
        search(request, response);

        if (!response.hasNext()) {
            rootDSE = null;
            return null;
        }

        rootDSE = response.next();
/*
        LDAPConnection connection = null;

        try {
            LDAPUrl ldapUrl = new LDAPUrl(url);

            connection = new LDAPConnection();
            connection.connect(ldapUrl.getHost(), ldapUrl.getPort());

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
            if (connection != null) try { connection.disconnect(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }
*/
        return rootDSE;
    }

    public SearchResult createSearchResult(LDAPEntry entry) {

        //log.debug("Converting attributes for "+entry.getDN());

        LDAPAttributeSet attributeSet = entry.getAttributeSet();
        Attributes attributes = new Attributes();

        for (Iterator i=attributeSet.iterator(); i.hasNext(); ) {
            LDAPAttribute ldapAttribute = (LDAPAttribute)i.next();
            //log.debug(" - "+ldapAttribute.getName()+": "+Arrays.asList(ldapAttribute.getSubtypes()));
            Attribute attribute = new Attribute(ldapAttribute.getName());

            for (Enumeration j=ldapAttribute.getStringValues(); j.hasMoreElements(); ) {
                String value = (String)j.nextElement();
                //log.debug("   - "+value);
                attribute.addValue(value);
            }

            attributes.add(attribute);
        }

        return new SearchResult(entry.getDN(), attributes);
    }

    public Collection<String> getNamingContexts() throws Exception {
        getRootDSE();

        Collection<String> list = new ArrayList<String>();

        Attribute namingContexts = rootDSE.getAttributes().get("namingContexts");
        if (namingContexts == null) return list;

        for (Object value : namingContexts.getValues()) {
            String namingContext = (String)value;
            list.add(namingContext);
        }

        return list;
    }

    public Schema getSchema() throws Exception {

        if (schema != null) return schema;

        getRootDSE();

        if (debug) log.debug("Searching Schema ...");

        try {
            Attribute schemaNamingContext = rootDSE.getAttributes().get("schemaNamingContext");
            Attribute subschemaSubentry = rootDSE.getAttributes().get("subschemaSubentry");

            String schemaDn;

            if (schemaNamingContext != null) {
                schemaDn = (String)schemaNamingContext.getValue();
                if (debug) log.debug("Active Directory Schema: "+schemaDn);
                schema = getActiveDirectorySchema(schemaDn);

            } else if (subschemaSubentry != null) {
                schemaDn = (String)subschemaSubentry.getValue();
                if (debug) log.debug("Standard LDAP Schema: "+schemaDn);
                schema = getLDAPSchema(schemaDn);

            } else {
                schemaDn = "cn=schema";
                if (debug) log.debug("Default Schema: "+schemaDn);
                schema = getLDAPSchema(schemaDn);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }

        return schema;
    }

    public Schema getActiveDirectorySchema(String schemaDn) throws Exception {

        if (debug) log.debug("Searching "+schemaDn+" ...");

        Schema schema = new Schema();

        getActiveDirectoryAttributeTypes(schema, schemaDn);
        getActiveDirectoryObjectClasses(schema, schemaDn);

        return schema;
    }

    public void getActiveDirectoryAttributeTypes(Schema schema, String schemaDn) throws Exception {

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        javax.naming.directory.SearchControls searchControls = new javax.naming.directory.SearchControls();
        searchControls.setSearchScope(javax.naming.directory.SearchControls.ONELEVEL_SCOPE);

        init();

        Collection<Control> requestControls = new ArrayList<Control>();
        requestControls.add(new PagedResultsControl(100, Control.NONCRITICAL));

        int page = 0;
        byte[] cookie;

        do {
            if (debug) {
                log.debug("Searching page #"+page);

                log.debug("Request Controls:");
                for (Control control : requestControls) {
                    log.debug(" - "+control.getID());
                }
            }

            NamingEnumeration ne;

            try {
                context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));
                ne = context.search(schemaDn, "(objectClass=attributeSchema)", searchControls);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                reconnect();
                context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));
                ne = context.search(schemaDn, "(objectClass=attributeSchema)", searchControls);
            }

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

            ne.close();

            // get cookie returned by server
            Control[] responseControls = context.getResponseControls();
            cookie = null;

            if (responseControls != null) {
                if (debug) log.debug("Response Controls:");
                for (Control control : responseControls) {
                    if (debug) log.debug(" - "+control.getID());
                    if (control instanceof PagedResultsResponseControl) {
                        PagedResultsResponseControl prrc = (PagedResultsResponseControl) control;
                        cookie = prrc.getCookie();
                    }
                }
            }

            // pass cookie back to server for the next page
            requestControls = new ArrayList<Control>();

            if (cookie != null) {
                requestControls.add(new PagedResultsControl(100, cookie, Control.CRITICAL));
            }

            page++;

        } while (cookie != null && cookie.length != 0);
    }

    public void getActiveDirectoryObjectClasses(Schema schema, String schemaDn) throws Exception {

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        javax.naming.directory.SearchControls searchControls = new javax.naming.directory.SearchControls();
        searchControls.setSearchScope(javax.naming.directory.SearchControls.ONELEVEL_SCOPE);

        init();

        Collection<Control> requestControls = new ArrayList<Control>();
        requestControls.add(new PagedResultsControl(100, Control.NONCRITICAL));

        int page = 0;
        byte[] cookie;

        do {
            if (debug) {
                log.debug("Searching page #"+page);
                log.debug("Request Controls:");
                for (Control control : requestControls) {
                    log.debug(" - "+control.getID());
                }
            }

            NamingEnumeration ne;

            try {
                context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));
                ne = context.search(schemaDn, "(objectClass=classSchema)", searchControls);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                reconnect();
                context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));
                ne = context.search(schemaDn, "(objectClass=classSchema)", searchControls);
            }

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
                    ne2.close();
                }

                javax.naming.directory.Attribute systemMustContain = attributes.get("systemMustContain");
                if (systemMustContain != null) {
                    NamingEnumeration ne2 = systemMustContain.getAll();
                    while (ne2.hasMore()) {
                        String requiredAttribute = (String)ne2.next();
                        oc.addRequiredAttribute(requiredAttribute);
                    }
                    ne2.close();
                }

                javax.naming.directory.Attribute mayContain = attributes.get("mayContain");
                if (mayContain != null) {
                    NamingEnumeration ne2 = mayContain.getAll();
                    while (ne2.hasMore()) {
                        String optionalAttribute = (String)ne2.next();
                        oc.addOptionalAttribute(optionalAttribute);
                    }
                    ne2.close();
                }

                javax.naming.directory.Attribute systemMayContain = attributes.get("systemMayContain");
                if (systemMayContain != null) {
                    NamingEnumeration ne2 = systemMayContain.getAll();
                    while (ne2.hasMore()) {
                        String optionalAttribute = (String)ne2.next();
                        oc.addOptionalAttribute(optionalAttribute);
                    }
                    ne2.close();
                }

                schema.addObjectClass(oc);
            }

            ne.close();

            // get cookie returned by server
            Control[] responseControls = context.getResponseControls();
            cookie = null;

            if (responseControls != null) {
                if (debug) log.debug("Response Controls:");
                for (Control control : responseControls) {
                    if (debug) log.debug(" - "+control.getID());
                    if (control instanceof PagedResultsResponseControl) {
                        PagedResultsResponseControl prrc = (PagedResultsResponseControl) control;
                        cookie = prrc.getCookie();
                    }
                }
            }

            // pass cookie back to server for the next page
            requestControls = new ArrayList<Control>();

            if (cookie != null) {
                requestControls.add(new PagedResultsControl(100, cookie, Control.CRITICAL));
            }

            page++;

        } while (cookie != null && cookie.length != 0);
    }

    public Schema getLDAPSchema(String schemaDn) throws Exception {

        Schema schema = new Schema();

        if (debug) log.debug("Searching "+schemaDn+" ...");

        javax.naming.directory.SearchControls ctls = new javax.naming.directory.SearchControls();
        ctls.setSearchScope(javax.naming.directory.SearchControls.OBJECT_SCOPE);
        ctls.setReturningAttributes(new String[] { "attributeTypes", "objectClasses" });

        init();

        NamingEnumeration ne;

        try {
            ne = context.search(schemaDn, "(objectClass=*)", ctls);

        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
            reconnect();
            ne = context.search(schemaDn, "(objectClass=*)", ctls);
        }

        javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();

        javax.naming.directory.Attributes attributes = sr.getAttributes();

        //log.debug("Object Classes:");
        javax.naming.directory.Attribute objectClasses = attributes.get("objectClasses");

        NamingEnumeration ne2 = objectClasses.getAll();
        while (ne2.hasMore()) {
            String value = (String)ne2.next();
            ObjectClass oc = parseObjectClass(value);
            if (oc == null) continue;

            //log.debug(" - "+oc.getName());
            schema.addObjectClass(oc);
        }
        ne2.close();

        //log.debug("Attribute Types:");
        javax.naming.directory.Attribute attributeTypes = attributes.get("attributeTypes");

        NamingEnumeration ne3 = attributeTypes.getAll();
        while (ne3.hasMore()) {
            String value = (String)ne3.next();
            AttributeType at = parseAttributeType(value);
            if (at == null) continue;

            //log.debug(" - "+at.getName());
            schema.addAttributeType(at);
        }
        ne3.close();
        ne.close();

        return schema;
    }

    public AttributeType parseAttributeType(String line) throws Exception {
        try {
            line = "attributetype "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Collection schema = parser.parse();
            return (AttributeType)schema.iterator().next();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public ObjectClass parseObjectClass(String line) throws Exception {
        try {
            line = "objectclass "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Collection schema = parser.parse();
            return (ObjectClass)schema.iterator().next();

        } catch (Exception e) {
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

    public SearchResult getEntry(String dn) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(request, response);

        if (!response.hasNext()) return null;

        return response.next();
/*
        DNBuilder db = new DNBuilder();
        db.set(dn);
        db.append(suffix);
        DN searchBase = db.toDn();

        javax.naming.directory.SearchControls ctls = new javax.naming.directory.SearchControls();
        ctls.setSearchScope(javax.naming.directory.SearchControls.OBJECT_SCOPE);

        if (searchBase.isEmpty()) {
            return getRootDSE();
        }

        init();

        NamingEnumeration ne;

        try {
            ne = context.search(escape(searchBase), "(objectClass=*)", ctls);

        } catch (CommunicationException e) {
            log.error(e.getMessage(), e);
            reconnect();
            ne = context.search(escape(searchBase), "(objectClass=*)", ctls);
        }

        SearchResult sr;

        if (ne.hasMore()) {
            sr = (SearchResult)ne.next();
            sr.setName(dn);

        } else {
            sr = null;
        }

        ne.close();

        return sr;
*/
    }

    public Collection<SearchResult> getChildren(String baseDn) throws Exception {

        Collection<SearchResult> results = new ArrayList<SearchResult>();

        DNBuilder db = new DNBuilder();
        db.set(baseDn);
        //db.append(suffix);
        DN searchBase = db.toDn();

        if (searchBase.isEmpty()) {
            SearchResult rootDse = getRootDSE();
/*
            log.debug("Searching Root DSE:");

            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.OBJECT_SCOPE);

            NamingEnumeration ne = context.search(searchBase, "(objectClass=*)", ctls);
            SearchResult rootDse = (SearchResult)ne.next();
            ne.close();
*/
            Attributes attributes = rootDse.getAttributes();
            Attribute attribute = attributes.get("namingContexts");

            for (Object value : attribute.getValues()) {
                String dn = (String)value;
                if (debug) log.debug(" - "+dn);

                SearchResult entry = getEntry(dn);
                results.add(entry);
            }

        } else {
            if (debug) log.debug("Searching "+searchBase+":");

            SearchRequest request = new SearchRequest();
            request.setDn(baseDn);
            request.setScope(SearchRequest.SCOPE_ONE);

            SearchResponse response = new SearchResponse();

            search(request, response);

            while (response.hasNext()) {
                SearchResult sr = response.next();
                if (debug) log.debug(" - "+sr.getDn());
                results.add(sr);
            }
/*
            javax.naming.directory.SearchControls ctls = new javax.naming.directory.SearchControls();
            ctls.setSearchScope(javax.naming.directory.SearchControls.ONELEVEL_SCOPE);

            init();

            NamingEnumeration ne;

            try {
                ne = context.search(escape(searchBase), "(objectClass=*)", ctls);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                reconnect();
                ne = context.search(escape(searchBase), "(objectClass=*)", ctls);
            }

            while (ne.hasMore()) {
                SearchResult sr = (SearchResult)ne.next();
                db.set(sr.getName());
                db.append(baseDn);
                DN dn = db.toDn();

                if (debug) log.debug(" - "+dn);
                sr.setName(dn.toString());
                results.add(sr);
            }

            ne.close();
*/
        }

        return results;
    }

    public static String[] parseURL(String s) {

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

        NamingEnumeration ne = attribute.getAll();

        while (ne.hasMore()) {
            Object value = ne.next();
            values.add(value);
        }

        ne.close();

        return values;
    }

    public void setRootDSE(SearchResult rootDSE) {
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

        String name = attribute.getName();
        javax.naming.directory.Attribute attr = new BasicAttribute(name);

        for (Object value : attribute.getValues()) {
            attr.add(value);
        }

        return attr;
    }


    public String escape(RDN rdn) {
        return escape(rdn.toString());
    }

    public String escape(DN dn) {
        return escape(dn.toString());
    }

    public String escape(String string) {
        String s = string.replaceAll("/", "\\\\/");
        if (debug) log.debug("Escape ["+string+"] => ["+s+"].");
        return s;
    }

    public Object clone() throws CloneNotSupportedException {

        LDAPClient client = (LDAPClient)super.clone();

        client.parameters = new Hashtable<String,Object>();
        client.parameters.putAll(parameters);

        client.binaryAttributes = new ArrayList<String>();
        client.binaryAttributes.addAll(binaryAttributes);

        client.suffix = suffix;
        client.url = url;

        client.rootDSE = rootDSE;
        client.schema = schema;

        client.pageSize = pageSize;

        try {
            if (context != null) client.context = context.newInstance(null);

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return client;
    }
}
