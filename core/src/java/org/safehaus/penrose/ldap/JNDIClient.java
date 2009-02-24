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
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.SchemaParser;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.util.TextUtil;

import org.ietf.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import com.sun.jndi.ldap.BerDecoder;

public class JNDIClient implements Cloneable {

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

    public Collection<String> binaryAttributes = new HashSet<String>();

    public String url;

    public String bindDn;
    public Object password;

    public SearchResult rootDSE;
    public Schema schema;

    public int defaultPageSize = 100;
    public boolean connectionPool;

    public LdapContext connection;

    public JNDIClient(LdapContext connection, Map<String,?> parameters) throws Exception {
        this.connection = connection;

        init(parameters);
    }

    public JNDIClient(Map<String,?> parameters) throws Exception {
        this(parameters, false);
    }

    public JNDIClient(Map<String,?> parameters, boolean connect) throws Exception {

        init(parameters);

        if (connect) {
            try {
                connection = createConnection();

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void init(Map<String,?> parameters) throws Exception {
        this.parameters.putAll(parameters);

        url = (String)parameters.get(Context.PROVIDER_URL);
        bindDn = (String)parameters.get(Context.SECURITY_PRINCIPAL);
        password = parameters.get(Context.SECURITY_CREDENTIALS);

        for (String attribute : DEFAULT_BINARY_ATTRIBUTES) {
            binaryAttributes.add(attribute.toLowerCase());
        }

        String s = (String)parameters.get("java.naming.ldap.attributes.binary");
        if (s != null) {
            StringTokenizer st = new StringTokenizer(s);
            while (st.hasMoreTokens()) {
                String attribute = st.nextToken();
                binaryAttributes.add(attribute.toLowerCase());
            }
        }

        //connectionPool = "true".equals(parameters.get("com.sun.jndi.ldap.connect.pool"));
/*
        String timeout = (String)parameters.get("com.sun.jndi.ldap.connect.timeout");
        if (timeout == null) {

            timeout = System.getProperty("com.sun.jndi.ldap.connect.pool.timeout");
            if (timeout == null) timeout = "30000"; // 30 seconds

            this.parameters.put("com.sun.jndi.ldap.connect.timeout", timeout);
        }
*/
    }

    public LdapContext reconnect(LdapContext context) throws Exception {

        if (context == null) {
            context = createConnection();
            return context;
        }

        try {
            context.reconnect(null);
        } catch (CommunicationException e) {
            context = createConnection();
        }

        return context;
    }

    public void close() throws Exception {
        if (connection != null) {
            log.debug("Closing LDAP connection.");
            connection.close();
            connection = null;
        }
    }

    public void connect() throws Exception {
        connection = createConnection();
    }

    public LdapContext createConnection() throws Exception {

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

    public void setConnectionPool(boolean connectionPool) {
        this.connectionPool = connectionPool;
    }

    public boolean getConnectionPool() {
        return connectionPool;
    }

    public LdapContext getConnection() throws Exception {
        if (connectionPool) {
            log.debug("Creating connection pool connection.");
            return createConnection();

        } else {
            if (connection == null) connection = createConnection();
            return connection;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        LdapContext context = getConnection();

        try {
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

            String escapedDn = escape(dn);

            try {
                context.createSubcontext(escapedDn, attrs);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                context = reconnect(context);
                context.createSubcontext(escapedDn, attrs);
            }

        } finally {
            if (connectionPool) {
                context.close();
            }
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

        //close();
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

        LdapContext context = getConnection();

        try {
            DN targetDn = request.getDn();

            DNBuilder db = new DNBuilder();
            db.set(targetDn);
            //db.append(suffix);
            DN dn = db.toDn();

            String filter = "("+request.getAttributeName()+"={0})";
            Object args[] = new Object[] { request.getAttributeValue() };

            SearchControls sc = new SearchControls();
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

            String escapedDn = escape(dn);
            NamingEnumeration ne;

            try {
                ne = context.search(escapedDn, filter, args, sc);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                context = reconnect(context);
                ne = context.search(escapedDn, filter, args, sc);
            }

            boolean b = ne.hasMore();

            ne.close();

            return b;

        } finally {
            if (connectionPool) {
                context.close();
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        LdapContext context = getConnection();

        try {
            DN targetDn = request.getDn();

            DNBuilder db = new DNBuilder();
            db.set(targetDn);
            //db.append(suffix);
            DN dn = db.toDn();

            if (warn) log.warn("Deleting "+dn+".");

            String escapedDn = escape(dn);

            try {
                context.destroySubcontext(escapedDn);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                context = reconnect(context);
                context.destroySubcontext(escapedDn);
            }

        } finally {
            if (connectionPool) {
                context.close();
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {


        LdapContext context = getConnection();

        try {
            DN targetDn = request.getDn();

            DNBuilder db = new DNBuilder();
            db.set(targetDn);
            //db.append(suffix);
            DN dn = db.toDn();

            Collection<ModificationItem> list = new ArrayList<ModificationItem>();

            for (Modification modification : request.getModifications()) {
                int oldType = modification.getType();
                Attribute oldAttribute = modification.getAttribute();

                int newType;
                switch (oldType) {
                    case Modification.ADD:
                        newType = DirContext.ADD_ATTRIBUTE;
                        break;
                    case Modification.DELETE:
                        newType = DirContext.REMOVE_ATTRIBUTE;
                        break;
                    case Modification.REPLACE:
                        newType = DirContext.REPLACE_ATTRIBUTE;
                        break;
                    default:
                        throw LDAP.createException(LDAP.PROTOCOL_ERROR);
                }

                javax.naming.directory.Attribute newAttribute = convertAttribute(oldAttribute);
                list.add(new ModificationItem(newType, newAttribute));
            }

            ModificationItem mods[] = list.toArray(new ModificationItem[list.size()]);

            if (warn) log.warn("Modifying "+dn+".");

            if (debug) {
                for (ModificationItem mi : mods) {
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

            String escapedDn = escape(dn);

            try {
                context.modifyAttributes(escapedDn, mods);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                context = reconnect(context);
                context.modifyAttributes(escapedDn, mods);
            }

        } finally {
            if (connectionPool) {
                context.close();
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        LdapContext context = getConnection();

        try {
            DN targetDn = request.getDn();
            RDN newRdn = request.getNewRdn();

            DNBuilder db = new DNBuilder();
            db.set(targetDn);
            //db.append(suffix);
            DN dn = db.toDn();

            if (warn) log.warn("Renaming "+dn+" to "+newRdn+".");

            String escapedDn = escape(dn);
            String escapedNewRdn = escape(newRdn);

            try {
                context.rename(escapedDn, escapedNewRdn);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                context = reconnect(context);
                context.rename(escapedDn, escapedNewRdn);
            }

        } finally {
            if (connectionPool) {
                context.close();
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        LdapContext context = getConnection();

        try {
            if (log.isDebugEnabled()) {
                log.debug(TextUtil.displaySeparator(80));
                log.debug(TextUtil.displayLine("LDAP SEARCH", 80));
                log.debug(TextUtil.displaySeparator(80));
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
                        s = BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])value, 0, 10);
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

            SearchControls sc = new SearchControls();
            sc.setSearchScope(scope);
            sc.setReturningAttributes(attributes.isEmpty() ? null : attributeNames);
            sc.setCountLimit(sizeLimit);
            sc.setTimeLimit((int)timeLimit);
            sc.setReturningObjFlag(!typesOnly);

            Collection<Control> origControls = convertControls(request.getControls());
            Collection<Control> requestControls = new ArrayList<Control>();

            //String referral = "follow";
            String referral = null; // "throw";
            int pageSize = defaultPageSize;

            for (Control control : origControls) {
                String id = control.getID();

                if (id.equals(ManageReferralControl.OID)) {
                    referral = "ignore";

                } else if (id.equals(PagedResultsControl.OID)) {

                    byte[] value = control.getEncodedValue();

                    BerDecoder ber = new BerDecoder(value, 0, value.length);
                    ber.parseSeq(null);
                    pageSize = ber.parseInt();
                    //cookie = ber.parseOctetString(Ber.ASN_OCTET_STR, null);

                } else {
                    requestControls.add(control);
                }
            }

            //if (referral != null) {
            //    this.parameters.put(Context.REFERRAL, referral);
            //}

            if (pageSize > 0) {
                requestControls.add(new PagedResultsControl(pageSize, Control.NONCRITICAL));
            }

            //Hashtable<String,Object> env = new Hashtable<String,Object>();
            //env.putAll(parameters);
            //env.put(Context.REFERRAL, referral);

            //context = open(env);

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

                        try {
                            context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));

                        } catch (CommunicationException e) {
                            log.error(e.getMessage(), e);
                            context = reconnect(context);
                            context.setRequestControls(requestControls.toArray(new Control[requestControls.size()]));
                        }

                        String escapedBaseDn = escape(baseDn);

                        NamingEnumeration ne;

                        try {
                            ne = context.search(escapedBaseDn, filter, values, sc);

                        } catch (CommunicationException e) {
                            log.error(e.getMessage(), e);
                            context = reconnect(context);
                            ne = context.search(escapedBaseDn, filter, values, sc);
                        }

                        while (ne.hasMore()) {
                            if (response.isClosed()) return;
                            javax.naming.directory.SearchResult sr = (javax.naming.directory.SearchResult)ne.next();
                            response.add(createSearchResult(request, sr));
                        }

                        ne.close();

                        cookie = null;

                        // get cookie returned by server
                        Control[] responseControls = context.getResponseControls();
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

                        for (Control control : origControls) {
                            String id = control.getID();

                            if (id.equals(ManageReferralControl.OID)) {

                            } else if (id.equals(PagedResultsControl.OID)) {

                            } else {
                                requestControls.add(control);
                            }
                        }

                        if (pageSize > 0 && cookie != null) {
                            requestControls.add(new PagedResultsControl(pageSize, cookie, Control.NONCRITICAL));
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
            if (connectionPool) {
                context.close();
            }
        }
    }

    public SearchResult createSearchResult(
            SearchRequest request,
            javax.naming.directory.SearchResult sr
    ) throws Exception {

        String s = sr.getName();

        DNBuilder db = new DNBuilder();
        if (s.startsWith("ldap://")) {
            LDAPUrl url = new LDAPUrl(s);
            db.set(LDAPUrl.decode(url.getDN()));
        } else {
            db.set(s);
        }

        db.append(request.getDn());

        DN dn = db.toDn();
        if (debug) log.debug("SearchResult: ["+dn+"]");

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

        Schema schema = new Schema("ad");

        getActiveDirectoryAttributeTypes(schema, schemaDn);
        getActiveDirectoryObjectClasses(schema, schemaDn);

        return schema;
    }

    public void getActiveDirectoryAttributeTypes(Schema schema, String schemaDn) throws Exception {

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        LdapContext context = getConnection();

        try {
            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

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
                    context = reconnect(context);
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

        } finally {
            if (connectionPool) {
                context.close();
            }
        }
    }

    public void getActiveDirectoryObjectClasses(Schema schema, String schemaDn) throws Exception {

        LdapContext context = getConnection();

        try {
            if (debug) log.debug("Search \""+ schemaDn +"\"");

            SearchControls searchControls = new SearchControls();
            searchControls.setSearchScope(SearchControls.ONELEVEL_SCOPE);

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
                    context = reconnect(context);
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

        } finally {
            if (connectionPool) {
                context.close();
            }
        }
    }

    public Schema getLDAPSchema(String schemaDn) throws Exception {

        LdapContext context = getConnection();

        try {
            Schema schema = new Schema("ldap");

            if (debug) log.debug("Searching "+schemaDn+" ...");

            SearchControls ctls = new SearchControls();
            ctls.setSearchScope(SearchControls.OBJECT_SCOPE);
            ctls.setReturningAttributes(new String[] { "attributeTypes", "objectClasses" });

            NamingEnumeration ne;

            try {
                ne = context.search(schemaDn, "(objectClass=*)", ctls);

            } catch (CommunicationException e) {
                log.error(e.getMessage(), e);
                context = reconnect(context);
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

        } finally {
            if (connectionPool) {
                context.close();
            }
        }
    }

    public AttributeType parseAttributeType(String line) throws Exception {
        try {
            line = "attributetype "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Schema schema = parser.parse();
            return schema.getAttributeTypes().iterator().next();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public ObjectClass parseObjectClass(String line) throws Exception {
        try {
            line = "objectclass "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Schema schema = parser.parse();
            return schema.getObjectClasses().iterator().next();

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

    public SearchResult find(String dn) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(request, response);

        if (!response.hasNext()) return null;

        return response.next();
    }

    public Collection<SearchResult> getChildren(String baseDn) throws Exception {

        Collection<SearchResult> results = new ArrayList<SearchResult>();

        DNBuilder db = new DNBuilder();
        db.set(baseDn);
        DN searchBase = db.toDn();

        if (searchBase.isEmpty()) {
            SearchResult rootDse = getRootDSE();

            Attributes attributes = rootDse.getAttributes();
            Attribute attribute = attributes.get("namingContexts");

            for (Object value : attribute.getValues()) {
                String dn = (String)value;
                if (debug) log.debug(" - "+dn);

                SearchResult entry = find(dn);
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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setRootDSE(SearchResult rootDSE) {
        this.rootDSE = rootDSE;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public javax.naming.directory.Attributes convertAttributes(Attributes attributes) throws Exception {

        javax.naming.directory.Attributes attrs = new BasicAttributes();
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

        JNDIClient client = (JNDIClient)super.clone();

        client.parameters = new Hashtable<String,Object>();
        client.parameters.putAll(parameters);

        client.binaryAttributes = new ArrayList<String>();
        client.binaryAttributes.addAll(binaryAttributes);

        client.url = url;

        client.rootDSE = rootDSE;
        client.schema = schema;

        client.defaultPageSize = defaultPageSize;
        client.connectionPool = connectionPool;

        try {
            if (connection != null) client.connection = connection.newInstance(null);

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return client;
    }
}