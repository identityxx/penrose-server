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

import org.ietf.ldap.*;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.SchemaParser;
import org.safehaus.penrose.util.BinaryUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.*;

public class LDAPClient implements Cloneable, LDAPAuthHandler {

    public Logger log    = LoggerFactory.getLogger(getClass());
    public boolean warn  = log.isWarnEnabled();
    public boolean debug = log.isDebugEnabled();

    public final static Collection<String> DEFAULT_BINARY_ATTRIBUTES = Arrays.asList(
        "photo", "personalSignature", "audio", "jpegPhoto", "javaSerializedData",
        "thumbnailPhoto", "thumbnailLogo", "userPassword", "userCertificate",
        "cACertificate", "authorityRevocationList", "certificateRevocationList",
        "crossCertificatePair", "x500UniqueIdentifier"
    );

    public Collection<String> binaryAttributes = new HashSet<String>();

    public SearchResult rootDSE;
    public Schema schema;

    public int defaultPageSize = 100;

    public LDAPConnectionFactory connectionFactory;

    public LDAPConnection connection;

    public String bindDn;
    public byte[] bindPassword;

    public LDAPClient(String url) throws Exception {
        this(new LDAPConnectionFactory(url), false);
    }

    public LDAPClient(String url, boolean connect) throws Exception {
        this(new LDAPConnectionFactory(url), connect);
    }

    public LDAPClient(Map<String,?> parameters) throws Exception {
        this(new LDAPConnectionFactory(parameters), false);
    }

    public LDAPClient(Map<String,?> parameters, boolean connect) throws Exception {
        this(new LDAPConnectionFactory(parameters), connect);
    }

    public LDAPClient(LDAPConnectionFactory connectionFactory) throws Exception {
        this(connectionFactory, false);
    }

    public LDAPClient(LDAPConnectionFactory connectionFactory, boolean connect) throws Exception {
        this.connectionFactory = connectionFactory;
        init();
        if (connect) connect();
    }

    public void init() throws Exception {

        bindDn       = connectionFactory.bindDn;
        bindPassword = connectionFactory.bindPassword;

        binaryAttributes.addAll(connectionFactory.binaryAttributes);
    }

    public void connect() throws Exception {

        if (connection == null) {
            log.debug("Creating new LDAP connection.");
            connection = connectionFactory.createConnection();
            initConnection();

        } else if (!connection.isConnected()) {
            log.debug("Not connected, creating LDAP connection.");
            connectionFactory.connect(connection);
            initConnection();
        }
    }

    public void initConnection() throws Exception {
        if (bindDn != null && bindPassword != null) {
            log.debug("Binding as "+bindDn+".");
            connection.bind(3, bindDn, bindPassword);
        }

        LDAPConstraints constraints = new LDAPConstraints();
        constraints.setReferralHandler(this);

        connection.setConstraints(constraints);
    }

    public LDAPAuthProvider getAuthProvider(String host, int port) {
        log.debug("Creating authentication provider.");
        return new LDAPAuthProvider(bindDn, bindPassword);
    }

    public void close() throws Exception {
        log.debug("Closing LDAP connection.");
        connection.disconnect();
    }

    public LDAPConnection getConnection() throws Exception {
        connect();
        return connection;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            AddRequest request,
            AddResponse response
    ) throws Exception {

        String dn = request.getDn().toString();
        Attributes attributes = request.getAttributes();

        LDAPAttributeSet attributeSet = convertAttributes(attributes);

        if (warn) log.warn("Adding "+dn+".");

        if (debug) {
            log.debug("Attributes:");
            for (Object attr : attributeSet) {
                LDAPAttribute attribute = (LDAPAttribute) attr;
                String name = attribute.getName();

                if (binaryAttributes.contains(name)) {
                    for (byte[] value : attribute.getByteValueArray()) {
                        log.debug(" - " + name + ": " + BinaryUtil.encode(BinaryUtil.BIG_INTEGER, value, 0, 10)+"...");
                    }

                } else {
                    for (String value : attribute.getStringValueArray()) {
                        log.debug(" - " + name + ": " + value);
                    }
                }
            }
        }

        LDAPConstraints constraints = new LDAPConstraints();
        constraints.setReferralFollowing(true);

        Collection<LDAPControl> requestControls = convertControls(request.getControls());
        constraints.setControls(requestControls.toArray(new LDAPControl[requestControls.size()]));

        if (debug && !requestControls.isEmpty()) {
            log.debug("Request Controls:");
            for (LDAPControl control : requestControls) {
                log.debug(" - "+control.getID());
            }
        }

        LDAPEntry entry = new LDAPEntry(dn, attributeSet);

        LDAPConnection connection = getConnection();

        try {
            connection.add(entry, constraints);

        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            BindRequest request,
            BindResponse response
    ) throws Exception {

        String bindDn = request.getDn().toString();
        byte[] bindPassword = request.getPassword();

        if (warn) log.warn("Binding as "+bindDn+".");
        
        if (debug) {
            log.debug("Password: "+new String(bindPassword));
        }

        LDAPConnection connection = getConnection();

        try {
            connection.bind(3, bindDn, bindPassword);

            this.bindDn       = bindDn;
            this.bindPassword = bindPassword;

        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        String dn = request.getDn().toString();
        String name = request.getAttributeName();
        Object value = request.getAttributeValue();

        if (warn) log.warn("Comparing "+dn+".");

        if (debug) {
            if (binaryAttributes.contains(name.toLowerCase())) {
                log.debug(" - " + name + ": " + BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[]) value, 0, 10)+"...");

            } else {
                log.debug(" - " + name + ": " + value);
            }
        }

        LDAPAttribute attr = new LDAPAttribute(name);

        if (value instanceof byte[]) {
            attr.addValue((byte[])value);
        } else {
            attr.addValue(value.toString());
        }

        LDAPConstraints constraints = new LDAPConstraints();
        constraints.setReferralFollowing(true);

        Collection<LDAPControl> requestControls = convertControls(request.getControls());
        constraints.setControls(requestControls.toArray(new LDAPControl[requestControls.size()]));

        if (debug && !requestControls.isEmpty()) {
            log.debug("Request Controls:");
            for (LDAPControl control : requestControls) {
                log.debug(" - "+control.getID());
            }
        }

        LDAPConnection connection = getConnection();

        try {
            return connection.compare(dn, attr, constraints);

        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        String dn = request.getDn().toString();

        if (warn) log.warn("Deleting "+dn+".");

        LDAPConstraints constraints = new LDAPConstraints();
        constraints.setReferralFollowing(true);

        Collection<LDAPControl> requestControls = convertControls(request.getControls());
        constraints.setControls(requestControls.toArray(new LDAPControl[requestControls.size()]));

        if (debug && !requestControls.isEmpty()) {
            log.debug("Request Controls:");
            for (LDAPControl control : requestControls) {
                log.debug(" - "+control.getID());
            }
        }

        LDAPConnection connection = getConnection();

        try {
            connection.delete(dn, constraints);

        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult find(String dn) throws Exception {

        SearchRequest request = new SearchRequest();
        request.setDn(dn);
        request.setScope(SearchRequest.SCOPE_BASE);

        SearchResponse response = new SearchResponse();

        search(request, response);

        if (!response.hasNext()) return null;

        return response.next();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        String dn = request.getDn().toString();
        if (warn) log.warn("Modifying "+dn+".");

        Collection<LDAPModification> list = new ArrayList<LDAPModification>();

        for (Modification modification : request.getModifications()) {
            int type = modification.getType();
            Attribute attribute = modification.getAttribute();
            String name = attribute.getName();

            if (debug) {
                log.debug(" - "+LDAP.getModificationOperation(type)+": "+name);

                for (Object value : attribute.getValues()) {
                    if (value instanceof byte[]) {
                        log.debug(" - " + name + ": " + BinaryUtil.encode(BinaryUtil.BIG_INTEGER, (byte[])value, 0, 10)+"...");
                    } else {
                        log.debug(" - " + name + ": " + value);
                    }
                }
            }

            int newType;
            switch (type) {
                case Modification.ADD:
                    newType = LDAPModification.ADD;
                    break;
                case Modification.DELETE:
                    newType = LDAPModification.DELETE;
                    break;
                case Modification.REPLACE:
                    newType = LDAPModification.REPLACE;
                    break;
                default:
                    throw LDAP.createException(LDAP.PROTOCOL_ERROR);
            }

            LDAPAttribute newAttribute = convertAttribute(attribute);
            list.add(new LDAPModification(newType, newAttribute));
        }

        LDAPModification modifications[] = list.toArray(new LDAPModification[list.size()]);

        LDAPConstraints constraints = new LDAPConstraints();
        constraints.setReferralFollowing(true);

        Collection<LDAPControl> requestControls = convertControls(request.getControls());
        constraints.setControls(requestControls.toArray(new LDAPControl[requestControls.size()]));

        if (debug && !requestControls.isEmpty()) {
            log.debug("Request Controls:");
            for (LDAPControl control : requestControls) {
                log.debug(" - "+control.getID());
            }
        }

        LDAPConnection connection = getConnection();

        try {
            connection.modify(dn, modifications, constraints);

        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        String dn = request.getDn().toString();
        String newRdn = request.getNewRdn().toString();
        boolean deleteOldRdn = request.getDeleteOldRdn();

        if (warn) log.warn("Renaming "+dn+" to "+newRdn+".");

        LDAPConstraints constraints = new LDAPConstraints();
        constraints.setReferralFollowing(true);

        Collection<LDAPControl> requestControls = convertControls(request.getControls());
        constraints.setControls(requestControls.toArray(new LDAPControl[requestControls.size()]));

        if (debug && !requestControls.isEmpty()) {
            log.debug("Request Controls:");
            for (LDAPControl control : requestControls) {
                log.debug(" - "+control.getID());
            }
        }

        LDAPConnection connection = getConnection();

        try {
            connection.rename(dn, newRdn, deleteOldRdn, constraints);

        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        try {
            if (log.isDebugEnabled()) {
                log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
                log.debug(org.safehaus.penrose.util.Formatter.displayLine("LDAP SEARCH", 80));
                log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            }

            String baseDn = request.getDn().toString();
            String filter = request.getFilter() == null ? "(objectClass=*)" : request.getFilter().toString();
            int scope = request.getScope();

            Collection<String> attributes = request.getAttributes();
            String attributeNames[] = attributes.toArray(new String[attributes.size()]);

            long sizeLimit = request.getSizeLimit();
            long timeLimit = request.getTimeLimit();
            boolean typesOnly = request.isTypesOnly();

            if (debug) {
                log.debug("Base       : "+baseDn+".");
                log.debug("Scope      : "+LDAP.getScope(scope));
                log.debug("Filter     : "+filter);
                log.debug("Attributes : "+attributes);
                log.debug("Size Limit : "+sizeLimit);
                log.debug("Time Limit : "+timeLimit);
            }

            LDAPSearchConstraints constraints = new LDAPSearchConstraints();
            constraints.setReferralFollowing(true);
            constraints.setMaxResults((int)sizeLimit);
            constraints.setTimeLimit((int)timeLimit);

            Collection<LDAPControl> requestControls = convertControls(request.getControls());
            constraints.setControls(requestControls.toArray(new LDAPControl[requestControls.size()]));

            if (debug && !requestControls.isEmpty()) {
                log.debug("Request Controls:");
                for (LDAPControl control : requestControls) {
                    log.debug(" - "+control.getID());
                }
            }

            LDAPConnection connection = getConnection();
            LDAPSearchResults rs = connection.search(baseDn, scope, filter, attributeNames, typesOnly, constraints);

            while (rs.hasMore()) {
                if (response.isClosed()) return;
                LDAPEntry entry = rs.next();
                response.add(createSearchResult(entry));
            }


        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;

        } finally {
            response.close();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        LDAPConnection connection = getConnection();

        try {
            connection.bind(3, null, null);

        } catch (Exception e) {
            log.error(e.getMessage());
            response.setException(e);
            throw e;
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Miscelleanous
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public SearchResult createSearchResult(
            LDAPEntry entry
    ) throws Exception {

        String dn = entry.getDN();

        if (debug) log.debug("SearchResult: ["+dn+"]");

        Attributes attributes = new Attributes();

        for (Object object : entry.getAttributeSet()) {
            LDAPAttribute attribute = (LDAPAttribute) object;
            String name = attribute.getName();

            if (binaryAttributes.contains(name.toLowerCase())) {
                for (byte[] value : attribute.getByteValueArray()) {
                    if (debug) log.debug(" - " + name + ": " + BinaryUtil.encode(BinaryUtil.BIG_INTEGER, value, 0, 10)+"...");
                    attributes.addValue(name, value);
                }

            } else {
                for (String value : attribute.getStringValueArray()) {
                    if (debug) log.debug(" - " + name + ": " + value);
                    attributes.addValue(name, value);
                }
            }
        }

        return new SearchResult(dn, attributes);
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

        Schema schema = new Schema();

        getActiveDirectoryAttributeTypes(schema, schemaDn);
        getActiveDirectoryObjectClasses(schema, schemaDn);

        return schema;
    }

    public void getActiveDirectoryAttributeTypes(Schema schema, String schemaDn) throws Exception {

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        LDAPSearchConstraints constraints = new LDAPSearchConstraints();

        LDAPConnection connection = getConnection();
        LDAPSearchResults ne = connection.search(schemaDn, LDAPConnection.SCOPE_ONE, "(objectClass=attributeSchema)", null, false, constraints);

        while (ne.hasMore()) {
            LDAPEntry sr = ne.next();
            LDAPAttributeSet attributes = sr.getAttributeSet();

            LDAPAttribute lDAPDisplayName = attributes.getAttribute("lDAPDisplayName");
            String atName = (String)lDAPDisplayName.getStringValues().nextElement();

            AttributeType at = new AttributeType();
            at.setName(atName);

            LDAPAttribute attributeID = attributes.getAttribute("attributeID");
            if (attributeID != null) at.setOid((String)attributeID.getStringValues().nextElement());

            LDAPAttribute adminDescription = attributes.getAttribute("adminDescription");
            if (adminDescription != null) at.setDescription((String)adminDescription.getStringValues().nextElement());

            LDAPAttribute attributeSyntax = attributes.getAttribute("attributeSyntax");
            if (attributeSyntax != null) at.setSyntax((String)attributeSyntax.getStringValues().nextElement());

            LDAPAttribute isSingleValued = attributes.getAttribute("isSingleValued");
            if (isSingleValued != null) at.setSingleValued(Boolean.valueOf((String)isSingleValued.getStringValues().nextElement()));

            schema.addAttributeType(at);
        }
    }

    public void getActiveDirectoryObjectClasses(Schema schema, String schemaDn) throws Exception {

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        LDAPSearchConstraints constraints = new LDAPSearchConstraints();

        LDAPConnection connection = getConnection();
        LDAPSearchResults ne = connection.search(schemaDn, LDAPConnection.SCOPE_ONE, "(objectClass=classSchema)", null, false, constraints);

        while (ne.hasMore()) {
            LDAPEntry sr = ne.next();
            LDAPAttributeSet attributes = sr.getAttributeSet();

            LDAPAttribute lDAPDisplayName = attributes.getAttribute("lDAPDisplayName");
            String ocName = (String)lDAPDisplayName.getStringValues().nextElement();

            ObjectClass oc = new ObjectClass();
            oc.setName(ocName);

            LDAPAttribute governsID = attributes.getAttribute("governsID");
            if (governsID != null) oc.setOid((String)governsID.getStringValues().nextElement());

            LDAPAttribute adminDescription = attributes.getAttribute("adminDescription");
            if (adminDescription != null) oc.setDescription((String)adminDescription.getStringValues().nextElement());

            LDAPAttribute mustContain = attributes.getAttribute("mustContain");
            if (mustContain != null) {
                Enumeration ne2 = mustContain.getStringValues();
                while (ne2.hasMoreElements()) {
                    String requiredAttribute = (String)ne2.nextElement();
                    oc.addRequiredAttribute(requiredAttribute);
                }
            }

            LDAPAttribute systemMustContain = attributes.getAttribute("systemMustContain");
            if (systemMustContain != null) {
                Enumeration ne2 = systemMustContain.getStringValues();
                while (ne2.hasMoreElements()) {
                    String requiredAttribute = (String)ne2.nextElement();
                    oc.addRequiredAttribute(requiredAttribute);
                }
            }

            LDAPAttribute mayContain = attributes.getAttribute("mayContain");
            if (mayContain != null) {
                Enumeration ne2 = mayContain.getStringValues();
                while (ne2.hasMoreElements()) {
                    String optionalAttribute = (String)ne2.nextElement();
                    oc.addOptionalAttribute(optionalAttribute);
                }
            }

            LDAPAttribute systemMayContain = attributes.getAttribute("systemMayContain");
            if (systemMayContain != null) {
                Enumeration ne2 = systemMayContain.getStringValues();
                while (ne2.hasMoreElements()) {
                    String optionalAttribute = (String)ne2.nextElement();
                    oc.addOptionalAttribute(optionalAttribute);
                }
            }

            schema.addObjectClass(oc);
        }
    }

    public Schema getLDAPSchema(String schemaDn) throws Exception {

        Schema schema = new Schema();

        if (debug) log.debug("Searching "+schemaDn+" ...");

        LDAPSearchConstraints ctls = new LDAPSearchConstraints();

        LDAPConnection connection = getConnection();
        LDAPSearchResults ne = connection.search(
                schemaDn,
                LDAPConnection.SCOPE_BASE,
                "(objectClass=*)",
                new String[] { "attributeTypes", "objectClasses" },
                false,
                ctls
        );

        LDAPEntry sr = ne.next();

        LDAPAttributeSet attributes = sr.getAttributeSet();

        //log.debug("Object Classes:");
        LDAPAttribute objectClasses = attributes.getAttribute("objectClasses");

        Enumeration ne2 = objectClasses.getStringValues();
        while (ne2.hasMoreElements()) {
            String value = (String)ne2.nextElement();
            ObjectClass oc = parseObjectClass(value);
            if (oc == null) continue;

            //log.debug(" - "+oc.getName());
            schema.addObjectClass(oc);
        }

        //log.debug("Attribute Types:");
        LDAPAttribute attributeTypes = attributes.getAttribute("attributeTypes");

        Enumeration ne3 = attributeTypes.getStringValues();
        while (ne3.hasMoreElements()) {
            String value = (String)ne3.nextElement();
            AttributeType at = parseAttributeType(value);
            if (at == null) continue;

            //log.debug(" - "+at.getName());
            schema.addAttributeType(at);
        }

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

    public Collection<LDAPControl> convertControls(Collection<org.safehaus.penrose.control.Control> controls) throws Exception {
        Collection<LDAPControl> list = new ArrayList<LDAPControl>();
        for (org.safehaus.penrose.control.Control control : controls) {

            String oid = control.getOid();
            boolean critical = control.isCritical();
            byte[] value = control.getValue();

            list.add(new LDAPControl(oid, critical, value));
        }

        return list;
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

    public void setRootDSE(SearchResult rootDSE) {
        this.rootDSE = rootDSE;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public LDAPAttributeSet convertAttributes(Attributes attributes) throws Exception {

        LDAPAttributeSet attributeSet = new LDAPAttributeSet();
        for (Attribute attribute : attributes.getAll()) {
            attributeSet.add(convertAttribute(attribute));
        }

        return attributeSet;
    }

    public LDAPAttribute convertAttribute(Attribute attribute) throws Exception {

        String name = attribute.getName();
        LDAPAttribute attr = new LDAPAttribute(name);

        for (Object value : attribute.getValues()) {
            if (value instanceof byte[]) {
                attr.addValue((byte[])value);
            } else {
                attr.addValue(value.toString());
            }
        }

        return attr;
    }

    public LDAPModification[] convertModifications(Collection<Modification> modifications) throws Exception {
        Collection<LDAPModification> list = new ArrayList<LDAPModification>();

        for (Modification modification : modifications) {
            int type = modification.getType();
            Attribute attribute = modification.getAttribute();

            LDAPAttribute attr = convertAttribute(attribute);
            list.add(new LDAPModification(type, attr));
        }

        return list.toArray(new LDAPModification[list.size()]);
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

        client.binaryAttributes = new ArrayList<String>();
        client.binaryAttributes.addAll(binaryAttributes);

        client.rootDSE = rootDSE;
        client.schema = schema;

        client.defaultPageSize = defaultPageSize;

        try {
            if (connection != null) client.connection = (LDAPConnection)connection.clone();

        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        return client;
    }
}
