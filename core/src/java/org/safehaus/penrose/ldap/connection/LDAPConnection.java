package org.safehaus.penrose.ldap.connection;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.partition.Partition;
//import org.springframework.ldap.pool.factory.*;
//import org.springframework.ldap.pool.validation.DefaultDirContextValidator;
//import org.springframework.ldap.core.support.LdapContextSource;

import javax.naming.Context;
//import javax.naming.ldap.LdapContext;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class LDAPConnection extends Connection {

    public final static String BASE_DN        = "baseDn";
    public final static String SCOPE          = "scope";
    public final static String FILTER         = "filter";
    public final static String OBJECT_CLASSES = "objectClasses";
    public final static String SIZE_LIMIT     = "sizeLimit";
    public final static String TIME_LIMIT     = "timeLimit";

    public final static String PAGE_SIZE      = "pageSize";
    public final static int    DEFAULT_PAGE_SIZE = 1000;

    public final static String AUTHENTICATION          = "authentication";
    public final static String AUTHENTICATION_DEFAULT  = "default";
    public final static String AUTHENTICATION_FULL     = "full";
    public final static String AUTHENTICATION_DISABLED = "disabled";
/*
    public String[] BINARY_ATTRIBUTES = new String[] {
        "photo", "personalSignature", "audio", "jpegPhoto", "javaSerializedData",
        "thumbnailPhoto", "thumbnailLogo", "userPassword", "userCertificate",
        "cACertificate", "authorityRevocationList", "certificateRevocationList",
        "crossCertificatePair", "x500UniqueIdentifier"
    };
*/
    public Map<String,String> parameters = new HashMap<String,String>();
    //public Collection<String> binaryAttributes;

    public DN suffix;
    public String url;

    //public PoolingContextSource connectionPool;

    LDAPClient client;
    
    public void init() throws Exception {

        log.debug("Initializing connection "+getName()+".");

        this.parameters.putAll(getParameters());

        this.parameters.put(Context.REFERRAL, "ignore");

        String providerUrl = parameters.get(Context.PROVIDER_URL);

        int index = providerUrl.indexOf("://");
        if (index < 0) throw new Exception("Invalid URL: "+providerUrl);

        index = providerUrl.indexOf("/", index+3);

        if (index >= 0) {
            suffix = new DN(providerUrl.substring(index+1));
            url = providerUrl.substring(0, index);
        } else {
            suffix = new DN();
            url = providerUrl;
        }

        this.parameters.put(Context.PROVIDER_URL, url);
/*
        binaryAttributes = new HashSet<String>();
        for (String name : BINARY_ATTRIBUTES) {
            binaryAttributes.add(name.toLowerCase());
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

        this.parameters.put("com.sun.jndi.ldap.connect.pool", "true");
*/
/*
        LdapContextSource ls = new LdapContextSource();

        //ls.setCacheEnvironmentProperties(false);
        //ls.setBaseEnvironmentProperties(getParameters());

        String initialContextFactory = getParameter(Context.INITIAL_CONTEXT_FACTORY);
        ls.setContextFactory(Class.forName(initialContextFactory));

        ls.setUrl(url);
        ls.setBase(suffix.toString());

        String bindDn = getParameter(Context.SECURITY_PRINCIPAL);
        ls.setUserDn(bindDn);

        String password = getParameter(Context.SECURITY_CREDENTIALS);
        ls.setPassword(password);

        ls.setPooled(false);

        ls.afterPropertiesSet();

        connectionPool = new PoolingContextSource();
        connectionPool.setContextSource(ls);
        connectionPool.setDirContextValidator(new DefaultDirContextValidator());
*/
        client = new LDAPClient(getParameters());

        log.debug("Connection "+getName()+" initialized.");
    }

    public LDAPClient getClient() throws Exception {
        return (LDAPClient)client.clone();
        //LdapContext ldapContext = (LdapContext)connectionPool.getReadWriteContext();
        //return new LDAPClient(ldapContext, getParameters());
    }

    public LDAPClient newClient() throws Exception {
        return new LDAPClient(getParameters());
    }

    public LDAPClient getClient(Session session, Source source) throws Exception {

        String authentication = source.getParameter(AUTHENTICATION);
        if (debug) log.debug("Authentication: "+authentication);

        LDAPClient client;

        if (AUTHENTICATION_FULL.equals(authentication)) {
            if (debug) log.debug("Getting connection info from session.");

            Partition partition = connectionContext.getPartition();
            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+".connection."+getName());

            if (client == null) {

                if (session == null || session.isRootUser()) {
                    if (debug) log.debug("Creating new connection.");

                    client = getClient();
                    //client = new LDAPClient(connection.getParameters());

                } else {
                    if (debug) log.debug("Missing credentials.");
                    throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
                }
            }

        } else {
            if (debug) log.debug("Creating new connection.");

            client = getClient();
            //client = new LDAPClient(connection.getParameters());
        }

        return client;
    }

    public void closeClient(Session session, Source source, LDAPClient client) throws Exception {

        String authentication = source.getParameter(AUTHENTICATION);
        //if (debug) log.debug("Authentication: "+authentication);

        if (!AUTHENTICATION_FULL.equals(authentication)) {
            try { if (client != null) client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Source source,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Bind "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        String authentication = source.getParameter(AUTHENTICATION);
        //if (debug) log.debug("Authentication: "+authentication);

        if (AUTHENTICATION_DISABLED.equals(authentication)) {
            if (debug) log.debug("Pass-Through Authentication is disabled.");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }

        LDAPClient client = getClient(session, source);

        try {
            DNBuilder db = new DNBuilder();
            db.append(request.getDn());

            DN baseDn = new DN(source.getParameter(BASE_DN));
            db.append(baseDn);

            DN dn = db.toDn();

            BindRequest newRequest = new BindRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Binding as "+dn);

            client.bind(newRequest, response);

            log.debug("Bind operation completed.");

        } finally {
            if (AUTHENTICATION_FULL.equals(authentication)) {
                if (debug) log.debug("Storing connection info in session.");

                Partition partition = connectionContext.getPartition();
                if (session != null) session.setAttribute(partition.getName()+".connection."+getName(), client);

            } else {
                try { if (client != null) client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Source source,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Compare "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, source);

        try {
            DNBuilder db = new DNBuilder();
            db.append(request.getDn());

            DN baseDn = new DN(source.getParameter(BASE_DN));
            db.append(baseDn);

            DN dn = db.toDn();

            CompareRequest newRequest = (CompareRequest)request.clone();
            newRequest.setDn(dn);

            if (debug) log.debug("Comparing entry "+dn);

            boolean result = client.compare(newRequest, response);

            log.debug("Compare operation completed ["+result+"].");
            response.setReturnCode(result ? LDAP.COMPARE_TRUE : LDAP.COMPARE_FALSE);

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Source source,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Delete "+ source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, source);

        try {
            DNBuilder db = new DNBuilder();
            db.append(request.getDn());

            DN baseDn = new DN(source.getParameter(BASE_DN));
            db.append(baseDn);

            DN dn = db.toDn();

            DeleteRequest newRequest = new DeleteRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Deleting entry "+dn);

            client.delete(newRequest, response);

            log.debug("Delete operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Source source,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Modify "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, source);

        try {
            DNBuilder db = new DNBuilder();
            db.append(request.getDn());

            DN baseDn = new DN(source.getParameter(BASE_DN));
            db.append(baseDn);

            DN dn = db.toDn();

            ModifyRequest newRequest = new ModifyRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Modifying entry "+dn);

            client.modify(newRequest, response);

            log.debug("Modify operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Source source,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ModRdn "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session, source);

        try {
            DNBuilder db = new DNBuilder();
            db.append(request.getDn());

            DN baseDn = new DN(source.getParameter(BASE_DN));
            db.append(baseDn);

            DN dn = db.toDn();

            ModRdnRequest newRequest = new ModRdnRequest(request);
            newRequest.setDn(dn);

            if (debug) log.debug("Renaming entry "+dn);

            client.modrdn(newRequest, response);

            log.debug("ModRdn operation completed.");

        } finally {
            closeClient(session, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Source source,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+source.getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        final LDAPClient client = getClient(session, source);

        try {
            response.setSizeLimit(request.getSizeLimit());

            DNBuilder db = new DNBuilder();
            db.append(request.getDn());

            final DN baseDn = new DN(source.getParameter(BASE_DN));
            db.append(baseDn);

            DN dn = db.toDn();

            SearchRequest newRequest = (SearchRequest)request.clone();
            newRequest.setDn(dn);

            String scope = source.getParameter(SCOPE);
            if ("OBJECT".equals(scope)) {
                newRequest.setScope(SearchRequest.SCOPE_BASE);

            } else if ("ONELEVEL".equals(scope)) {
                if (request.getDn() == null) {
                    newRequest.setScope(SearchRequest.SCOPE_ONE);
                } else {
                    newRequest.setScope(SearchRequest.SCOPE_BASE);
                }

            } else if ("SUBTREE".equals(scope)) {
                newRequest.setScope(SearchRequest.SCOPE_SUB);
            }

            Filter filter = request.getFilter();

            String s = source.getParameter(FILTER);
            if (s != null) {
                Filter f = FilterTool.parseFilter(s);
                filter = FilterTool.appendAndFilter(filter, f);
                newRequest.setFilter(filter);
            }

            String sizeLimit = source.getParameter(SIZE_LIMIT);
            if (sizeLimit != null) {
                newRequest.setSizeLimit(Long.parseLong(sizeLimit));
            }

            String timeLimit = source.getParameter(TIME_LIMIT);
            if (timeLimit != null) {
                newRequest.setTimeLimit(Long.parseLong(timeLimit));
            }

            SearchResponse newResponse = new SearchResponse() {
                public void add(SearchResult sr) throws Exception {

                    if (response.isClosed()) {
                        close();
                        return;
                    }

                    SearchResult searchResult = createSearchResult(baseDn, source, sr);
                    if (searchResult == null) return;

                    if (debug) {
                        searchResult.print();
                    }

                    response.add(searchResult);
                }
            };

            client.search(newRequest, newResponse);

            log.debug("Search operation completed.");

        } finally {
            response.close();
            closeClient(session, source, client);
        }
    }

    public SearchResult createSearchResult(
            DN baseDn,
            Source source,
            SearchResult sr
    ) throws Exception {

        DN dn = sr.getDn();
        DN newDn = dn.getPrefix(dn.getSize() - baseDn.getSize());
        if (debug) log.debug("Creating search result ["+newDn+"]");

        Attributes attributes = sr.getAttributes();
        Attributes newAttributes;

        if (source.getFields().isEmpty()) {
            newAttributes = (Attributes)attributes.clone();

        } else {
            newAttributes = new Attributes();

            RDN rdn = newDn.getRdn();
            for (String name : rdn.getNames()) {
                Object value = rdn.get(name);
                newAttributes.addValue("primaryKey." + name, value);
            }

            for (Field field : source.getFields()) {

                String fieldName = field.getName();
                String originalName = field.getOriginalName();

                if ("dn".equals(originalName)) {
                    newAttributes.addValue(fieldName, dn.toString());

                } else {
                    Attribute attr = attributes.get(originalName);
                    if (attr == null) {
                        //if (field.isPrimaryKey()) return null;
                        continue;
                    }

                    newAttributes.addValues(fieldName, attr.getValues());
                }
            }
        }

        return new SearchResult(newDn, newAttributes);
    }
}
