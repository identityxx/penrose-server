package org.safehaus.penrose.directory;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterProcessor;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.connection.Connection;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class ProxyEntry extends Entry {

    public final static String BASE_DN                = "baseDn";
    public final static String ATTRIBUTES             = "attributes";

    public final static String AUTHENTICATON          = "authentication";
    public final static String AUTHENTICATON_DEFAULT  = "default";
    public final static String AUTHENTICATON_FULL     = "full";
    public final static String AUTHENTICATON_DISABLED = "disabled";

    Source source;

    DN proxyBaseDn;
    Collection<String> attributeNames = new HashSet<String>();

    String authentication;

    public void init() throws Exception {
        source = partition.getSource();

        String s = entryMapping.getParameter(BASE_DN);
        if (s == null) s = source.getParameter(BASE_DN);

        if (s != null) {
            proxyBaseDn = new DN(s);
            if (debug) log.debug("Proxy Base DN: "+proxyBaseDn);
        }

        String attributes = entryMapping.getParameter(ATTRIBUTES);
        if (attributes != null) {
            StringTokenizer st = new StringTokenizer(attributes, ", ");
            while (st.hasMoreTokens()) {
                String attributeName = st.nextToken();
                attributeNames.add(attributeName.toLowerCase());
            }
            if (debug) log.debug("Attributes: "+attributeNames);
        }

        authentication = entryMapping.getParameter(AUTHENTICATON);
        if (authentication == null) authentication = source.getParameter(AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);
    }

    public DN convertDn(DN dn, DN oldSuffix, DN newSuffix) throws Exception {

        if (dn == null || dn.isEmpty()) return dn;
        if (oldSuffix == null || oldSuffix.isEmpty() || !dn.endsWith(oldSuffix)) return dn;
        if (newSuffix == null || newSuffix.isEmpty()) return dn;

        //if (debug) log.debug("Converting "+dn);

        int start = dn.getSize() - oldSuffix.getSize();

        DNBuilder db = new DNBuilder();
        for (int i=0; i<start; i++) {
            RDN rdn = dn.get(i);
            db.append(rdn);
        }

        db.append(newSuffix);

        return db.toDn();
    }

    public LDAPClient createClient(Session session) throws Exception {

        if (AUTHENTICATON_DISABLED.equals(authentication)) {
            if (debug) log.debug("Pass-Through Authentication is disabled.");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }

        Connection connection = source.getConnection();
        return new LDAPClient(connection.getParameters());
    }

    public void storeClient(Session session, LDAPClient client) throws Exception {

        if (AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Storing connection info in session.");

            Connection connection = source.getConnection();
            if (session != null) session.setAttribute(partition.getName()+".connection."+connection.getName(), client);

        } else {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public LDAPClient getClient(Session session) throws Exception {

        Connection connection = source.getConnection();
        LDAPClient client;

        if (AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Getting connection info from session.");

            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+".connection."+connection.getName());

            if (client == null) {
                if (debug) log.debug("Creating new connection.");
                client = new LDAPClient(connection.getParameters());
            }

        } else {
            if (debug) log.debug("Creating new connection.");
            client = new LDAPClient(connection.getParameters());
        }

        return client;
    }

    public void closeClient(Session session, LDAPClient client) throws Exception {

        if (!AUTHENTICATON_FULL.equals(authentication)) {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("ADD", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session);

        try {
            AddRequest newRequest = (AddRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            for (Attribute attribute : newRequest.getAttributes().getAll()) {
                if (!attributeNames.contains(attribute.getName().toLowerCase())) continue;

                Collection<Object> values = new ArrayList<Object>();
                for (Object value : attribute.getValues()) {
                    DN dn = new DN(value.toString());
                    dn = convertDn(dn, getDn(), proxyBaseDn);
                    values.add(dn.toString());
                }

                attribute.setValues(values);
            }

            client.add(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("BIND", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = createClient(session);

        try {
            BindRequest newRequest = (BindRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            client.bind(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            storeClient(session, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("COMPARE", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = createClient(session);

        try {
            CompareRequest newRequest = (CompareRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            if (attributeNames.contains(newRequest.getAttributeName().toLowerCase())) {
                Object value = newRequest.getAttributeValue();
                DN dn;
                if (value instanceof byte[]) {
                    dn = new DN(new String((byte[])value));
                } else {
                    dn = new DN(value.toString());
                }
                dn = convertDn(dn, getDn(), proxyBaseDn);
                newRequest.setAttributeValue(dn.toString());
            }

            boolean result = client.compare(newRequest, response);

            response.setReturnCode(result ? LDAP.COMPARE_TRUE : LDAP.COMPARE_FALSE);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            storeClient(session, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Find
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Collection<Entry> findEntries(DN dn) throws Exception {

        if (debug) log.debug("Finding matching entries for "+dn+":");

        if (dn == null) return EMPTY_ENTRIES;

        DN thisDn = getDn();
        int level = thisDn.getSize() - 1;
        int length = dn.getSize();

        if (!dn.endsWith(thisDn)) {
            if (debug) log.debug("Doesn't match "+thisDn);
            return EMPTY_ENTRIES;
        }

        if (level < length - 1) { // children has priority
            Collection<Entry> results = new ArrayList<Entry>();
            for (Entry child : children.values()) {
                Collection<Entry> list = child.findEntries(dn, level + 1);
                results.addAll(list);
            }
            if (!results.isEmpty()) return results;
        }

        Collection<Entry> results = new ArrayList<Entry>();
        results.add(this);
        if (debug) log.debug(" - "+getDn());

        return results;
    }

    public Collection<Entry> findEntries(DN dn, int level) throws Exception {

        RDN thisRdn = getRdn();
        int length = dn.getSize();
        RDN rdn = dn.get(length - level - 1);

        if (!thisRdn.matches(rdn)) {
            if (debug) log.debug("Doesn't match with "+getDn());
            return EMPTY_ENTRIES;
        }

        if (level < length - 1) { // children has priority
            Collection<Entry> results = new ArrayList<Entry>();
            for (Entry child : children.values()) {
                Collection<Entry> list = child.findEntries(dn, level + 1);
                results.addAll(list);
            }
            if (!results.isEmpty()) return results;
        }

        Collection<Entry> results = new ArrayList<Entry>();
        results.add(this);
        if (debug) log.debug(" - "+getDn());

        return results;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("DELETE", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session);

        try {
            DeleteRequest newRequest = (DeleteRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            client.delete(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODIFY", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session);

        try {
            ModifyRequest newRequest = (ModifyRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            for (Modification modification : newRequest.getModifications()) {
                Attribute attribute = modification.getAttribute();
                if (!attributeNames.contains(attribute.getName().toLowerCase())) continue;

                Collection<Object> values = new ArrayList<Object>();
                for (Object value : attribute.getValues()) {
                    DN dn = new DN(value.toString());
                    dn = convertDn(dn, getDn(), proxyBaseDn);
                    values.add(dn.toString());
                }
                
                attribute.setValues(values);
            }

            client.modify(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("MODRDN", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session);

        try {
            ModRdnRequest newRequest = (ModRdnRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            client.modrdn(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            Session session,
            Entry base,
            SourceValues sourceValues,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        int scope = request.getScope();

        searchEntry(session, base, sourceValues, request, response);

        if (scope == SearchRequest.SCOPE_ONE) {

            if (base == this) {

                if (debug) log.debug("Searching children of "+entryMapping.getDn()+" ("+children.size()+")");

                for (Entry child : children.values()) {
                    child.search(session, base, sourceValues, request, response);
                }
            }

        } else if (scope == SearchRequest.SCOPE_SUB) {

            if (debug) log.debug("Searching children of "+entryMapping.getDn()+" ("+children.size()+")");

            for (Entry child : children.values()) {
                child.search(session, base, sourceValues, request, response);
            }
        }
    }

    public void searchEntry(
            final Session session,
            final Entry base,
            final SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session);

        try {
            SearchRequest newRequest = (SearchRequest)request.clone();

            if (request.getDn().getSize() >= getDn().getSize()) {
                newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            } else {
                newRequest.setDn(proxyBaseDn);

                if (newRequest.getScope() == SearchRequest.SCOPE_ONE) {
                    newRequest.setScope(SearchRequest.SCOPE_BASE);
                }
            }

            FilterProcessor fp = new FilterProcessor() {
                public void process(Stack<Filter> path, Filter filter) throws Exception {
                    if (!(filter instanceof SimpleFilter)) {
                        super.process(path, filter);
                        return;
                    }

                    SimpleFilter sf = (SimpleFilter)filter;

                    String attribute = sf.getAttribute();
                    if (!attributeNames.contains(attribute.toLowerCase())) return;

                    DN dn = new DN(sf.getValue().toString());
                    dn = convertDn(dn, getDn(), proxyBaseDn);
                    sf.setValue(dn.toString());
                }
            };
            
            fp.process(newRequest.getFilter());

            final Entry enty = this;

            SearchResponse sr = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    DN newDn = convertDn(result.getDn(), proxyBaseDn, getDn());
                    if (debug) log.debug("Entry "+newDn);

                    Attributes newAttributes = (Attributes)result.getAttributes().clone();

                    for (String attributeName : attributeNames) {
                        Attribute attribute = newAttributes.get(attributeName);
                        if (attribute == null) continue;
                        
                        Collection<Object> newValues = new ArrayList<Object>();
                        for (Object value : attribute.getValues()) {
                            DN dn = new DN(value.toString());
                            dn = convertDn(dn, proxyBaseDn, getDn());
                            if (debug) log.debug(" - "+attributeName+": "+dn);
                            newValues.add(dn.toString());
                        }
                        
                        attribute.setValues(newValues);
                    }

                    SearchResult searchResult = new SearchResult(newDn, newAttributes);
                    searchResult.setEntry(enty);
                    response.add(searchResult);
                }
            };

            client.search(newRequest, sr);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
            Session session,
            UnbindRequest request,
            UnbindResponse response
    ) throws Exception {

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("UNBIND", 80));
            log.debug(Formatter.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(Formatter.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }
    }
}
