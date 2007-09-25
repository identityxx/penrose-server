package org.safehaus.penrose.directory;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterProcessor;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.mapping.SourceMapping;
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

    SourceMapping sourceMapping;
    Source source;

    DN proxyBaseDn;
    Collection<String> attributeNames = new HashSet<String>();

    public void init() throws Exception {
        sourceMapping = getSourceMapping(0);
        source = partition.getSource(sourceMapping.getSourceName());

        String baseDn = entryMapping.getParameter(BASE_DN);
        if (baseDn == null) baseDn = source.getParameter(BASE_DN);

        if (baseDn != null) {
            proxyBaseDn = new DN(baseDn);
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

        DN newDn = db.toDn();
        //if (debug) log.debug("into "+newDn);

        return newDn;
    }

    public LDAPClient createClient(Session session) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        if (AUTHENTICATON_DISABLED.equals(authentication)) {
            if (debug) log.debug("Pass-Through Authentication is disabled.");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }

        String connectionName = source.getConnectionName();
        Connection connection = partition.getConnection(connectionName);

        return new LDAPClient(connection.getParameters());
    }

    public void storeClient(Session session, LDAPClient client) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        if (AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Storing connection info in session.");

            String connectionName = source.getConnectionName();
            Connection connection = partition.getConnection(connectionName);

            if (session != null) session.setAttribute(partition.getName()+".connection."+connection.getName(), client);
        } else {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public LDAPClient getClient(Session session) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        Connection connection = partition.getConnection(source.getConnectionName());
        LDAPClient client;

        if (AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Getting connection info from session.");

            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+".connection."+connection.getName());

            if (client == null) {

                if (session == null || session.isRootUser()) {
                    if (debug) log.debug("Creating new connection.");

                    client = new LDAPClient(connection.getParameters());

                } else {
                    if (debug) log.debug("Missing credentials.");
                    throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
                }
            }

        } else {
            if (debug) log.debug("Creating new connection.");

            client = new LDAPClient(connection.getParameters());
        }

        return client;
    }

    public void closeClient(Session session, LDAPClient client) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        if (!AUTHENTICATON_FULL.equals(authentication)) {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
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
            newRequest.setDn(convertDn(request.getDn(), getDn(), proxyBaseDn));

            client.bind(newRequest, response);

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

        Collection<Entry> results = new ArrayList<Entry>();
        if (dn == null) return results;

        if (dn.endsWith(getDn())) {
            results.add(this);
            return results;
        }

        for (Entry child : children.values()) {
            Collection<Entry> list = child.findEntries(dn);
            results.addAll(list);
        }

        return results;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
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
                newRequest.setDn(convertDn(request.getDn(), getDn(), proxyBaseDn));

            } else {
                newRequest.setDn(proxyBaseDn);

                if (request.getScope() == SearchRequest.SCOPE_ONE) {
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

            //newRequest.addControl(new Control("2.16.840.1.113730.3.4.2", null, true));

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
            response.close();
            closeClient(session, client);
        }
    }
}
