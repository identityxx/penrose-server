package org.safehaus.penrose.directory;

import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterProcessor;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.connection.LDAPConnection;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionListener;
import org.safehaus.penrose.util.Formatter;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class ProxyEntry extends Entry {

    public final static String BASE_DN                = "baseDn";
    public final static String FILTER                 = "filter";
    public final static String SCOPE                  = "scope";
    public final static String ATTRIBUTES             = "attributes";

    public final static String SIZE_LIMIT             = "sizeLimit";
    public final static String TIME_LIMIT             = "timeLimit";

    public final static String AUTHENTICATON          = "authentication";
    public final static String AUTHENTICATON_DEFAULT  = "default";
    public final static String AUTHENTICATON_FULL     = "full";
    public final static String AUTHENTICATON_DISABLED = "disabled";

    SourceRef sourceRef;
    LDAPSource source;
    LDAPConnection connection;

    DN proxyBaseDn;
    Filter proxyFilter;
    int proxyScope = SearchRequest.SCOPE_SUB;
    long proxySizeLimit;
    long proxyTimeLimit;

    Collection<String> attributeNames = new HashSet<String>();

    String authentication;

    public void init() throws Exception {
        sourceRef = localPrimarySourceRefs.values().iterator().next();
        source = (LDAPSource)sourceRef.getSource();
        connection = (LDAPConnection)source.getConnection();

        String s = entryMapping.getParameter(BASE_DN);
        if (s == null) s = source.getParameter(BASE_DN);

        if (s != null) {
            proxyBaseDn = new DN(s);
            if (debug) log.debug("Proxy Base DN: "+proxyBaseDn);
        }

        s = entryMapping.getParameter(FILTER);
        if (s == null) s = source.getParameter(FILTER);

        if (s != null) {
            proxyFilter = FilterTool.parseFilter(s);
            if (debug) log.debug("Proxy Filter: "+proxyFilter);
        }

        s = entryMapping.getParameter(SCOPE);
        if (s == null) s = source.getParameter(SCOPE);

        if (s != null) {
            if ("OBJECT".equals(s)) {
                proxyScope = SearchRequest.SCOPE_BASE;
                
            } else if ("ONELEVEL".equals(s)) {
                proxyScope = SearchRequest.SCOPE_ONE;

            } else if ("SUBTREE".equals(s)) {
                proxyScope = SearchRequest.SCOPE_SUB;
            }

            if (debug) log.debug("Proxy Scope: "+proxyScope);
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

        s = entryMapping.getParameter(SIZE_LIMIT);
        if (s == null) s = source.getParameter(SIZE_LIMIT);

        if (s != null) {
            proxySizeLimit = Long.parseLong(s);
            if (debug) log.debug("Proxy Size Limit: "+proxySizeLimit);
        }

        s = entryMapping.getParameter(TIME_LIMIT);
        if (s == null) s = source.getParameter(TIME_LIMIT);

        if (s != null) {
            proxyTimeLimit = Long.parseLong(s);
            if (debug) log.debug("Proxy Time Limit: "+proxyTimeLimit);
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
            log.debug(Formatter.displayLine("DN : "+request.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

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

        } finally {
            connection.closeClient(session);
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
            log.debug(Formatter.displayLine("DN : "+request.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

        try {
            BindRequest newRequest = (BindRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            client.bind(newRequest, response);

        } finally {
            //storeClient(session, client);
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
            log.debug(Formatter.displayLine("DN : "+request.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

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

        } finally {
            connection.closeClient(session);
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
            for (Entry child : children) {
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
            for (Entry child : children) {
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
            log.debug(Formatter.displayLine("DN : "+request.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

        try {
            DeleteRequest newRequest = (DeleteRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            client.delete(newRequest, response);

        } finally {
            connection.closeClient(session);
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
            log.debug(Formatter.displayLine("DN : "+request.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

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

        } finally {
            connection.closeClient(session);
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
            log.debug(Formatter.displayLine("DN : "+request.getDn(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

        try {
            ModRdnRequest newRequest = (ModRdnRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            client.modrdn(newRequest, response);

        } finally {
            connection.closeClient(session);
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

                for (Entry child : children) {
                    child.search(session, base, sourceValues, request, response);
                }
            }

        } else if (scope == SearchRequest.SCOPE_SUB) {

            if (debug) log.debug("Searching children of "+entryMapping.getDn()+" ("+children.size()+")");

            for (Entry child : children) {
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

        final DN dn = request.getDn();
        final Filter filter = request.getFilter();
        final int scope = request.getScope();

        if (debug) {
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("PROXY SEARCH", 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Entry  : "+getDn(), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Base   : "+dn, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Filter : "+filter, 80));
            log.debug(org.safehaus.penrose.util.Formatter.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(org.safehaus.penrose.util.Formatter.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

        SearchRequest newRequest = (SearchRequest)request.clone();
        boolean subset = newRequest.getDn().getSize() >= getDn().getSize();

        try {
            if (subset) {
                newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

                if (proxyScope == SearchRequest.SCOPE_BASE) {
                    newRequest.setScope(SearchRequest.SCOPE_BASE);

                } else if (proxyScope == SearchRequest.SCOPE_ONE && newRequest.getScope() == SearchRequest.SCOPE_SUB) {
                    newRequest.setScope(SearchRequest.SCOPE_ONE);
                }
                
            } else {
                newRequest.setDn(proxyBaseDn);

                if (newRequest.getScope() == SearchRequest.SCOPE_ONE) {
                    newRequest.setScope(SearchRequest.SCOPE_BASE);
                }
            }

            FilterProcessor fp = new FilterProcessor() {
                public Filter process(Stack<Filter> path, Filter filter) throws Exception {
                    if (!(filter instanceof SimpleFilter)) {
                        return super.process(path, filter);
                    }

                    SimpleFilter sf = (SimpleFilter)filter;

                    String attribute = sf.getAttribute();
                    if (!attributeNames.contains(attribute.toLowerCase())) return filter;

                    DN dn = new DN(sf.getValue().toString());
                    dn = convertDn(dn, getDn(), proxyBaseDn);
                    sf.setValue(dn.toString());

                    return filter;
                }
            };

            fp.process(newRequest.getFilter());

            if (proxyFilter != null) {
                newRequest.setFilter(FilterTool.appendAndFilter(proxyFilter, newRequest.getFilter()));
            }

            if (proxySizeLimit > 0 && newRequest.getSizeLimit() > proxySizeLimit) {
                newRequest.setSizeLimit(proxySizeLimit);
            }

            if (proxyTimeLimit > 0 && newRequest.getTimeLimit() > proxyTimeLimit) {
                newRequest.setTimeLimit(proxyTimeLimit);
            }

            final Interpreter interpreter = partition.newInterpreter();

            SearchResponse sr = new SearchResponse() {
                public void add(SearchResult result) throws Exception {

                    SearchResult searchResult = createSearchResult(interpreter, result);
                    searchResult.setEntry(ProxyEntry.this);
                    response.add(searchResult);
                }
            };

            client.search(newRequest, sr);

        } catch (Exception e) {
            if (subset) throw e;
            
        } finally {
            connection.closeClient(session);
        }
    }

    public SearchResult createSearchResult(Interpreter interpreter, SearchResult result) throws Exception {

        DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        SourceValues sv = result.getSourceValues();
        sv.set(sourceRef.getAlias(), attributes);
        
        if (debug) {
            log.debug("Source values:");
            sv.print();
        }
        
        interpreter.set(sv);

        DN newDn = convertDn(dn, proxyBaseDn, getDn());
        if (debug) log.debug("Entry "+newDn);

        Attributes newAttributes = (Attributes)attributes.clone();

        for (String attributeName : attributeNames) {
            Attribute attribute = newAttributes.get(attributeName);
            if (attribute == null) continue;

            Collection<Object> newValues = new ArrayList<Object>();
            for (Object value : attribute.getValues()) {
                DN dnValue = new DN(value.toString());
                dnValue = convertDn(dnValue, proxyBaseDn, getDn());
                if (debug) log.debug(" - "+attributeName+": "+dnValue);
                newValues.add(dnValue.toString());
            }

            attribute.setValues(newValues);
        }

        for (AttributeMapping attributeMapping : getAttributeMappings()) {
            String attributeName = attributeMapping.getName();
            if (debug) log.debug("Transforming attribute "+attributeName+":");
            Object object = interpreter.eval(attributeMapping);

            if (object == null) {
                if (debug) log.debug("Attribute "+attributeName+" removed.");
                newAttributes.remove(attributeName);

            } else if (object instanceof Collection) {
                Collection<Object> values = (Collection<Object>)object;
                for (Object value : values) {
                    if (debug) log.debug(" - "+value);
                }
                newAttributes.setValues(attributeMapping.getName(), values);

            } else {
                if (debug) log.debug(" - "+object);
                newAttributes.setValue(attributeMapping.getName(), object);
            }
        }

        interpreter.clear();

        return new SearchResult(newDn, newAttributes);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
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
