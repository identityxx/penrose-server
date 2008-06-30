package org.safehaus.penrose.directory;

import org.ietf.ldap.LDAPUrl;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterProcessor;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.connection.LDAPConnection;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.pipeline.Pipeline;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.util.TextUtil;

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
        sourceRef = localSourceRefs.values().iterator().next();
        source = (LDAPSource)sourceRef.getSource();
        connection = (LDAPConnection)source.getConnection();

        String s = entryConfig.getParameter(BASE_DN);
        if (s == null) s = source.getParameter(BASE_DN);

        if (s != null) {
            proxyBaseDn = new DN(s);
            if (debug) log.debug("Proxy Base DN: "+proxyBaseDn);
        }

        s = entryConfig.getParameter(FILTER);
        if (s == null) s = source.getParameter(FILTER);

        if (s != null) {
            proxyFilter = FilterTool.parseFilter(s);
            if (debug) log.debug("Proxy Filter: "+proxyFilter);
        }

        s = entryConfig.getParameter(SCOPE);
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

        String attributes = entryConfig.getParameter(ATTRIBUTES);
        if (attributes != null) {
            StringTokenizer st = new StringTokenizer(attributes, ", ");
            while (st.hasMoreTokens()) {
                String attributeName = st.nextToken();
                attributeNames.add(attributeName.toLowerCase());
            }
            if (debug) log.debug("Attributes: "+attributeNames);
        }

        s = entryConfig.getParameter(SIZE_LIMIT);
        if (s == null) s = source.getParameter(SIZE_LIMIT);

        if (s != null) {
            proxySizeLimit = Long.parseLong(s);
            if (debug) log.debug("Proxy Size Limit: "+proxySizeLimit);
        }

        s = entryConfig.getParameter(TIME_LIMIT);
        if (s == null) s = source.getParameter(TIME_LIMIT);

        if (s != null) {
            proxyTimeLimit = Long.parseLong(s);
            if (debug) log.debug("Proxy Time Limit: "+proxyTimeLimit);
        }

        authentication = entryConfig.getParameter(AUTHENTICATON);
        if (authentication == null) authentication = source.getParameter(AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);
    }

    public DN convertDn(DN dn, DN oldSuffix, DN newSuffix) throws Exception {

        if (debug) {
            log.debug("Converting "+dn+":");
            log.debug(" - old suffix: "+oldSuffix);
            log.debug(" - new suffix: "+newSuffix);
        }

        if (dn == null || dn.isEmpty()) return dn;
        if (oldSuffix == null || oldSuffix.isEmpty() || !dn.endsWith(oldSuffix)) return dn;
        if (newSuffix == null || newSuffix.isEmpty()) return dn;

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
    // Scope
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateScope(SearchRequest request) throws Exception {
        // ignore
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Filter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean validateFilter(Attributes attributes, Filter filter) throws Exception {
        return true;
    }

    public void validateFilter(Filter filter) throws Exception {
        // ignore
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PROXY ADD", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

        LDAPClient client = connection.getClient(session);

        try {
            AddRequest newRequest = (AddRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            for (Attribute attribute : newRequest.getAttributes().getAll()) {
                if (!attributeNames.contains(attribute.getName().toLowerCase())) continue;

                Collection<Object> values = new ArrayList<Object>();
                for (Object value : attribute.getValues()) {
                    DN newDn = new DN(value.toString());
                    newDn = convertDn(newDn, getDn(), proxyBaseDn);
                    values.add(newDn.toString());
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
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PROXY BIND", 80));
            log.debug(TextUtil.displayLine("DN : "+request.getDn(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

        try {
            BindRequest newRequest = (BindRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            client.bind(newRequest, response);

        } finally {
            connection.closeClient(session);
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

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PROXY COMPARE", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

        LDAPClient client = connection.getClient(session);

        try {
            CompareRequest newRequest = (CompareRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            if (attributeNames.contains(newRequest.getAttributeName().toLowerCase())) {
                Object value = newRequest.getAttributeValue();
                DN newDn;
                if (value instanceof byte[]) {
                    newDn = new DN(new String((byte[])value));
                } else {
                    newDn = new DN(value.toString());
                }
                newDn = convertDn(newDn, getDn(), proxyBaseDn);
                newRequest.setAttributeValue(newDn.toString());
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

    public boolean contains(DN dn) throws Exception {

        DN entryDn = getDn();

        if (entryDn.getSize() == dn.getSize()) {
            return entryDn.matches(dn);

        } else if (entryDn.getSize() < dn.getSize()) {
            return entryDn.endsWith(dn);
        }

        return false;
    }

    public Collection<Entry> findEntries(DN dn) throws Exception {

        if (dn == null) return EMPTY_ENTRIES;

        DN entryDn        = getDn();
        if (debug) log.debug("Finding matching entries for \""+dn+"\" in \""+entryDn+"\".");

        int entryDnLength = entryDn.getSize();
        int dnLength      = dn.getSize();

        if (dnLength == 0 && entryDnLength == 0) { // Root DSE
            Collection<Entry> results = new ArrayList<Entry>();
            results.add(this);
            return results;
        }

        if (!dn.endsWith(entryDn)) {
            //if (debug) log.debug("Doesn't match "+entryDn);
            return EMPTY_ENTRIES;
        }

        if (debug) log.debug("Searching children of \""+entryDn+"\".");

        Collection<Entry> results = new ArrayList<Entry>();

        if (dnLength > entryDnLength) { // children has priority
            for (Entry child : getChildren()) {
                Collection<Entry> list = child.findEntries(dn);
                results.addAll(list);
            }
            if (!results.isEmpty()) return results;
        }

        results.add(this);

        if (debug) log.debug("Found entry \""+entryDn+"\".");

        return results;
    }
/*
    public Collection<Entry> findEntries(DN dn, int level) throws Exception {

        if (debug) log.debug("Finding matching entries for "+dn+":");

        if (dn == null) return EMPTY_ENTRIES;

        DN entryDn        = getDn();

        int entryDnLength = entryDn.getSize();
        int dnLength      = dn.getSize();

        RDN entryRdn      = getRdn();
        RDN rdn           = dn.get(dnLength - entryDnLength - 1);

        if (!entryRdn.matches(rdn)) {
            if (debug) log.debug("Doesn't match with "+entryDn);
            return EMPTY_ENTRIES;
        }

        Collection<Entry> results = new ArrayList<Entry>();

        if (dnLength > entryDnLength) { // children has priority
            for (Entry child : getChildren()) {
                Collection<Entry> list = child.findEntries(dn, entryDnLength);
                results.addAll(list);
            }
            if (!results.isEmpty()) return results;
        }


        results.add(this);

        if (debug) log.debug("Found entry "+entryDn);

        return results;
    }
*/
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PROXY DELETE", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

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

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PROXY MODIFY", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

        LDAPClient client = connection.getClient(session);

        try {
            ModifyRequest newRequest = (ModifyRequest)request.clone();
            newRequest.setDn(convertDn(newRequest.getDn(), getDn(), proxyBaseDn));

            for (Modification modification : newRequest.getModifications()) {
                Attribute attribute = modification.getAttribute();
                if (!attributeNames.contains(attribute.getName().toLowerCase())) continue;

                Collection<Object> values = new ArrayList<Object>();
                for (Object value : attribute.getValues()) {
                    DN newDn = new DN(value.toString());
                    newDn = convertDn(newDn, getDn(), proxyBaseDn);
                    values.add(newDn.toString());
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

        DN dn = request.getDn();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PROXY MODRDN", 80));
            log.debug(TextUtil.displayLine("Entry : "+getDn(), 80));
            log.debug(TextUtil.displayLine("DN    : "+dn, 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        validatePermission(session, request);

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
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        final DN baseDn     = request.getDn();
        final Filter filter = request.getFilter();
        final int scope     = request.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("PROXY SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        response = createSearchResponse(session, request, response);

        try {
            validateScope(request);
            validatePermission(session, request);
            validateFilter(filter);

        } catch (Exception e) {
            response.close();
            return;
        }

        try {
            generateSearchResults(session, request, response);

        } finally {
            response.close();
        }
    }

    public SearchResponse createSearchResponse(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) {
        return response;
    }

    public void generateSearchResults(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        final DN baseDn     = request.getDn();
        final Filter filter = request.getFilter();
        final int scope     = request.getScope();

        LDAPClient client = connection.getClient(session);

        boolean subset = baseDn.endsWith(getDn());

        try {
            SearchRequest newRequest = (SearchRequest)request.clone();

            if (subset) {
                newRequest.setDn(convertDn(baseDn, getDn(), proxyBaseDn));

                if (proxyScope == SearchRequest.SCOPE_BASE) {
                    newRequest.setScope(SearchRequest.SCOPE_BASE);

                } else if (proxyScope == SearchRequest.SCOPE_ONE && scope == SearchRequest.SCOPE_SUB) {
                    newRequest.setScope(SearchRequest.SCOPE_ONE);
                }

            } else {
                newRequest.setDn(proxyBaseDn);

                if (scope == SearchRequest.SCOPE_ONE) {
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

            fp.process(filter);

            if (proxyFilter != null) {
                newRequest.setFilter(FilterTool.appendAndFilter(proxyFilter, filter));
            }

            if (proxySizeLimit > 0 && request.getSizeLimit() > proxySizeLimit) {
                newRequest.setSizeLimit(proxySizeLimit);
            }

            if (proxyTimeLimit > 0 && request.getTimeLimit() > proxyTimeLimit) {
                newRequest.setTimeLimit(proxyTimeLimit);
            }

            final Interpreter interpreter = partition.newInterpreter();

            SearchResponse newResponse = new Pipeline(response) {
                public void add(SearchResult result) throws Exception {
                    SearchResult newResult = createSearchResult(interpreter, result);
                    newResult.setEntry(ProxyEntry.this);
                    super.add(newResult);
                }

                public void add(SearchReference reference) throws Exception {
                    SearchReference newReference = createSearchReference(reference);
                    super.add(newReference);
                }
            };

            client.search(newRequest, newResponse);

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

    public SearchReference createSearchReference(SearchReference reference) throws Exception {

        DN dn = reference.getDn();

        DN newDn = convertDn(dn, proxyBaseDn, getDn());
        if (debug) log.debug("Reference "+newDn);

        Collection<String> urls = new ArrayList<String>();

        for (String url : reference.getUrls()) {
            LDAPUrl ldapUrl = new LDAPUrl(url);
            DN dnValue = new DN(ldapUrl.getDN());

            dnValue = convertDn(dnValue, proxyBaseDn, getDn());
            urls.add(dnValue.toString());
        }

        return new SearchReference(newDn, urls);
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
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("UNBIND", 80));
            log.debug(TextUtil.displayLine("Entry DN    : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Entry Class : "+getClass().getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

        try {
            UnbindRequest newRequest = (UnbindRequest)request.clone();

            client.unbind(newRequest, response);

        } finally {
            connection.closeClient(session);
        }
    }
}
