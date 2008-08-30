package org.safehaus.penrose.ldap.source;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.util.TextUtil;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class ADGroupSource extends LDAPSource {

    public final static String SAM_ACCOUNT_NAME = "sAMAccountName";
    public final static String CN               = "cn";
    public final static String MEMBER           = "member";
    public final static String MEMBER_OF        = "memberOf";

    public Field cnField;
    public Field memberField;

    public void init() throws Exception {
        super.init();
        
        cnField = getFieldByOriginalName(CN);
        memberField = getFieldByOriginalName(MEMBER);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        if (debug) {
            log.debug(TextUtil.displaySeparator(80));
            log.debug(TextUtil.displayLine("AD Group Search "+getName(), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        LDAPClient client = connection.getClient(session);

        try {
            response.setSizeLimit(request.getSizeLimit());

            ADGroupFilterProcessor fp = new ADGroupFilterProcessor(this);
            List<Filter> filters = new ArrayList<Filter>();

            createFilter(session, request, response, filters, fp, client);

            if (filters.isEmpty()) {
                log.debug("No user found.");
                return;
            }

            Map<String,SearchResult> results = new LinkedHashMap<String,SearchResult>();

            for (int i = 0; i < filters.size(); i++) {
                Filter f = filters.get(i);

                f = FilterTool.appendAndFilter(f, filter);

                searchEntries(session, request, response, f, filters, results, fp, client);
            }

            log.debug("Search operation completed.");

        } finally {
            response.close();
            connection.closeClient(session);
        }
    }

    public void createFilter(
            final Session session,
            final SearchRequest request,
            final SearchResponse response,
            final List<Filter> filters,
            final ADGroupFilterProcessor fp,
            final LDAPClient client
    ) throws Exception {

        DN baseDn = request.getDn();
        int scope = request.getScope();
        Filter filter = request.getFilter();

        Filter newFilter = null;

        if (scope == SearchRequest.SCOPE_BASE) {
            RDN rdn = baseDn.getRdn();
            for (String name : rdn.getNames()) {
                Object value = rdn.get(name);
                newFilter = FilterTool.appendAndFilter(newFilter, new SimpleFilter(name, "=", value));
            }
        }

        fp.process(filter);

        SimpleFilter cnFilter = fp.getCnFilter();

        if (cnFilter != null) {
            cnFilter = new SimpleFilter(cnField.getOriginalName(), cnFilter.getOperator(), cnFilter.getValue());
        }

        SimpleFilter memberFilter = fp.getMemberFilter();

        if (memberFilter == null) {
            newFilter = FilterTool.appendAndFilter(newFilter, cnFilter);

        } else {
            Object value = memberFilter.getValue();
            SearchResult result = findUser(client, value);

            if (result == null) {
                log.debug("User "+value+" not found.");
                return;
            }

            DN dn = result.getDn();

            String userDn = dn.toString();

            Filter f = new SimpleFilter(memberField.getOriginalName(), memberFilter.getOperator(), userDn);
            newFilter = FilterTool.appendAndFilter(newFilter, f);
        }

        filters.add(newFilter);
    }

    public SearchResult findUser(LDAPClient client, Object sAMAccountName) throws Exception {

        SearchRequest newRequest = new SearchRequest();
        newRequest.setDn(baseDn);
        newRequest.setFilter(new SimpleFilter(SAM_ACCOUNT_NAME, "=", sAMAccountName));
        newRequest.setSizeLimit(sourceSizeLimit);
        newRequest.setTimeLimit(sourceTimeLimit);

        SearchResponse newResponse = new SearchResponse();

        client.search(newRequest, newResponse);

        if (!newResponse.hasNext()) return null;

        return newResponse.next();
    }

    public SearchResult findGroup(LDAPClient client, Object cn) throws Exception {

        SearchRequest newRequest = new SearchRequest();
        newRequest.setDn(baseDn);
        newRequest.setScope(scope);

        Filter newFilter = new SimpleFilter(CN, "=", cn);
        newFilter = FilterTool.appendAndFilter(newFilter, filter);

        newRequest.setFilter(newFilter);
        newRequest.setSizeLimit(sourceSizeLimit);
        newRequest.setTimeLimit(sourceTimeLimit);

        SearchResponse newResponse = new SearchResponse();

        client.search(newRequest, newResponse);

        if (!newResponse.hasNext()) return null;

        return newResponse.next();
    }

    public void searchEntries(
            final Session session,
            final SearchRequest request,
            final SearchResponse response,
            final Filter filter,
            final List<Filter> filters,
            final Map<String,SearchResult> map,
            final ADGroupFilterProcessor fp,
            final LDAPClient client
    ) throws Exception {

        SearchRequest newRequest = new SearchRequest();
        newRequest.setDn(baseDn);
        newRequest.setScope(scope);
        newRequest.setFilter(filter);
        newRequest.setSizeLimit(sourceSizeLimit);
        newRequest.setTimeLimit(sourceTimeLimit);

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult searchResult) throws Exception {

                if (response.isClosed()) {
                    if (debug) log.debug("Search response has been closed.");
                    close();
                    return;
                }

                SearchResult newSearchResult = createSearchResult(baseDn, searchResult);

                DN dn = newSearchResult.getDn().append(baseDn);
                String groupDn = dn.getNormalizedDn();

                Collection<Object> users = searchMembers(baseDn, searchResult, map, client);

                Attributes attributes = newSearchResult.getAttributes();
                attributes.setValues(memberField.getName(), users);

                if (!map.containsKey(groupDn)) {
                    map.put(groupDn, newSearchResult);

                    SimpleFilter memberFilter = fp.getMemberFilter();
                    if (memberFilter != null) filters.add(new SimpleFilter(MEMBER, "=", groupDn));
                    
                    response.add(newSearchResult);

                    if (debug) {
                        newSearchResult.print();
                    }
                }
            }
        };

        client.search(newRequest, newResponse);
    }

    public Collection<Object> searchMembers(
            DN baseDn,
            SearchResult searchResult,
            Map<String,SearchResult> results,
            LDAPClient client
    ) throws Exception {

        Collection<Object> users = new LinkedHashSet<Object>();

        List<String> groups = new ArrayList<String>();
        groups.add(searchResult.getDn().getNormalizedDn());

        for (int i = 0; i < groups.size(); i++) {
            String dn = groups.get(i);

            SearchResult sr = results.get(dn);
            if (sr == null) {
                log.debug("Searching for members of group "+dn+".");

                Collection<Object> list = expandMembers(dn, baseDn, client, groups);
                users.addAll(list);

            } else {
                log.debug("Getting members of group "+dn+" from cache.");

                Attributes attributes = sr.getAttributes();
                users.addAll(attributes.getValues(memberField.getName()));
            }
        }

        return users;
    }

    public Collection<Object> expandMembers(
            String dn,
            DN baseDn,
            LDAPClient client,
            List<String> groups
    ) throws Exception {

        Collection<Object> users = new LinkedHashSet<Object>();

        SearchRequest req1 = new SearchRequest();
        req1.setDn(baseDn);
        req1.setFilter(new SimpleFilter(MEMBER_OF, "=", dn));
        req1.setAttributes(new String[] { "objectClass", SAM_ACCOUNT_NAME, MEMBER });

        SearchResponse res1 = new SearchResponse();

        log.debug("Filter: "+req1.getFilter());

        client.search(req1, res1);

        while (res1.hasNext()) {
            SearchResult result = res1.next();
            Attributes attributes = result.getAttributes();
            Attribute objectClass = attributes.get("objectClass");

            if (objectClass.containsValue("user")) {
                Attribute sAMAccountName = attributes.get(SAM_ACCOUNT_NAME);
                String userUid = (String)sAMAccountName.getValue();
                users.add(userUid);
                log.debug(" - Found user "+userUid);

            } else if (objectClass.containsValue("group")) {
                String groupDn = result.getDn().getNormalizedDn();
                if (!groups.contains(groupDn)) {
                    groups.add(groupDn);
                    log.debug(" - Found group "+groupDn);
                }
            }
        }

        return users;
    }

    public SearchResult createSearchResult(
            DN baseDn,
            SearchResult sr
    ) throws Exception {

        DN dn = sr.getDn();
        DN newDn = dn.getPrefix(baseDn);
        if (debug) log.debug("Creating search result ["+newDn+"]");

        Attributes attributes = sr.getAttributes();
        Attributes newAttributes = new Attributes();

        RDN rdn = newDn.getRdn();
        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);
            newAttributes.addValue("primaryKey." + name, value);
        }

        for (Field field : getFields()) {

            String fieldName = field.getName();
            String originalName = field.getOriginalName();

            if ("dn".equals(originalName)) {
                newAttributes.addValue(fieldName, dn.toString());

            } else {
                Attribute attr = attributes.remove(originalName);
                if (attr == null) continue;

                newAttributes.addValues(fieldName, attr.getValues());
            }
        }

        return new SearchResult(newDn, newAttributes);
    }
}
