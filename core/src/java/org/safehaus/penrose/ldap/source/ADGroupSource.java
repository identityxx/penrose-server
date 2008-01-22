package org.safehaus.penrose.ldap.source;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.util.Formatter;

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
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("AD Search "+getName(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        LDAPClient client = getClient(session);

        try {
            response.setSizeLimit(request.getSizeLimit());

            List<Filter> filters = new ArrayList<Filter>();

            createFilter(session, request, response, filters, client);

            if (filters.isEmpty()) {
                log.debug("No user found.");
                return;
            }

            Collection<String> results = new LinkedHashSet<String>();

            for (int i = 0; i < filters.size(); i++) {
                Filter f = filters.get(i);

                f = FilterTool.appendAndFilter(f, sourceFilter);

                searchEntries(session, request, response, f, filters, results, client);
            }

            log.debug("Search operation completed.");

        } finally {
            response.close();
            closeClient(session);
        }
    }

    public void createFilter(
            final Session session,
            final SearchRequest request,
            final SearchResponse response,
            final List<Filter> filters,
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

        ADGroupFilterProcessor fp = new ADGroupFilterProcessor(this);
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
            filters.add(f);

            newFilter = FilterTool.appendAndFilter(newFilter, f);
        }

        filters.add(newFilter);
    }

    public SearchResult findUser(LDAPClient client, Object sAMAccountName) throws Exception {

        SearchRequest newRequest = new SearchRequest();
        newRequest.setDn(sourceBaseDn);
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
        newRequest.setDn(sourceBaseDn);
        newRequest.setScope(sourceScope);

        Filter filter = new SimpleFilter(CN, "=", cn);
        filter = FilterTool.appendAndFilter(filter, sourceFilter);

        newRequest.setFilter(filter);
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
            final Collection<String> results,
            final LDAPClient client
    ) throws Exception {

        SearchRequest newRequest = new SearchRequest();
        newRequest.setDn(sourceBaseDn);
        newRequest.setScope(sourceScope);
        newRequest.setFilter(filter);
        newRequest.setSizeLimit(sourceSizeLimit);
        newRequest.setTimeLimit(sourceTimeLimit);

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult searchResult) throws Exception {

                if (response.isClosed()) {
                    close();
                    return;
                }

                SearchResult newSearchResult = createSearchResult(sourceBaseDn, searchResult);
                if (newSearchResult == null) return;

                searchMembers(sourceBaseDn, searchResult, newSearchResult, client);

                if (debug) {
                    newSearchResult.print();
                }

                DN dn = newSearchResult.getDn().append(sourceBaseDn);
                String groupDn = dn.getNormalizedDn();
                if (!results.contains(groupDn)) {
                    results.add(groupDn);
                    filters.add(new SimpleFilter(MEMBER, "=", groupDn));
                    response.add(newSearchResult);
                }
            }
        };

        client.search(newRequest, newResponse);
    }

    public void searchMembers(DN baseDn, SearchResult sr, SearchResult searchResult, LDAPClient client) throws Exception {

        Collection<Object> users = new LinkedHashSet<Object>();

        List<String> groups = new ArrayList<String>();
        groups.add(sr.getDn().getNormalizedDn());

        for (int i = 0; i < groups.size(); i++) {
            String dn = groups.get(i);
            expandMembers(dn, baseDn, client, users, groups);
        }

        Attributes attributes = searchResult.getAttributes();
        attributes.setValues(memberField.getName(), users);
    }

    public void expandMembers(
            String dn,
            DN baseDn,
            LDAPClient client,
            Collection<Object> users,
            List<String> groups
    ) throws Exception {

        SearchRequest req1 = new SearchRequest();
        req1.setDn(baseDn);
        req1.setFilter(new SimpleFilter(MEMBER_OF, "=", dn));
        req1.setAttributes(new String[] { "objectClass", SAM_ACCOUNT_NAME, MEMBER });

        SearchResponse res1 = new SearchResponse();

        log.debug("Searching for "+req1.getFilter());

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

            } else if (MEMBER.equals(originalName)) {
                Attribute attr = attributes.remove(originalName);
                if (attr == null) continue;

                newAttributes.addValues(fieldName, attr.getValues());

            } else {
                Attribute attr = attributes.remove(originalName);
                if (attr == null) {
                    //if (field.isPrimaryKey()) return null;
                    continue;
                }

                newAttributes.addValues(fieldName, attr.getValues());
            }
        }

        return new SearchResult(newDn, newAttributes);
    }
}
