package org.safehaus.penrose.directory;

import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.interpreter.Interpreter;

/**
 * @author Endi Sukma Dewata
 */
public class RootEntry extends Entry {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Scope
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void validateScope(SearchRequest request) throws Exception {

        int scope = request.getScope();

        if (scope != SearchRequest.SCOPE_BASE) {
            log.debug("Entry out of scope.");
            throw LDAP.createException(LDAP.NO_SUCH_OBJECT);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Filter
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

     public void validateFilter(Filter filter) throws Exception {
        // ignore
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
            log.debug(TextUtil.displayLine("ROOT DSE SEARCH", 80));
            log.debug(TextUtil.displayLine("Entry  : "+getDn(), 80));
            log.debug(TextUtil.displayLine("Base   : "+baseDn, 80));
            log.debug(TextUtil.displayLine("Filter : "+filter, 80));
            log.debug(TextUtil.displayLine("Scope  : "+ LDAP.getScope(scope), 80));
            log.debug(TextUtil.displaySeparator(80));
        }

        try {
            validateSearchRequest(session, request, response);

        } catch (Exception e) {
            response.close();
            return;
        }

        response = createSearchResponse(session, request, response);

        try {
            executeSearch(session, request, response);

        } finally {
            response.close();
        }
    }

    public void executeSearch(
            Session session,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        Interpreter interpreter = partition.newInterpreter();

        DN dn = new DN();
        Attributes attributes = computeAttributes(interpreter);

        SearchResult result = new SearchResult(dn, attributes);
        result.setEntryId(getId());

        response.add(result);
    }
}