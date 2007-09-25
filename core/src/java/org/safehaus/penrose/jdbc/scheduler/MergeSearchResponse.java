package org.safehaus.penrose.jdbc.scheduler;

import org.safehaus.penrose.ldap.*;

/**
 * @author Endi S. Dewata
 */
public class MergeSearchResponse extends SearchResponse {

    SearchResponse response;

    DN lastDn;
    SourceValues lastSourceValues;

    public MergeSearchResponse(
            SearchResponse response
    ) throws Exception {
        this.response = response;
    }

    public void add(SearchResult result) throws Exception {

        DN dn = result.getDn();

        if (debug) {
            log.debug("Synchronizing "+dn);
        }

        SourceValues sv = result.getSourceValues();

        if (lastDn == null) {
            if (debug) log.debug("Generating entry "+dn);
            lastDn = dn;
            lastSourceValues = sv;

        } else if (lastDn.equals(dn)) {
            if (debug) log.debug("Merging entry " + dn);
            lastSourceValues.add(sv);

        } else {
            if (debug) log.debug("Returning entry " + lastDn);
            SearchResult searchResult = new SearchResult();
            searchResult.setDn(lastDn);
            searchResult.setSourceValues(lastSourceValues);
            response.add(searchResult);

            if (debug) log.debug("Generating entry "+dn);
            lastDn = dn;
            lastSourceValues = sv;
        }
    }

    public void close() throws Exception {

        if (lastDn != null) {
            if (debug) log.debug("Returning entry " + lastDn);
            SearchResult searchResult = new SearchResult();
            searchResult.setDn(lastDn);
            searchResult.setSourceValues(lastSourceValues);
            response.add(searchResult);
        }

        response.close();
    }
}
