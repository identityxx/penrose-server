package org.safehaus.penrose.handler.proxy;

import org.safehaus.penrose.handler.DefaultHandler;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.directory.Entry;

/**
 * @author Endi S. Dewata
 */
public class ProxyHandler extends DefaultHandler {

    public ProxyHandler() throws Exception {
    }

    public String getEngineName() {
        return "PROXY";
    }

    public void search(
            final Session session,
            final Entry base,
            final Entry entry,
            SourceValues sourceValues,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        final DN baseDn = request.getDn();

        if (debug) {
            log.debug("Base DN: "+baseDn);
            log.debug("Entry mapping: "+entry.getDn());
        }

        SearchResponse sr = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                response.add(result);
            }
        };

        super.performSearch(
                session,
                base,
                entry,
                sourceValues,
                request,
                sr
        );
	}
}
