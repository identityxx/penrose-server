package org.safehaus.penrose.handler.proxy;

import org.safehaus.penrose.handler.DefaultHandler;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.entry.SourceValues;
import org.safehaus.penrose.engine.Engine;

/**
 * @author Endi S. Dewata
 */
public class ProxyHandler extends DefaultHandler {

    public ProxyHandler() throws Exception {
    }

    public Engine getEngine(Partition partition, EntryMapping entryMapping) {
        return engineManager.getEngine("PROXY");
    }

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        final DN baseDn = request.getDn();

        if (debug) {
            log.debug("Base DN: "+baseDn);
            log.debug("Entry mapping: "+entryMapping.getDn());
        }

        Engine engine = getEngine(partition, entryMapping);

        SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>() {
            public void add(SearchResult object) throws Exception {
                SearchResult searchResult = (SearchResult)object;
                response.add(searchResult);
            }
        };

        SourceValues sourceValues = new SourceValues();

        engine.search(
                session,
                partition,
                sourceValues,
                entryMapping,
                request,
                sr
        );
	}
}
