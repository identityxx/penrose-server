package org.safehaus.penrose.nis.source;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.nis.NISClient;
import org.safehaus.penrose.nis.NISMap;
import org.safehaus.penrose.nis.NISObject;
import org.safehaus.penrose.nis.connection.NISConnection;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.naming.PenroseContext;

/**
 * @author Endi Sukma Dewata
 */
public class NISAutomountsSource extends Source {

    public final static String BASE           = "base";
    public final static String OBJECT_CLASSES = "objectClasses";

    public NISConnection connection;

    public String base;
    public String type;

    public void init() throws Exception {
        connection = (NISConnection)getConnection();

        base = getParameter(BASE);
        type = getParameter(OBJECT_CLASSES);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final SearchRequest request,
            final SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        final DN baseDn = request.getDn();
        final Filter filter = request.getFilter();
        final int scope = request.getScope();

        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("Search "+getName(), 70));
            log.debug(TextUtil.displayLine(" - Base   : "+baseDn, 70));
            log.debug(TextUtil.displayLine(" - Filter : "+filter, 70));
            log.debug(TextUtil.displayLine(" - Scope  : "+LDAP.getScope(scope), 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        PartitionContext partitionContext = partition.getPartitionContext();
        PenroseContext penroseContext = partitionContext.getPenroseContext();

        final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                Attributes attributes = result.getAttributes();
                if (!filterEvaluator.eval(attributes, filter)) return;
                response.add(result);
            }
        };

        newResponse.setSizeLimit(request.getSizeLimit());

        NISClient client = connection.createClient();

        try {
            if (baseDn != null && baseDn.isEmpty()) {

                if (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB) {

                    if (debug) log.debug("Searching root entry.");

                    SearchResult result = new SearchResult();
                    response.add(result);
                }

                if (scope == SearchRequest.SCOPE_ONE || scope == SearchRequest.SCOPE_SUB) {

                    if (debug) log.debug("Searching top entries.");

                    client.list(base, type, newResponse);
                }

            } else if (baseDn != null && (scope == SearchRequest.SCOPE_BASE || scope == SearchRequest.SCOPE_SUB)) {

                RDN rdn = baseDn.getRdn();

                if (debug) log.debug("Searching entry: "+rdn);

                client.lookup(base, rdn, type, newResponse);

            } else if (baseDn == null) {

                if (debug) log.debug("Searching all entries.");

                client.list(base, type, newResponse);
            }

        } finally {
            response.close();
            client.close();
        }

        log.debug("Search operation completed.");
    }

    public NISMap getAutomountMap(Session session, String automountMapName) throws Exception {

        boolean debug = log.isDebugEnabled();

        SearchResponse response = new SearchResponse();

        NISClient client = connection.createClient();

        try {
            client.list(automountMapName, "automount", response);

        } catch (Exception e) {

            if (debug) log.debug("Failed accessing "+automountMapName+": "+e.getMessage());

            if (automountMapName.startsWith("auto.")) {
                automountMapName = "auto_" + automountMapName.substring(5);
            }

            try {
                client.list(automountMapName, "automount", response);

            } catch (Exception ex) {
                if (debug) log.debug("Failed accessing "+automountMapName+": "+ex.getMessage());
                throw LDAP.createException(LDAP.OPERATIONS_ERROR);
            }

        } finally {
            client.close();
        }

        NISMap map = new NISMap();
        map.setName(automountMapName);

        while (response.hasNext()) {

            SearchResult result = response.next();
            Attributes attributes = result.getAttributes();

            NISObject entry = new NISObject();

            String automountKey = (String)attributes.getValue("automountKey");
            entry.setName(automountKey);

            String automountInformation = (String)attributes.getValue("automountInformation");
            entry.setValue(automountInformation);

            String description = (String)attributes.getValue("description");
            entry.setDescription(description);

            map.addObject(entry);
        }

        return map;
    }
}