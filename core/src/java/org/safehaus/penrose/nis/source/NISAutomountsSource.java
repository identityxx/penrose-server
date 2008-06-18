package org.safehaus.penrose.nis.source;

import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.SearchResult;
import org.safehaus.penrose.nis.NISClient;
import org.safehaus.penrose.nis.NISMap;
import org.safehaus.penrose.nis.NISObject;
import org.safehaus.penrose.nis.connection.NISConnection;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.source.Source;

/**
 * @author Endi Sukma Dewata
 */
public class NISAutomountsSource extends Source {

    public final static String BASE = "base";

    NISConnection connection;
    NISClient client;

    public void init() throws Exception {
        connection = (NISConnection)getConnection();
        client = connection.client;
    }

    public NISMap getAutomountMap(Session session, String automountMapName) throws Exception {

        SearchResponse response = new SearchResponse();

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
                return null;
            }
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