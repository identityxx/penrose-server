package org.safehaus.penrose.nis.source;

import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterEvaluator;
import org.safehaus.penrose.util.TextUtil;
import org.safehaus.penrose.partition.PartitionContext;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.nis.connection.NISConnection;
import org.safehaus.penrose.nis.NISClient;
import org.safehaus.penrose.nis.NIS;
import org.safehaus.penrose.Penrose;

import java.util.*;

/**
 * @author Endi Sukma Dewata
 */
public class NISSource extends Source {

    public NISConnection connection;

    public String base;
    public String type;

    public void init() throws Exception {
        connection = (NISConnection)getConnection();

        base = getParameter(NIS.BASE);
        type = getParameter(NIS.OBJECT_CLASSES);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        String serviceName = getParameter(NIS.PAM);

        if (serviceName == null || serviceName.equals("")) {
            Penrose.errorLog.error("Missing PAM service name.");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }

        RDN rdn = request.getDn().getRdn();

        String username = (String)rdn.getValue();
        byte[] password = request.getPassword();

        NISClient client = connection.createClient();
        client.bind(serviceName, username, password);
        client.close();
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

        final Map<DN,SearchResult> entries = new LinkedHashMap<DN,SearchResult>();

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {
                SearchResult sr = entries.get(result.getDn()); // merge
                if (sr == null) {
                    entries.put(result.getDn(), result);
                } else {
                    sr.getAttributes().add(result.getAttributes());
                }
            }
            public void close() throws Exception {
                // ignore
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

            PartitionContext partitionContext = partition.getPartitionContext();
            PenroseContext penroseContext = partitionContext.getPenroseContext();

            final FilterEvaluator filterEvaluator = penroseContext.getFilterEvaluator();
            for (SearchResult result : entries.values()) {
                Attributes attributes = result.getAttributes();
                if (!filterEvaluator.eval(attributes, filter)) return;

                response.add(result);
            }

        } finally {
            response.close();
            client.close();
        }

        log.debug("Search operation completed.");
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Automount Maps
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void searchAutomountMaps(
            final Session session,
            final String automountMapName,
            final SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("Search Automount Maps", 70));
            log.debug(TextUtil.displayLine(" - Map : "+automountMapName, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        if (automountMapName == null) {

            String base = getParameter(NIS.BASE);

            Collection<String> names = getAutomountMapNames(base);
/*
            StringTokenizer st = new StringTokenizer(base, ", ");
            while (st.hasMoreTokens()) {
                String name = st.nextToken();
                names.add(name);
            }
*/
            for (String name : names) {

                RDNBuilder rb = new RDNBuilder();
                rb.set("automountMapName", name);
                DN dn = new DN(rb.toRdn());

                Attributes attributes = new Attributes();
                attributes.setValue("automountMapName", name);

                SearchResult result = new SearchResult(dn, attributes);
                response.add(result);
            }

        } else {

            RDNBuilder rb = new RDNBuilder();
            rb.set("automountMapName", automountMapName);
            DN dn = new DN(rb.toRdn());

            Attributes attributes = new Attributes();
            attributes.setValue("automountMapName", automountMapName);

            SearchResult result = new SearchResult(dn, attributes);
            response.add(result);
        }

        response.close();
    }

    public Collection<String> getAutomountMapNames(
        String base
    ) throws Exception {

        Collection<String> list = new LinkedHashSet<String>();
        list.add(base);

        getAutomountMapNames(base, list);

        return list;
    }

    public void getAutomountMapNames(
            final String base,
            final Collection<String> list
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                Attributes attributes = result.getAttributes();
                String automountKey = (String)attributes.getValue("automountKey");
                String automountInformation = (String)attributes.getValue("automountInformation");

                if (debug) log.debug(" - "+automountKey+": "+automountInformation);
                if (automountInformation == null) return;

                StringTokenizer st = new StringTokenizer(automountInformation, " \t");
                if (!st.hasMoreTokens()) return;

                String automountMapName = st.nextToken();
                if (automountMapName.startsWith("-")) return;

                if (automountMapName.indexOf(":") > 0) return;

                if (automountMapName.startsWith("/")) {
                    int i = automountMapName.lastIndexOf('/');
                    automountMapName = automountMapName.substring(i+1);
                }

                if (automountMapName.startsWith("auto_")) {
                    automountMapName = "auto." + automountMapName.substring(5);
                }

                try {
                    getAutomountMapNames(automountMapName, list);
                    list.add(automountMapName);

                } catch (Exception e) {
                    // ignore
                }
            }
        };

        NISClient client = connection.createClient();

        try {
            client.list(base, "automount", newResponse);

        } catch (Exception e) {

            String automountMapName = base;

            if (automountMapName.startsWith("auto.")) {
                automountMapName = "auto_" + automountMapName.substring(5);
                client.list(automountMapName, "automount", newResponse);

            } else {
                if (debug) log.debug("Automount map "+automountMapName+" is not stored in NIS.");
                throw e;
            }

        } finally {
            client.close();
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Automount Entries
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void searchAutomountEntries(
            Session session,
            String automountKey,
            String automountMapName,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug(TextUtil.displaySeparator(70));
            log.debug(TextUtil.displayLine("Search Automount Entries", 70));
            log.debug(TextUtil.displayLine(" - Key : "+automountKey, 70));
            log.debug(TextUtil.displayLine(" - Map : "+automountMapName, 70));
            log.debug(TextUtil.displaySeparator(70));
        }

        if (automountMapName == null) {

            String base = getParameter(NIS.BASE);
            getAutomountEntries(base, null, response);
/*
            Collection<SearchResult> results = getAutomountMapNames(base);

            for (SearchResult result : results) {

                RDN rdn = result.getDn().getRdn();
                automountMapName = (String)rdn.get("automountMapName");

                getAutomountEntries(automountMapName, null, response);
            }
*/
        } else if (automountKey == null) {
            getAutomountEntries(automountMapName, null, response);

        } else {

            if (automountKey.equals("/")) {
                automountKey = "*";
            }

            RDNBuilder rb = new RDNBuilder();
            rb.set("automountKey", automountKey);
            RDN rdn = rb.toRdn();

            getAutomountEntries(automountMapName, rdn, response);
        }

        response.close();
    }
/*
    public void getAutomountEntries(
            final String base,
            final RDN rdn,
            final SearchResponse response
    ) throws Exception {

        log.debug("Searching "+ base +":");

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                Attributes attributes = result.getAttributes();
                String automountKey = (String)attributes.getValue("automountKey");
                String automountInformation = (String)attributes.getValue("automountInformation");

                if (automountKey.equals("*")) { // convert * into /
                    automountKey = "/";

                    RDNBuilder rb = new RDNBuilder();
                    rb.set("automountMapName", base);
                    rb.set("automountKey", automountKey);
                    DN dn = new DN(rb.toRdn());
                    result.setDn(dn);

                    attributes.setValue("primaryKey.automountKey", automountKey);
                    attributes.setValue("automountKey", automountKey);
                }

                if (automountInformation == null) {
                    response.add(result);
                    return;
                }

                StringTokenizer st = new StringTokenizer(automountInformation, " \t");
                if (!st.hasMoreTokens()) {
                    response.add(result);
                    return;
                }

                String automountMapName = st.nextToken();
                String remainder = automountInformation.substring(automountMapName.length()).trim();

                if (automountMapName.startsWith("-")) {
                    response.add(result);
                    return;
                }

                if (automountMapName.indexOf(":") > 0) {
                    response.add(result);
                    return;
                }

                if (automountMapName.startsWith("/")) {
                    int i = automountMapName.lastIndexOf('/');
                    automountMapName = automountMapName.substring(i+1);
                }

                if (automountMapName.startsWith("auto_")) {
                    automountMapName = "auto." + automountMapName.substring(5);
                }

                automountInformation = "ldap:"+automountMapName+" "+remainder;
                attributes.setValue("automountInformation", automountInformation);

                response.add(result);
            }
        };

        if (rdn == null) {

            try {
                client.list(base, "automount", newResponse);

            } catch (Exception e) {

                String automountMapName = base;

                if (automountMapName.startsWith("auto.")) {
                    automountMapName = "auto_" + automountMapName.substring(5);
                    client.list(automountMapName, "automount", newResponse);

                } else {
                    if (debug) log.debug("Automount map "+automountMapName+" is not stored in NIS.");
                    throw e;
                }
            }

        } else {

            try {
                client.lookup(base, rdn, "automount", newResponse);

            } catch (Exception e) {

                String automountMapName = base;

                if (automountMapName.startsWith("auto.")) {
                    automountMapName = "auto_" + automountMapName.substring(5);
                    client.lookup(automountMapName, rdn, "automount", newResponse);

                } else {
                    if (debug) log.debug("Automount map "+automountMapName+" is not stored in NIS.");
                    throw e;
                }
            }
        }
    }
*/
    public void getAutomountEntries(
            final String base,
            final RDN rdn,
            final SearchResponse response
    ) throws Exception {

        Collection<String> names = new LinkedHashSet<String>();
        getAutomountEntries(base, rdn, response, names);
    }

    public void getAutomountEntries(
            final String base,
            final RDN rdn,
            final SearchResponse response,
            final Collection<String> names
    ) throws Exception {

        final boolean debug = log.isDebugEnabled();
        if (names.contains(base)) return;

        names.add(base);

        log.debug("Searching "+ base +":");

        SearchResponse newResponse = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                Attributes attributes = result.getAttributes();
                String automountKey = (String)attributes.getValue("automountKey");
                String automountInformation = (String)attributes.getValue("automountInformation");

                if (automountKey.equals("*")) { // convert * into /
                    automountKey = "/";

                    RDNBuilder rb = new RDNBuilder();
                    rb.set("automountMapName", base);
                    rb.set("automountKey", automountKey);
                    DN dn = new DN(rb.toRdn());
                    result.setDn(dn);

                    attributes.setValue("primaryKey.automountKey", automountKey);
                    attributes.setValue("automountKey", automountKey);
                }

                if (automountInformation == null) {
                    response.add(result);
                    return;
                }

                StringTokenizer st = new StringTokenizer(automountInformation, " \t");
                if (!st.hasMoreTokens()) {
                    response.add(result);
                    return;
                }

                String automountMapName = st.nextToken();
                String remainder = automountInformation.substring(automountMapName.length()).trim();

                if (automountMapName.startsWith("-")) {
                    response.add(result);
                    return;
                }

                if (automountMapName.indexOf(":") > 0) {
                    response.add(result);
                    return;
                }

                if (automountMapName.startsWith("/")) {
                    int i = automountMapName.lastIndexOf('/');
                    automountMapName = automountMapName.substring(i+1);
                }

                if (automountMapName.startsWith("auto_")) {
                    automountMapName = "auto." + automountMapName.substring(5);
                }
                
                try {
                    getAutomountEntries(automountMapName, rdn, response, names);

                    automountInformation = "ldap:"+automountMapName;
                    if (remainder.length() > 0) automountInformation += " "+remainder;
                    attributes.setValue("automountInformation", automountInformation);

                } catch (Exception e) {
                    if (debug) log.debug("Automount map "+automountMapName+" is not stored in NIS.");
                }

                response.add(result);
            }
        };

        if (rdn == null) {

            NISClient client = connection.createClient();

            try {
                client.list(base, "automount", newResponse);

            } catch (Exception e) {

                String automountMapName = base;

                if (automountMapName.startsWith("auto.")) {
                    automountMapName = "auto_" + automountMapName.substring(5);
                    client.list(automountMapName, "automount", newResponse);

                } else {
                    if (debug) log.debug("Automount map "+automountMapName+" is not stored in NIS.");
                    throw e;
                }

            } finally {
                client.close();
            }

        } else {

            NISClient client = connection.createClient();

            try {
                client.lookup(base, rdn, "automount", newResponse);

            } catch (Exception e) {

                String automountMapName = base;

                if (automountMapName.startsWith("auto.")) {
                    automountMapName = "auto_" + automountMapName.substring(5);
                    client.lookup(automountMapName, rdn, "automount", newResponse);

                } else {
                    if (debug) log.debug("Automount map "+automountMapName+" is not stored in NIS.");
                    throw e;
                }

            } finally {
                client.close();
            }
        }
    }
}
