/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.engine.proxy;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.source.Source;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ProxyEngine extends Engine {

    public final static String BASE_DN                = "baseDn";
    
    public final static String AUTHENTICATON          = "authentication";
    public final static String AUTHENTICATON_DEFAULT  = "default";
    public final static String AUTHENTICATON_FULL     = "full";
    public final static String AUTHENTICATON_DISABLED = "disabled";

    public void init() throws Exception {
        super.init();

        log.debug("Proxy engine initialized.");
    }

    public DN convertDn(DN dn, DN oldSuffix, DN newSuffix) throws Exception {

        if (dn == null || dn.isEmpty()) return null;

        if (!dn.endsWith(oldSuffix)) {
            log.debug("["+dn+"] is not a decendant of ["+oldSuffix+"]");
            return null;
        }

        int start = dn.getSize() - oldSuffix.getSize();

        DNBuilder db = new DNBuilder();
        for (int i=0; i<start; i++) {
            RDN rdn = dn.get(i);
            db.append(rdn);
        }

        db.append(newSuffix);

        return db.toDn();
    }

    public LDAPClient createClient(Session session, Partition partition, Source source) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        if (AUTHENTICATON_DISABLED.equals(authentication)) {
            if (debug) log.debug("Pass-Through Authentication is disabled.");
            throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
        }

        String connectionName = source.getConnectionName();
        Connection connection = partition.getConnection(connectionName);

        return new LDAPClient(connection.getParameters());
    }

    public void storeClient(Session session, Partition partition, Source source, LDAPClient client) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        if (AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Storing connection info in session.");

            String connectionName = source.getConnectionName();
            Connection connection = partition.getConnection(connectionName);

            if (session != null) session.setAttribute(partition.getName()+".connection."+connection.getName(), client);
        } else {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public void closeClient(Session session, Partition partition, Source source, LDAPClient client) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        //if (debug) log.debug("Authentication: "+authentication);

        if (!AUTHENTICATON_FULL.equals(authentication)) {
            try { if (client != null) client.close(); } catch (Exception e) { log.debug(e.getMessage(), e); }
        }
    }

    public LDAPClient getClient(Session session, Partition partition, Source source) throws Exception {

        String authentication = source.getParameter(AUTHENTICATON);
        if (debug) log.debug("Authentication: "+authentication);

        Connection connection = partition.getConnection(source.getConnectionName());
        LDAPClient client;

        if (AUTHENTICATON_FULL.equals(authentication)) {
            if (debug) log.debug("Getting connection info from session.");

            client = session == null ? null : (LDAPClient)session.getAttribute(partition.getName()+".connection."+connection.getName());

            if (client == null) {

                if (session == null || session.isRootUser()) {
                    if (debug) log.debug("Creating new connection.");

                    client = new LDAPClient(connection.getParameters());

                } else {
                    if (debug) log.debug("Missing credentials.");
                    throw LDAP.createException(LDAP.INVALID_CREDENTIALS);
                }
            }

        } else {
            if (debug) log.debug("Creating new connection.");

            client = new LDAPClient(connection.getParameters());
        }

        return client;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        DN dn = request.getDn();
        Attributes attributes = request.getAttributes();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);

        Source source = partition.getSource(sourceMapping.getSourceName());

        LDAPClient client = getClient(session, partition, source);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(source.getParameter(BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Modifying via proxy as \""+targetDn+"\"");

            AddRequest newRequest = new AddRequest();
            newRequest.setDn(targetDn);
            newRequest.setAttributes(attributes);

            client.add(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            BindRequest request,
            BindResponse response
    ) throws Exception {

        DN dn = request.getDn();
        byte[] password = request.getPassword();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);

        Source source = partition.getSource(sourceMapping.getSourceName());

        LDAPClient client = createClient(session, partition, source);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(source.getParameter(BASE_DN));
            DN bindDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Binding via proxy as \""+bindDn +"\" with "+password);

            BindRequest newRequest = new BindRequest();
            newRequest.setDn(bindDn);
            newRequest.setPassword(password);

            client.bind(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            storeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void compare(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            CompareRequest request,
            CompareResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);

        Source source = partition.getSource(sourceMapping.getSourceName());

        LDAPClient client = createClient(session, partition, source);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(source.getParameter(BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Comparing via proxy entry \""+targetDn +"\"");

            CompareRequest newRequest = (CompareRequest)request.clone();
            newRequest.setDn(targetDn);

            boolean result = client.compare(newRequest, response);

            response.setReturnCode(result ? LDAP.COMPARE_TRUE : LDAP.COMPARE_FALSE);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            storeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            DeleteRequest request,
            DeleteResponse response
    ) throws Exception {

        DN dn = request.getDn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);

        Source source = partition.getSource(sourceMapping.getSourceName());

        LDAPClient client = getClient(session, partition, source);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(source.getParameter(BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Deleting via proxy entry \""+targetDn+"\"");

            DeleteRequest newRequest = new DeleteRequest();
            newRequest.setDn(targetDn);

            client.delete(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            ModifyRequest request,
            ModifyResponse response
    ) throws Exception {

        DN dn = request.getDn();
        Collection<Modification> modifications = request.getModifications();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);

        Source source = partition.getSource(sourceMapping.getSourceName());

        LDAPClient client = getClient(session, partition, source);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(source.getParameter(BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Modifying \""+targetDn+"\"");

            ModifyRequest newRequest = new ModifyRequest();
            newRequest.setDn(targetDn);
            newRequest.setModifications(modifications);

            client.modify(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRDN
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            Session session,
            Partition partition,
            EntryMapping entryMapping,
            SourceValues sourceValues,
            ModRdnRequest request,
            ModRdnResponse response
    ) throws Exception {

        DN dn = request.getDn();
        RDN newRdn = request.getNewRdn();
        boolean deleteOldRdn = request.getDeleteOldRdn();

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);

        Source source = partition.getSource(sourceMapping.getSourceName());

        LDAPClient client = getClient(session, partition, source);

        try {
            DN proxyDn = entryMapping.getDn();
            DN proxyBaseDn = new DN(source.getParameter(BASE_DN));
            DN targetDn = convertDn(dn, proxyDn, proxyBaseDn);

            if (debug) log.debug("Renaming \""+targetDn+"\"");

            ModRdnRequest newRequest = new ModRdnRequest();
            newRequest.setDn(targetDn);
            newRequest.setNewRdn(newRdn);
            newRequest.setDeleteOldRdn(deleteOldRdn);

            client.modrdn(newRequest, response);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            closeClient(session, partition, source, client);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void search(
            final Session session,
            final Partition partition,
            final EntryMapping baseMapping,
            final EntryMapping entryMapping,
            final SearchRequest request,
            final SearchResponse<SearchResult> response
    ) throws Exception {

        final DN baseDn = request.getDn();
        final Filter filter = request.getFilter();
        int scope = request.getScope();
        Collection attributes = request.getAttributes();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("SEARCH", 80));
            log.debug(Formatter.displayLine("Mapping DN: \""+entryMapping.getDn()+"\"", 80));
            log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
            log.debug(Formatter.displayLine("Filter: "+filter, 80));
            log.debug(Formatter.displayLine("Scope: "+ LDAP.getScope(scope), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        SourceMapping sourceMapping = entryMapping.getSourceMapping(0);

        final Source source = partition.getSource(sourceMapping.getSourceName());

        final LDAPClient client = getClient(session, partition, source);

        try {
            final DN proxyDn = entryMapping.getDn();
            if (debug) log.debug("Proxy DN: "+proxyDn);

            final DN proxyBaseDn = new DN(source.getParameter(BASE_DN));
            if (debug) log.debug("Proxy Base DN: "+proxyBaseDn);

            if (debug) log.debug("Checking whether "+baseDn+" is under "+entryMapping.getDn());
            boolean found = baseDn.endsWith(entryMapping.getDn());

            if (debug) log.debug("Result: "+found);

            SearchRequest newRequest = (SearchRequest)request.clone();
            newRequest.setFilter(filter);
            newRequest.setScope(scope);
            //newRequest.addControl(new Control("2.16.840.1.113730.3.4.2", null, true));

            final DN targetDn;
            if (found) {
                targetDn = convertDn(baseDn, proxyDn, proxyBaseDn);

            } else {
                if (scope == SearchRequest.SCOPE_BASE) {
                    //return;

                } else if (scope == SearchRequest.SCOPE_ONE) {
                    newRequest.setScope(SearchRequest.SCOPE_BASE);
                }
                targetDn = proxyBaseDn;
            }

            newRequest.setDn(targetDn);

            SearchResponse<SearchResult> sr = new SearchResponse<SearchResult>() {
                public void add(SearchResult result) throws Exception {

                    DN dn = convertDn(result.getDn(), proxyBaseDn, proxyDn);
                    if (debug) log.debug("Entry "+dn);

                    Attributes attributes = (Attributes)result.getAttributes().clone();

                    SearchResult searchResult = new SearchResult(dn, attributes);
                    searchResult.setEntryMapping(entryMapping);
                    response.add(searchResult);
                }
            };

            if (debug) log.debug("Searching for \""+targetDn+"\" with filter="+filter+" attrs="+attributes);

            client.search(newRequest, sr);

        } catch (Exception e) {
            throw LDAP.createException(e);

        } finally {
            response.close();
            closeClient(session, partition, source, client);
        }
    }
}
