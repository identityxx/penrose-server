/**
 * Copyright 2009 Red Hat, Inc.
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
package org.safehaus.penrose.ldapbackend.opends;

import org.opends.messages.MessageBuilder;
import org.opends.server.api.ClientConnection;
import org.opends.server.api.plugin.PreParsePluginResult;
import org.opends.server.core.SearchOperation;
import org.opends.server.protocols.ldap.LDAPClientConnection;
import org.opends.server.types.Control;
import org.opends.server.types.DN;
import org.opends.server.types.*;
import org.opends.server.types.operation.PreParseSearchOperation;
import org.safehaus.penrose.ldapbackend.*;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Endi S. Dewata
 */
public class SearchHandler extends Handler {

    public SearchHandler(LDAPBackendPlugin plugin) {
        super(plugin);
    }

    public PreParsePluginResult process(PreParseSearchOperation operation) {
        final SearchOperation op = (SearchOperation)operation;

        try {
            int messageId = op.getMessageID();
            String rawBaseDn = op.getRawBaseDN().toString();
            String rawFilter = op.getRawFilter().toString();
            SearchScope rawScope = op.getScope();

            if (debug) {
                String s = "sub";
                switch (op.getScope()) {
                    case BASE_OBJECT:
                        s = "base";
                        break;
                    case SINGLE_LEVEL:
                        s = "onelevel";
                        break;
                    case WHOLE_SUBTREE:
                        s = "sub";
                        break;
                    case SUBORDINATE_SUBTREE:
                        s = "subord";
                        break;
                }

                if (debug) log("search", "search(\""+rawBaseDn+"\", \""+rawFilter+"\", \""+s+"\")");
            }

            if (plugin.backend == null) {
                if (debug) log("search", "Missing backend.");
                return new PreParsePluginResult(false, true, false);
            }

            org.safehaus.penrose.ldapbackend.DN baseDn = plugin.backend.createDn(rawBaseDn);
            Filter filter = plugin.backend.createFilter(rawFilter);

            int scope = rawScope.intValue();

            if (!plugin.backend.contains(baseDn)) {
                if (debug) log("search", "Bypassing "+baseDn);
                return new PreParsePluginResult(false, true, false);
                //return PreParsePluginResult.SUCCESS;
            }

            long connectionId = op.getConnectionID();
            Connection connection = plugin.getConnection(connectionId);

            if (connection == null) {
                if (debug) log("search", "Invalid connection "+connectionId+".");
                op.setErrorMessage(new MessageBuilder("Invalid connection "+connectionId+"."));
                op.setResultCode(ResultCode.OPERATIONS_ERROR);
                return new PreParsePluginResult(false, false, true);
            }

            SearchRequest request = plugin.backend.createSearchRequest();
            request.setMessageId(messageId);
            request.setDn(baseDn);
            request.setFilter(filter);
            request.setScope(scope);
            request.setTimeLimit(op.getTimeLimit() * 1000);
            request.setSizeLimit(op.getSizeLimit());
            request.setAttributes(op.getAttributes());
            getControls(op, request);

            final SearchResponse response = plugin.backend.createSearchResponse();
            response.setMessageId(messageId);

            ClientConnection clientConnection = operation.getClientConnection();

            if (clientConnection instanceof LDAPClientConnection) {

                final LDAPClientConnection ldapConnection = (LDAPClientConnection)clientConnection;

                response.addListener(new SearchListener() {
                    public void add(SearchResult result) throws Exception {
                        sendSearchResult(op, ldapConnection, result);
                    }
                    public void add(SearchReference reference) throws Exception {
                        sendSearchReference(op, ldapConnection, reference);
                    }
                    public void close() throws Exception {
                    }
                });

            } else { // does not work properly
                
                response.addListener(new SearchListener() {
                    public void add(SearchResult result) throws Exception {
                        sendSearchResult(op, result);
                    }
                    public void add(SearchReference reference) throws Exception {
                        sendSearchReference(op, reference);
                    }
                    public void close() throws Exception {
                    }
                });
            }

            connection.search(request, response);

            setControls(response, op);

            int rc = response.getReturnCode();
            if (rc != 0) {
                String errorMessage = response.getErrorMessage();
                if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            }

            op.setResultCode(ResultCode.valueOf(rc));

        } catch (org.ietf.ldap.LDAPException e) {
            if (debug) log("search", e);
            String errorMessage = e.getLDAPErrorMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.valueOf(e.getResultCode()));

        } catch (Exception e) {
            if (debug) log("search", e);
            String errorMessage = e.getMessage();
            if (errorMessage != null) op.setErrorMessage(new MessageBuilder(errorMessage));
            op.setResultCode(ResultCode.OPERATIONS_ERROR);
        }

        return new PreParsePluginResult(false, false, true);
    }

    public void sendSearchResult(
            SearchOperation op,
            SearchResult result
    ) throws Exception {

        org.safehaus.penrose.ldapbackend.DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        if (debug) log("search", "Returning "+dn);

        Entry newEntry = plugin.createEntry(dn, attributes);
        List<Control> newControls = plugin.createControls(result.getControls());

        op.returnEntry(newEntry, newControls);
    }

    public void sendSearchReference(
            SearchOperation op,
            SearchReference reference
    ) throws Exception {

        if (debug) log("search", "References:");

        DN dn = plugin.createDn(reference.getDn());

        List<String> urls = new ArrayList<String>();
        for (String url : reference.getUrls()) {
            if (debug) log("search", " - "+url);
            urls.add(url);
        }

        List<Control> newControls = plugin.createControls(reference.getControls());
        SearchResultReference newReference = new SearchResultReference(urls, newControls);

        op.returnReference(dn, newReference);
    }

    public void sendSearchResult(
            SearchOperation op,
            LDAPClientConnection connection,
            SearchResult result
    ) throws Exception {

        org.safehaus.penrose.ldapbackend.DN dn = result.getDn();
        Attributes attributes = result.getAttributes();

        if (debug) log("search", "Returning "+dn);

        Entry newEntry = plugin.createEntry(dn, attributes);
        List<Control> newControls = plugin.createControls(result.getControls());
        SearchResultEntry searchResultEntry = new SearchResultEntry(newEntry, newControls);

        connection.sendSearchEntry(op, searchResultEntry);
    }

    public void sendSearchReference(
            SearchOperation op,
            LDAPClientConnection connection,
            SearchReference reference
    ) throws Exception {

        if (debug) log("search", "References:");

        List<String> urls = new ArrayList<String>();
        for (String url : reference.getUrls()) {
            if (debug) log("search", " - "+url);
            urls.add(url);
        }

        List<Control> newControls = plugin.createControls(reference.getControls());
        SearchResultReference newReference = new SearchResultReference(urls, newControls);

        connection.sendSearchReference(op, newReference);
    }
}
