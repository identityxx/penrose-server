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
package org.safehaus.penrose.backend;

import org.apache.log4j.xml.DOMConfigurator;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldapbackend.ConnectRequest;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.server.PenroseServer;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.safehaus.penrose.ldapbackend.Connection;
import org.safehaus.penrose.ldapbackend.DisconnectRequest;
import org.safehaus.penrose.ldapbackend.Attribute;
import org.safehaus.penrose.ldapbackend.Modification;

/**
 * @author Endi S. Dewata
 */
public class PenroseBackend implements org.safehaus.penrose.ldapbackend.Backend {

    public File home;

    public PenroseServer penroseServer;

    public Map<Object,PenroseConnection> connections = Collections.synchronizedMap(new HashMap<Object,PenroseConnection>());

    public PenroseBackend() {
        home = new File(System.getProperty("penrose.home"));

        File f = new File(home, "conf"+File.separator+"log4j.xml");
        if (f.exists()) DOMConfigurator.configure(f.getAbsolutePath());
    }

    public PenroseBackend(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
        home = penroseServer.getHome();
    }

    public void init() throws Exception {
        if (penroseServer != null) return;

        penroseServer = new PenroseServer(home);

        penroseServer.start();
    }

    public boolean contains(String dn) throws Exception {
        return contains(new DN(dn));
    }

    public boolean contains(org.safehaus.penrose.ldapbackend.DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        return contains(penroseDn.getDn());
    }

    public boolean contains(DN dn) throws Exception {
        Penrose penrose = penroseServer.getPenrose();
        PenroseConfig penroseConfig = penrose.getPenroseConfig();
        if (penroseConfig.getRootDn().matches(dn)) return true;

        PartitionManager partitionManager = penrose.getPartitionManager();
        return !partitionManager.findEntries(dn).isEmpty();
    }

    public Connection getConnection(Object connectionId) throws Exception {
        return connections.get(connectionId);
    }

    public ConnectRequest createConnectRequest() throws Exception {
        return new PenroseConnectRequest();
    }

    public DisconnectRequest createDisconnectRequest() throws Exception {
        return new PenroseDisconnectRequest();
    }

    public Connection connect(ConnectRequest request) throws Exception {

        Object connectionId = request.getConnectionId();

        Penrose penrose = penroseServer.getPenrose();
        Session session = penrose.createSession(""+connectionId);

        PenroseConnection connection = new PenroseConnection(session);
        connection.connect(request);

        connections.put(connectionId, connection);

        return connection;
    }

    public void disconnect(DisconnectRequest request) throws Exception {

        Object connectionId = request.getConnectionId();

        PenroseConnection connection = connections.remove(connectionId);
        if (connection == null) return;

        connection.disconnect(request);
        connection.close();
    }

    public org.safehaus.penrose.ldapbackend.Control createControl(String oid, byte[] value, boolean critical) throws Exception {
        return new PenroseControl(new Control(oid, value, critical));
    }

    public org.safehaus.penrose.ldapbackend.DN createDn(String dn) throws Exception {
        return new PenroseDN(new DN(dn));
    }

    public org.safehaus.penrose.ldapbackend.RDN createRdn(String rdn) throws Exception {
        return new PenroseRDN(new RDN(rdn));
    }

    public org.safehaus.penrose.ldapbackend.Filter createFilter(String filter) throws Exception {
        return new PenroseFilter(FilterTool.parseFilter(filter));
    }

    public org.safehaus.penrose.ldapbackend.Attributes createAttributes() throws Exception {
        return new PenroseAttributes(new Attributes());
    }

    public Attribute createAttribute(String name) throws Exception {
        return new PenroseAttribute(name);
    }

    public Modification createModification(int type, Attribute attribute) throws Exception {
        return new PenroseModification(type, attribute);
    }

    public org.safehaus.penrose.ldapbackend.AbandonRequest createAbandonRequest() throws Exception {
        return new PenroseAbandonRequest(new AbandonRequest());
    }

    public org.safehaus.penrose.ldapbackend.AbandonResponse createAbandonResponse() throws Exception {
        return new PenroseAbandonResponse(new AbandonResponse());
    }

    public org.safehaus.penrose.ldapbackend.AddRequest createAddRequest() throws Exception {
        return new PenroseAddRequest(new AddRequest());
    }

    public org.safehaus.penrose.ldapbackend.AddResponse createAddResponse() throws Exception {
        return new PenroseAddResponse(new AddResponse());
    }

    public org.safehaus.penrose.ldapbackend.BindRequest createBindRequest() throws Exception {
        return new PenroseBindRequest(new BindRequest());
    }

    public org.safehaus.penrose.ldapbackend.BindResponse createBindResponse() throws Exception {
        return new PenroseBindResponse(new BindResponse());
    }

    public org.safehaus.penrose.ldapbackend.CompareRequest createCompareRequest() throws Exception {
        return new PenroseCompareRequest(new CompareRequest());
    }

    public org.safehaus.penrose.ldapbackend.CompareResponse createCompareResponse() throws Exception {
        return new PenroseCompareResponse(new CompareResponse());
    }

    public org.safehaus.penrose.ldapbackend.DeleteRequest createDeleteRequest() throws Exception {
        return new PenroseDeleteRequest(new DeleteRequest());
    }

    public org.safehaus.penrose.ldapbackend.DeleteResponse createDeleteResponse() throws Exception {
        return new PenroseDeleteResponse(new DeleteResponse());
    }

    public org.safehaus.penrose.ldapbackend.ModifyRequest createModifyRequest() throws Exception {
        return new PenroseModifyRequest(new ModifyRequest());
    }

    public org.safehaus.penrose.ldapbackend.ModifyResponse createModifyResponse() throws Exception {
        return new PenroseModifyResponse(new ModifyResponse());
    }

    public org.safehaus.penrose.ldapbackend.ModRdnRequest createModRdnRequest() throws Exception {
        return new PenroseModRdnRequest(new ModRdnRequest());
    }

    public org.safehaus.penrose.ldapbackend.ModRdnResponse createModRdnResponse() throws Exception {
        return new PenroseModRdnResponse(new ModRdnResponse());
    }

    public org.safehaus.penrose.ldapbackend.SearchRequest createSearchRequest() throws Exception {
        return new PenroseSearchRequest(new SearchRequest());
    }

    public org.safehaus.penrose.ldapbackend.SearchResponse createSearchResponse() throws Exception {
        return new PenroseSearchResponse(new SearchResponse());
    }

    public org.safehaus.penrose.ldapbackend.UnbindRequest createUnbindRequest() throws Exception {
        return new PenroseUnbindRequest(new UnbindRequest());
    }

    public org.safehaus.penrose.ldapbackend.UnbindResponse createUnbindResponse() throws Exception {
        return new PenroseUnbindResponse(new UnbindResponse());
    }
}
