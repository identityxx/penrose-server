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

import java.util.*;
import java.io.File;

import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.Attributes;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.partition.PartitionManager;
import org.safehaus.penrose.server.PenroseServer;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.service.ServiceConfig;
import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

/**
 * @author Endi S. Dewata
 */
public class PenroseBackend implements com.identyx.javabackend.Backend {

    Logger log = LoggerFactory.getLogger(getClass());

    public String home;

    public PenroseServer penroseServer;

    public Map sessions = new HashMap();

    public PenroseBackend() {
        home = System.getProperty("penrose.home");

        File f = new File((home == null ? "" : home+File.separator)+"conf"+File.separator+"log4j.xml");
        if (f.exists()) DOMConfigurator.configure(f.getAbsolutePath());
    }

    public PenroseBackend(PenroseServer penroseServer) {
        this.penroseServer = penroseServer;
        home = penroseServer.getPenroseConfig().getHome();
    }

    /**
     * Initialize Penrose engine.
     *
     * @return return code
     * @throws Exception
     */
    public void init() throws Exception {
        if (penroseServer != null) return;

        penroseServer = new PenroseServer(home);

        PenroseConfig penroseConfig = penroseServer.getPenroseConfig();

        ServiceConfig ldapServiceConfig = penroseConfig.getServiceConfig("LDAP");
        ldapServiceConfig.setEnabled(false);

        penroseServer.start();
    }

    public boolean contains(String dn) throws Exception {
        return contains(new DN(dn));
    }

    public boolean contains(com.identyx.javabackend.DN dn) throws Exception {
        PenroseDN penroseDn = (PenroseDN)dn;
        return contains(penroseDn.getDn());
    }

    public boolean contains(DN dn) throws Exception {
        PenroseConfig penroseConfig = penroseServer.getPenroseConfig();
        if (penroseConfig.getRootDn().equals(dn)) return true;
        
        Penrose penrose = penroseServer.getPenrose();
        PenroseContext penroseContext = penrose.getPenroseContext();
        PartitionManager partitionManager = penroseContext.getPartitionManager();
        return partitionManager.getPartition(dn) != null;
    }

    /**
     * Get session.
     *
     * @param id
     * @return session
     */
    public com.identyx.javabackend.Session getSession(Object id) throws Exception {
        return (PenroseSession)sessions.get(id);
    }

    /**
     * Create session.
     *
     * @param id
     */
    public com.identyx.javabackend.Session createSession(Object id) throws Exception {
        log.debug("openConnection("+id+")");
        Penrose penrose = penroseServer.getPenrose();
        PenroseSession session = new PenroseSession(penrose.newSession());
        if (session == null) throw new Exception("Unable to create session.");
        sessions.put(id, session);
        return session;
    }

    /**
     * Close session.
     *
     * @param id
     */
    public void closeSession(Object id) throws Exception {
        log.debug("closeConnection("+id+")");
        PenroseSession session = (PenroseSession)sessions.remove(id);
        if (session != null) session.close();
    }

    public com.identyx.javabackend.Control createControl(String oid, byte[] value, boolean critical) throws Exception {
        return new PenroseControl(new Control(oid, value, critical));
    }

    public com.identyx.javabackend.DN createDn(String dn) throws Exception {
        return new PenroseDN(new DN(dn));
    }

    public com.identyx.javabackend.RDN createRdn(String rdn) throws Exception {
        return new PenroseRDN(new RDN(rdn));
    }

    public com.identyx.javabackend.Filter createFilter(String filter) throws Exception {
        return new PenroseFilter(FilterTool.parseFilter(filter));
    }

    public com.identyx.javabackend.Attributes createAttributes() throws Exception {
        return new PenroseAttributes(new Attributes());
    }

    public com.identyx.javabackend.Attribute createAttribute(String name) throws Exception {
        return new PenroseAttribute(name);
    }

    public com.identyx.javabackend.Modification createModification(int type, com.identyx.javabackend.Attribute attribute) throws Exception {
        return new PenroseModification(type, attribute);
    }

    public com.identyx.javabackend.AddRequest createAddRequest() throws Exception {
        return new PenroseAddRequest(new AddRequest());
    }

    public com.identyx.javabackend.AddResponse createAddResponse() throws Exception {
        return new PenroseAddResponse(new AddResponse());
    }

    public com.identyx.javabackend.BindRequest createBindRequest() throws Exception {
        return new PenroseBindRequest(new BindRequest());
    }

    public com.identyx.javabackend.BindResponse createBindResponse() throws Exception {
        return new PenroseBindResponse(new BindResponse());
    }

    public com.identyx.javabackend.CompareRequest createCompareRequest() throws Exception {
        return new PenroseCompareRequest(new CompareRequest());
    }

    public com.identyx.javabackend.CompareResponse createCompareResponse() throws Exception {
        return new PenroseCompareResponse(new CompareResponse());
    }

    public com.identyx.javabackend.DeleteRequest createDeleteRequest() throws Exception {
        return new PenroseDeleteRequest(new DeleteRequest());
    }

    public com.identyx.javabackend.DeleteResponse createDeleteResponse() throws Exception {
        return new PenroseDeleteResponse(new DeleteResponse());
    }

    public com.identyx.javabackend.ModifyRequest createModifyRequest() throws Exception {
        return new PenroseModifyRequest(new ModifyRequest());
    }

    public com.identyx.javabackend.ModifyResponse createModifyResponse() throws Exception {
        return new PenroseModifyResponse(new ModifyResponse());
    }

    public com.identyx.javabackend.ModRdnRequest createModRdnRequest() throws Exception {
        return new PenroseModRdnRequest(new ModRdnRequest());
    }

    public com.identyx.javabackend.ModRdnResponse createModRdnResponse() throws Exception {
        return new PenroseModRdnResponse(new ModRdnResponse());
    }

    public com.identyx.javabackend.SearchRequest createSearchRequest() throws Exception {
        return new PenroseSearchRequest(new SearchRequest());
    }

    public com.identyx.javabackend.SearchResponse createSearchResponse() throws Exception {
        return new PenroseSearchResponse(new SearchResponse());
    }

    public com.identyx.javabackend.UnbindRequest createUnbindRequest() throws Exception {
        return new PenroseUnbindRequest(new UnbindRequest());
    }

    public com.identyx.javabackend.UnbindResponse createUnbindResponse() throws Exception {
        return new PenroseUnbindResponse(new UnbindResponse());
    }
}
