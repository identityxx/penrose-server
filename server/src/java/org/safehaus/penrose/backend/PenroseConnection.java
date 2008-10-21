package org.safehaus.penrose.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.AddRequest;
import org.safehaus.penrose.ldap.AddResponse;
import org.safehaus.penrose.ldap.BindRequest;
import org.safehaus.penrose.ldap.BindResponse;
import org.safehaus.penrose.ldap.CompareRequest;
import org.safehaus.penrose.ldap.CompareResponse;
import org.safehaus.penrose.ldap.DeleteRequest;
import org.safehaus.penrose.ldap.DeleteResponse;
import org.safehaus.penrose.ldap.Modification;
import org.safehaus.penrose.ldap.ModifyRequest;
import org.safehaus.penrose.ldap.ModifyResponse;
import org.safehaus.penrose.ldap.ModRdnRequest;
import org.safehaus.penrose.ldap.ModRdnResponse;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.ldap.UnbindRequest;
import org.safehaus.penrose.ldap.UnbindResponse;
import org.safehaus.penrose.ldap.ConnectRequest;
import org.safehaus.penrose.ldap.DisconnectRequest;

import java.util.Collection;
import java.util.ArrayList;

import org.safehaus.penrose.ldapbackend.Connection;

public class PenroseConnection implements Connection {

    public Logger log = LoggerFactory.getLogger(getClass());
    boolean debug = log.isDebugEnabled();

    Session session;

    public PenroseConnection(Session session) {
        this.session = session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Session getSession() {
        return session;
    }

    public void close() throws Exception {
        session.close();
    }

    public org.safehaus.penrose.ldapbackend.DN getBindDn() {
        return session.getBindDn() == null ? null : new PenroseDN(session.getBindDn());
    }

    public boolean isAnonymous() {
        return session.getBindDn() == null;
    }

    public boolean isRoot() {
        return session.isRootUser();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Connect
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void connect(org.safehaus.penrose.ldapbackend.ConnectRequest request) throws Exception {

        if (debug) log.debug("connect("+request.getConnectionId()+")");

        ConnectRequest penroseRequest = ((PenroseConnectRequest)request).getRequest();

        session.connect(penroseRequest);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Disconnect
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void disconnect(org.safehaus.penrose.ldapbackend.DisconnectRequest request) throws Exception {

        if (debug) log.debug("disconnect("+request.getConnectionId()+")");

        DisconnectRequest penroseRequest = ((PenroseDisconnectRequest)request).getRequest();

        session.disconnect(penroseRequest);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Abandon
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void abandon(
            int idToAbandon
    ) throws Exception {

        if (debug) log.debug("abandon("+idToAbandon+")");

        session.abandon(""+idToAbandon);
    }

    public void abandon(
            org.safehaus.penrose.ldapbackend.AbandonRequest request,
            org.safehaus.penrose.ldapbackend.AbandonResponse response
    ) throws Exception {

        if (debug) log.debug("abandon("+request.getIdToAbandon()+")");

        AbandonRequest penroseRequest = ((PenroseAbandonRequest)request).getAbandonRequest();
        AbandonResponse penroseResponse = ((PenroseAbandonResponse)response).getAbandonResponse();

        session.abandon(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            org.safehaus.penrose.ldapbackend.DN dn,
            org.safehaus.penrose.ldapbackend.Attributes attributes
    ) throws Exception {

        if (debug) log.debug("add("+dn+")");

        DN penroseDn = ((PenroseDN)dn).getDn();
        Attributes penroseAttributes = ((PenroseAttributes)attributes).getAttributes();

        session.add(penroseDn, penroseAttributes);
    }

    public void add(
            org.safehaus.penrose.ldapbackend.AddRequest request,
            org.safehaus.penrose.ldapbackend.AddResponse response
    ) throws Exception {

        if (debug) log.debug("add("+request.getDn()+")");

        AddRequest penroseRequest = ((PenroseAddRequest)request).getAddRequest();
        AddResponse penroseResponse = ((PenroseAddResponse)response).getAddResponse();

        session.add(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            org.safehaus.penrose.ldapbackend.DN dn,
            String password
    ) throws Exception {

        if (debug) log.debug("bind(\""+dn+", \""+password+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        session.bind(penroseDn, password);
    }

    public void bind(
            org.safehaus.penrose.ldapbackend.DN dn,
            byte[] password
    ) throws Exception {

        if (debug) log.debug("bind(\""+dn+"\", \""+new String(password)+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        session.bind(penroseDn, password);
    }

    public void bind(
            org.safehaus.penrose.ldapbackend.BindRequest request,
            org.safehaus.penrose.ldapbackend.BindResponse response
    ) throws Exception {

        byte[] password = request.getPassword();
        if (debug) log.debug("bind(\""+request.getDn()+"\", "+(password == null ? null : "\""+new String(password)+"\"")+")");

        BindRequest penroseRequest = ((PenroseBindRequest)request).getBindRequest();
        BindResponse penroseResponse = ((PenroseBindResponse)response).getBindResponse();

        session.bind(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            org.safehaus.penrose.ldapbackend.DN dn,
            String attributeName,
            Object attributeValue
    ) throws Exception {

        if (debug) log.debug("compare(\""+dn+", \""+attributeName+"\", \""+attributeValue+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        return session.compare(penroseDn, attributeName, attributeValue);
    }

    public boolean compare(
            org.safehaus.penrose.ldapbackend.CompareRequest request,
            org.safehaus.penrose.ldapbackend.CompareResponse response
    ) throws Exception {

        if (debug) log.debug("compare("+request.getDn()+", "+request.getAttributeName()+", "+request.getAttributeValue()+")");

        CompareRequest penroseRequest = ((PenroseCompareRequest)request).getCompareRequest();
        CompareResponse penroseResponse = ((PenroseCompareResponse)response).getCompareResponse();

        session.compare(penroseRequest, penroseResponse);

        return penroseResponse.getReturnCode() == LDAP.COMPARE_TRUE;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            org.safehaus.penrose.ldapbackend.DN dn
    ) throws Exception {

        if (debug) log.debug("delete(\""+dn+")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        session.delete(penroseDn);
    }

    public void delete(
            org.safehaus.penrose.ldapbackend.DeleteRequest request,
            org.safehaus.penrose.ldapbackend.DeleteResponse response
    ) throws Exception {

        if (debug) log.debug("delete("+request.getDn()+")");

        DeleteRequest penroseRequest = ((PenroseDeleteRequest)request).getDeleteRequest();
        DeleteResponse penroseResponse = ((PenroseDeleteResponse)response).getDeleteResponse();

        session.delete(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            org.safehaus.penrose.ldapbackend.DN dn,
            Collection<org.safehaus.penrose.ldapbackend.Modification> modifications
    ) throws Exception {

        if (debug) log.debug("modify("+dn+")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        Collection<Modification> penroseModifications = new ArrayList<Modification>();
        for (org.safehaus.penrose.ldapbackend.Modification modification : modifications) {
            PenroseModification penroseModification = (PenroseModification) modification;
            penroseModifications.add(penroseModification.getModification());
        }

        session.modify(penroseDn, penroseModifications);
    }

    public void modify(
            org.safehaus.penrose.ldapbackend.ModifyRequest request,
            org.safehaus.penrose.ldapbackend.ModifyResponse response
    ) throws Exception {

        if (debug) log.debug("modify("+request.getDn()+")");

        ModifyRequest penroseRequest = ((PenroseModifyRequest)request).getModifyRequest();
        ModifyResponse penroseResponse = ((PenroseModifyResponse)response).getModifyResponse();

        session.modify(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            org.safehaus.penrose.ldapbackend.DN dn,
            org.safehaus.penrose.ldapbackend.RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        if (debug) log.debug("modrdn(\""+dn+"\", \""+newRdn+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();
        RDN penroseNewRdn = ((PenroseRDN)newRdn).getRdn();

        session.modrdn(penroseDn, penroseNewRdn, deleteOldRdn);
    }

    public void modrdn(
            org.safehaus.penrose.ldapbackend.ModRdnRequest request,
            org.safehaus.penrose.ldapbackend.ModRdnResponse response
    ) throws Exception {

        if (debug) log.debug("modrdn(\""+request.getDn()+"\", \""+request.getNewRdn()+"\")");

        ModRdnRequest penroseRequest = ((PenroseModRdnRequest)request).getModRdnRequest();
        ModRdnResponse penroseResponse = ((PenroseModRdnResponse)response).getModRdnResponse();

        session.modrdn(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public org.safehaus.penrose.ldapbackend.SearchResponse search(
            org.safehaus.penrose.ldapbackend.DN dn,
            org.safehaus.penrose.ldapbackend.Filter filter,
            int scope
    ) throws Exception {

        if (debug) log.debug("search(\""+dn+"\", \""+filter+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();
        Filter penroseFilter = ((PenroseFilter)filter).getFilter();

        return new PenroseSearchResponse(session.search(penroseDn, penroseFilter, scope));
    }

    public void search(
            org.safehaus.penrose.ldapbackend.SearchRequest request,
            org.safehaus.penrose.ldapbackend.SearchResponse response
    ) throws Exception {

        if (debug) log.debug("search(\""+request.getDn()+"\", \""+request.getFilter()+"\")");

        SearchRequest penroseRequest = ((PenroseSearchRequest)request).getSearchRequest();
        SearchResponse penroseResponse = ((PenroseSearchResponse)response).getSearchResponse();

        session.search(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
    ) throws Exception {

        if (debug) log.debug("unbind()");

        session.unbind();
    }

    public void unbind(
            org.safehaus.penrose.ldapbackend.UnbindRequest request,
            org.safehaus.penrose.ldapbackend.UnbindResponse response
    ) throws Exception {

        if (debug) log.debug("unbind()");

        UnbindRequest penroseRequest = ((PenroseUnbindRequest)request).getUnbindRequest();
        UnbindResponse penroseResponse = ((PenroseUnbindResponse)response).getUnbindResponse();

        session.unbind(penroseRequest, penroseResponse);
    }
}
