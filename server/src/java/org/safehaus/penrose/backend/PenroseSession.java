package org.safehaus.penrose.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.*;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.RDN;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.ldap.*;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

public class PenroseSession implements com.identyx.javabackend.Session {

    public Logger log = LoggerFactory.getLogger(getClass());

    Session session;

    public PenroseSession(Session session) {
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

    public com.identyx.javabackend.DN getBindDn() {
        return session.getBindDn() == null ? null : new PenroseDN(session.getBindDn());
    }

    public boolean isAnonymous() {
        return session.getBindDn() == null;
    }

    public boolean isRoot() {
        return session.isRootUser();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Add
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(
            com.identyx.javabackend.DN dn,
            com.identyx.javabackend.Attributes attributes
    ) throws Exception {

        log.debug("add("+dn+")");

        DN penroseDn = ((PenroseDN)dn).getDn();
        Attributes penroseAttributes = ((PenroseAttributes)attributes).getAttributes();

        session.add(penroseDn, penroseAttributes);
    }

    public void add(
            com.identyx.javabackend.AddRequest request,
            com.identyx.javabackend.AddResponse response
    ) throws Exception {

        log.debug("add("+request.getDn()+")");

        AddRequest penroseRequest = ((PenroseAddRequest)request).getAddRequest();
        AddResponse penroseResponse = ((PenroseAddResponse)response).getAddResponse();

        session.add(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void bind(
            com.identyx.javabackend.DN dn,
            String password
    ) throws Exception {

        log.debug("bind(\""+dn+", \""+password+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        session.bind(penroseDn, password);
    }

    public void bind(
            com.identyx.javabackend.DN dn,
            byte[] password
    ) throws Exception {

        log.debug("bind(\""+dn+", \""+password+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        session.bind(penroseDn, password);
    }

    public void bind(
            com.identyx.javabackend.BindRequest request,
            com.identyx.javabackend.BindResponse response
    ) throws Exception {

        byte[] password = request.getPassword();
        log.debug("bind(\""+request.getDn()+"\", "+(password == null ? null : "\""+new String(password)+"\"")+")");

        BindRequest penroseRequest = ((PenroseBindRequest)request).getBindRequest();
        BindResponse penroseResponse = ((PenroseBindResponse)response).getBindResponse();

        session.bind(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Compare
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean compare(
            com.identyx.javabackend.DN dn,
            String attributeName,
            Object attributeValue
    ) throws Exception {

        log.debug("compare(\""+dn+", \""+attributeName+"\", \""+attributeValue+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        return session.compare(penroseDn, attributeName, attributeValue);
    }

    public boolean compare(
            com.identyx.javabackend.CompareRequest request,
            com.identyx.javabackend.CompareResponse response
    ) throws Exception {

        log.debug("compare("+request.getDn()+", "+request.getAttributeName()+", "+request.getAttributeValue()+")");

        CompareRequest penroseRequest = ((PenroseCompareRequest)request).getCompareRequest();
        CompareResponse penroseResponse = ((PenroseCompareResponse)response).getCompareResponse();

        session.compare(penroseRequest, penroseResponse);

        return penroseResponse.getReturnCode() == LDAP.COMPARE_TRUE;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Delete
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void delete(
            com.identyx.javabackend.DN dn
    ) throws Exception {

        log.debug("delete(\""+dn+")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        session.delete(penroseDn);
    }

    public void delete(
            com.identyx.javabackend.DeleteRequest request,
            com.identyx.javabackend.DeleteResponse response
    ) throws Exception {

        log.debug("delete("+request.getDn()+")");

        DeleteRequest penroseRequest = ((PenroseDeleteRequest)request).getDeleteRequest();
        DeleteResponse penroseResponse = ((PenroseDeleteResponse)response).getDeleteResponse();

        session.delete(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Modify
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modify(
            com.identyx.javabackend.DN dn,
            Collection modifications
    ) throws Exception {

        log.debug("modify("+dn+")");

        DN penroseDn = ((PenroseDN)dn).getDn();

        Collection<Modification> penroseModifications = new ArrayList<Modification>();
        for (Iterator i=modifications.iterator(); i.hasNext(); ) {
            PenroseModification modification = (PenroseModification)i.next();
            penroseModifications.add(modification.getModification());
        }

        session.modify(penroseDn, penroseModifications);
    }

    public void modify(
            com.identyx.javabackend.ModifyRequest request,
            com.identyx.javabackend.ModifyResponse response
    ) throws Exception {

        log.debug("modify("+request.getDn()+")");

        ModifyRequest penroseRequest = ((PenroseModifyRequest)request).getModifyRequest();
        ModifyResponse penroseResponse = ((PenroseModifyResponse)response).getModifyResponse();

        session.modify(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // ModRdn
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void modrdn(
            com.identyx.javabackend.DN dn,
            com.identyx.javabackend.RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        log.debug("modrdn(\""+dn+"\", \""+newRdn+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();
        RDN penroseNewRdn = ((PenroseRDN)newRdn).getRdn();

        session.modrdn(penroseDn, penroseNewRdn, deleteOldRdn);
    }

    public void modrdn(
            com.identyx.javabackend.ModRdnRequest request,
            com.identyx.javabackend.ModRdnResponse response
    ) throws Exception {

        log.debug("modrdn(\""+request.getDn()+"\", \""+request.getNewRdn()+"\")");

        ModRdnRequest penroseRequest = ((PenroseModRdnRequest)request).getModRdnRequest();
        ModRdnResponse penroseResponse = ((PenroseModRdnResponse)response).getModRdnResponse();

        session.modrdn(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Search
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public com.identyx.javabackend.SearchResponse search(
            com.identyx.javabackend.DN dn,
            com.identyx.javabackend.Filter filter,
            int scope
    ) throws Exception {

        log.debug("search(\""+dn+"\", \""+filter+"\")");

        DN penroseDn = ((PenroseDN)dn).getDn();
        Filter penroseFilter = ((PenroseFilter)filter).getFilter();

        return new PenroseSearchResponse(session.search(penroseDn, penroseFilter, scope));
    }

    public void search(
            com.identyx.javabackend.SearchRequest request,
            com.identyx.javabackend.SearchResponse response
    ) throws Exception {

        log.debug("search(\""+request.getDn()+"\", \""+request.getFilter()+"\")");

        SearchRequest penroseRequest = ((PenroseSearchRequest)request).getSearchRequest();
        SearchResponse<SearchResult> penroseResponse = ((PenroseSearchResponse)response).getSearchResponse();

        session.search(penroseRequest, penroseResponse);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Unbind
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void unbind(
    ) throws Exception {

        log.debug("unbind()");

        session.unbind();
    }

    public void unbind(
            com.identyx.javabackend.UnbindRequest request,
            com.identyx.javabackend.UnbindResponse response
    ) throws Exception {

        log.debug("unbind()");

        UnbindRequest penroseRequest = ((PenroseUnbindRequest)request).getUnbindRequest();
        UnbindResponse penroseResponse = ((PenroseUnbindResponse)response).getUnbindResponse();

        session.unbind(penroseRequest, penroseResponse);
    }
}
