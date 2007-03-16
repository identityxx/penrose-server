package org.safehaus.penrose.backend;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.session.*;

public class PenroseSession implements com.identyx.javabackend.Session {

    Logger log = LoggerFactory.getLogger(getClass());

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

    public boolean isRoot() {
        return session.isRootUser();
    }

    /**
     * Performs add operation.
     *
     * @throws Exception
     */
    public void add(
            com.identyx.javabackend.AddRequest request,
            com.identyx.javabackend.AddResponse response
    ) throws Exception {

        log.debug("add("+request.getDn()+")");

        AddRequest penroseRequest = ((PenroseAddRequest)request).getAddRequest();
        AddResponse penroseResponse = ((PenroseAddResponse)response).getAddResponse();

        session.add(penroseRequest, penroseResponse);
    }

    /**
     * Performs bind operation.
     *
     * @throws Exception
     */
    public void bind(
            com.identyx.javabackend.BindRequest request,
            com.identyx.javabackend.BindResponse response
    ) throws Exception {

        log.debug("bind(\""+request.getDn()+", \""+request.getPassword()+"\")");

        BindRequest penroseRequest = ((PenroseBindRequest)request).getBindRequest();
        BindResponse penroseResponse = ((PenroseBindResponse)response).getBindResponse();

        session.bind(penroseRequest, penroseResponse);
    }

    /**
     * Performs compare operation.
     *
     * @throws Exception
     */
    public boolean compare(
            com.identyx.javabackend.CompareRequest request,
            com.identyx.javabackend.CompareResponse response
    ) throws Exception {

        log.debug("compare("+request.getDn()+", "+request.getAttributeName()+", "+request.getAttributeValue()+")");

        CompareRequest penroseRequest = ((PenroseCompareRequest)request).getCompareRequest();
        CompareResponse penroseResponse = ((PenroseCompareResponse)response).getCompareResponse();

        return session.compare(penroseRequest, penroseResponse);
    }

    /**
     * Performs delete operation.
     *
     * @throws Exception
     */
    public void delete(
            com.identyx.javabackend.DeleteRequest request,
            com.identyx.javabackend.DeleteResponse response
    ) throws Exception {

        log.debug("delete("+request.getDn()+")");

        DeleteRequest penroseRequest = ((PenroseDeleteRequest)request).getDeleteRequest();
        DeleteResponse penroseResponse = ((PenroseDeleteResponse)response).getDeleteResponse();

        session.delete(penroseRequest, penroseResponse);
    }

    /**
     * Performs modify operation.
     *
     * @throws Exception
     */
    public void modify(
            com.identyx.javabackend.ModifyRequest request,
            com.identyx.javabackend.ModifyResponse response
    ) throws Exception {

        log.debug("modify("+request.getDn()+")");

        ModifyRequest penroseRequest = ((PenroseModifyRequest)request).getModifyRequest();
        ModifyResponse penroseResponse = ((PenroseModifyResponse)response).getModifyResponse();

        session.modify(penroseRequest, penroseResponse);
    }

    /**
     * Performs modrdn operation.
     *
     * @throws Exception
     */
    public void modrdn(
            com.identyx.javabackend.ModRdnRequest request,
            com.identyx.javabackend.ModRdnResponse response
    ) throws Exception {

        log.debug("modrdn(\""+request.getDn()+"\", \""+request.getNewRdn()+"\")");

        ModRdnRequest penroseRequest = ((PenroseModRdnRequest)request).getModRdnRequest();
        ModRdnResponse penroseResponse = ((PenroseModRdnResponse)response).getModRdnResponse();

        session.modrdn(penroseRequest, penroseResponse);
    }

    /**
     * Performs search operation.
     *
     * @throws Exception
     */
    public void search(
            com.identyx.javabackend.SearchRequest request,
            com.identyx.javabackend.SearchResponse response
    ) throws Exception {

        log.debug("search(\""+request.getDn()+"\", \""+request.getFilter()+"\")");

        SearchRequest penroseRequest = ((PenroseSearchRequest)request).getSearchRequest();
        SearchResponse penroseResponse = ((PenroseSearchResponse)response).getSearchResponse();

        session.search(penroseRequest, penroseResponse);
    }

    /**
     * Performs unbind operation.
     *
     * @throws Exception
     */
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
