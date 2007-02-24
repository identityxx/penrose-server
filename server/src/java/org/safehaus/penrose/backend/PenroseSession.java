package org.safehaus.penrose.backend;

import java.util.Collection;

import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;

import com.identyx.javabackend.Results;
import com.identyx.javabackend.Session;
import org.safehaus.penrose.session.PenroseSearchControls;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PenroseSession implements Session {

    Logger log = LoggerFactory.getLogger(getClass());

    org.safehaus.penrose.session.PenroseSession session;

    public PenroseSession(org.safehaus.penrose.session.PenroseSession session) {
        this.session = session;
    }

    public void setSession(org.safehaus.penrose.session.PenroseSession session) {
        this.session = session;
    }

    public org.safehaus.penrose.session.PenroseSession getSession() {
        return session;
    }

    public void close() throws Exception {
        session.close();
    }

    /**
     * Performs bind operation.
     *
     * @param dn
     * @param password
     * @return return code
     * @throws Exception
     */
    public void bind(String dn, String password) throws Exception {

        log.debug("bind(\""+dn+", \""+password+"\")");

        session.bind(dn, password);
    }

    /**
     * Performs unbind operation.
     *
     * @return return value
     * @throws Exception
     */
    public void unbind() throws Exception {

        log.debug("unbind()");

        session.unbind();
    }

    /**
     * Performs search operation.
     *
     * @param baseDn
     * @param filter
     * @return search result
     * @throws Exception
     */
    public Results search(
            String baseDn,
            String filter,
            SearchControls sc)
    throws Exception {

        log.debug("search(\""+baseDn+"\", \""+filter+"\", sc)");

        PenroseSearchResults results = new PenroseSearchResults();

        PenroseSearchControls psc = new PenroseSearchControls();
        psc.setScope(sc.getSearchScope());
        psc.setSizeLimit(sc.getCountLimit());
        psc.setTimeLimit(sc.getTimeLimit());
        psc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
        psc.setAttributes(sc.getReturningAttributes());

        session.search(baseDn, filter, psc, results);

        return new PenroseResults(results);
    }

    /**
     * Performs add operation.
     *
     * @param dn
     * @param attributes
     * @return return code
     * @throws Exception
     */
    public void add(
            String dn,
            Attributes attributes)
    throws Exception {

        log.debug("add("+dn+")");

        session.add(dn, attributes);
    }

    /**
     * Performs delete operation.
     *
     * @param dn
     * @return return code
     * @throws Exception
     */
    public void delete(
            String dn)
    throws Exception {

        log.debug("delete("+dn+")");

        session.delete(dn);
    }

    /**
     * Performs modify operation.
     *
     * @param dn
     * @param modifications
     * @return return code
     * @throws Exception
     */
    public void modify(
            String dn,
            Collection modifications)
    throws Exception {

        log.debug("modify("+dn+")");

        session.modify(dn, modifications);
    }

    /**
     * Performs modrdn operation.
     *
     * @param dn
     * @param newrdn
     * @return return code
     * @throws Exception
     */
    public void modrdn(
            String dn,
            String newrdn,
            boolean deleteOldRdn)
    throws Exception {

        log.debug("modrdn(\""+dn+"\", \""+newrdn+"\")");

        session.modrdn(dn, newrdn, deleteOldRdn);
    }

    /**
     * Performs compare operation.
     *
     * @param dn
     * @param attributeName
     * @param attributeValue
     * @return return code
     * @throws Exception
     */
    public boolean compare(
            String dn,
            String attributeName,
            Object attributeValue)
    throws Exception {

        log.debug("compare("+dn+", "+attributeName+", "+attributeValue+")");

        return session.compare(dn, attributeName, attributeValue);
    }

    public boolean isRoot() {
        return false;
    }
}
