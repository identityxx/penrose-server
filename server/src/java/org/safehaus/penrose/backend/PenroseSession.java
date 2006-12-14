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

import com.novell.ldap.LDAPException;

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
    public int bind(String dn, String password) throws Exception {

        log.debug("bind(\""+dn+", \""+password+"\")");

        try {
            return session.bind(dn, password);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs unbind operation.
     *
     * @return return value
     * @throws Exception
     */
    public int unbind() throws Exception {

        log.debug("unbind()");

        try {
            return session.unbind();

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
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

        try {
            PenroseSearchControls psc = new PenroseSearchControls();
            psc.setScope(sc.getSearchScope());
            psc.setSizeLimit(sc.getCountLimit());
            psc.setTimeLimit(sc.getTimeLimit());
            psc.setDereference(PenroseSearchControls.DEREF_ALWAYS);
            psc.setAttributes(sc.getReturningAttributes());

            int rc = session.search(baseDn, filter, psc, results);
            results.setReturnCode(rc);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);

            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            results.close();
        }

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
    public int add(
            String dn,
            Attributes attributes)
    throws Exception {

        log.debug("add("+dn+")");

        try {
            return session.add(dn, attributes);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs delete operation.
     *
     * @param dn
     * @return return code
     * @throws Exception
     */
    public int delete(
            String dn)
    throws Exception {

        log.debug("delete("+dn+")");

        try {
            return session.delete(dn);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs modify operation.
     *
     * @param dn
     * @param modifications
     * @return return code
     * @throws Exception
     */
    public int modify(
            String dn,
            Collection modifications)
    throws Exception {

        log.debug("modify("+dn+")");

        try {
            return session.modify(dn, modifications);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }

    /**
     * Performs modrdn operation.
     *
     * @param dn
     * @param newrdn
     * @return return code
     * @throws Exception
     */
    public int modrdn(
            String dn,
            String newrdn,
            boolean deleteOldRdn)
    throws Exception {

        log.debug("modrdn(\""+dn+"\", \""+newrdn+"\")");

        try {
            return session.modrdn(dn, newrdn, deleteOldRdn);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
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
    public int compare(
            String dn,
            String attributeName,
            Object attributeValue)
    throws Exception {

        log.debug("compare("+dn+", "+attributeName+", "+attributeValue+")");

        try {
            return session.compare(dn, attributeName, attributeValue);

        } catch (Throwable e) {
            log.error(e.getMessage(), e);
            return LDAPException.OPERATIONS_ERROR;
        }
    }
}
