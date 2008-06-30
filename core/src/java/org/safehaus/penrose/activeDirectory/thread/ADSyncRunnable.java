package org.safehaus.penrose.activeDirectory.thread;

import org.safehaus.penrose.activeDirectory.module.ADSyncModule;
import org.safehaus.penrose.control.Control;
import org.safehaus.penrose.control.DirSyncControl;
import org.safehaus.penrose.control.DirSyncResponseControl;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.connection.LDAPConnection;
import org.safehaus.penrose.ldap.source.LDAPSource;
import org.safehaus.penrose.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class ADSyncRunnable implements Runnable {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean warn = log.isWarnEnabled();
    public boolean debug = log.isDebugEnabled();

    ADSyncModule module;
    LDAPSource source;
    LDAPConnection connection;

    byte[] cookie;

    boolean stopped;

    public ADSyncRunnable(ADSyncModule module) throws Exception {
        this.module = module;

        source = module.getSource();
        connection = (LDAPConnection)source.getConnection();

        Session session = module.createAdminSession();
        LDAPClient client = connection.getClient(session);

        SearchRequest request = new SearchRequest();
        request.setDn(module.getBaseDn());
        request.setFilter(source.getFilter());
        request.setAttributes(source.getFieldOriginalNames());
        request.addAttribute("isDeleted");

        Collection<Control> requestControls = new ArrayList<Control>();
        requestControls.add(new DirSyncControl());
        request.setControls(requestControls);

        SearchResponse response = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                DN dn = result.getDn();
                if (!dn.endsWith(source.getBaseDn())) return;

                if (debug) log.debug("Found entry "+result.getDn()+".");

                super.add(result);
            }
        };

        client.search(request, response);

        response.waitFor();
        log.debug("Total entries: "+response.getTotalCount());

        Collection<Control> responseControls = response.getControls();
        for (Control control : responseControls) {
            if (!DirSyncResponseControl.OID.equals(control.getOid())) continue;

            log.debug("DirSyncResponseControl:");

            DirSyncResponseControl dirSyncResponseControl = new DirSyncResponseControl(control);
            cookie = dirSyncResponseControl.getCookie();

            log.debug(" - flag: "+dirSyncResponseControl.getFlag());
            log.debug(" - maxReturnLength: "+dirSyncResponseControl.getMaxReturnLength());
        }

        session.close();
    }

    public void run() {
        try {
            runImpl();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    public void runImpl() throws Exception {

        while (!stopped) {
            try {
                Thread.sleep(module.getInterval() * 1000);

                synchronize();

            } catch (Exception e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    public void stop() {
        stopped = true;
    }

    public void synchronize() throws Exception {

        Session session = module.createAdminSession();
        LDAPClient client = connection.getClient(session);

        SearchRequest request = new SearchRequest();
        request.setDn(module.getBaseDn());
        request.setFilter(source.getFilter());
        request.setAttributes(source.getFieldOriginalNames());
        request.addAttribute("isDeleted");

        Collection<Control> requestControls = new ArrayList<Control>();
        requestControls.add(new DirSyncControl(1, Integer.MAX_VALUE, cookie, true));
        request.setControls(requestControls);

        SearchResponse response = new SearchResponse() {
            public void add(SearchResult result) throws Exception {

                DN dn = result.getDn();
                if (!dn.endsWith(source.getBaseDn())) return;

                if (debug) {
                    log.debug("Entry "+result.getDn()+" changed:");
                    Attributes attributes = result.getAttributes();
                    attributes.print();
                }

                super.add(result);
            }
        };

        client.search(request, response);

        response.waitFor();
        log.debug("Total changed: "+response.getTotalCount());

        Collection<Control> responseControls = response.getControls();
        for (Control control : responseControls) {
            if (!DirSyncResponseControl.OID.equals(control.getOid())) continue;

            log.debug("DirSyncResponseControl:");

            DirSyncResponseControl dirSyncResponseControl = new DirSyncResponseControl(control);
            cookie = dirSyncResponseControl.getCookie();

            log.debug(" - flag: "+dirSyncResponseControl.getFlag());
            log.debug(" - maxReturnLength: "+dirSyncResponseControl.getMaxReturnLength());
        }

        session.close();
    }
}
