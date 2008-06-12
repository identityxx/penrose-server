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
package org.safehaus.penrose.etrust;

import org.safehaus.penrose.module.Module;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.PenroseFactory;
import org.safehaus.penrose.directory.Directory;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.ldap.connection.LDAPConnection;

import javax.naming.Context;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi S. Dewata
 */
public class ETrustDirectoryModule extends Module {

    public final static String HOME       = "home";
    public final static String SERVER     = "server";
    public final static String CONNECTION = "connection";
    public final static String INTERVAL   = "interval";

    public final static int DEFAULT_INTERVAL = 5; // seconds
    public String home;
    public String server;
    public String connection;
    public int interval; // 1 second

    public ETrustDirectoryRunnable runnable;

    SimpleDateFormat dateFormat1 = new SimpleDateFormat("yyyyMMdd");
    SimpleDateFormat dateFormat2 = new SimpleDateFormat("yyyyMMdd.HHmmss.SSS");

    public void init() throws Exception {

        log.debug("Initializing ETrustDirectoryModule");

        home = getParameter(HOME);
        if (home == null) {
            throw new Exception("Missing home parameter.");
        } else {
            log.debug("Home: "+home);
        }

        server = getParameter(SERVER);
        if (server == null) {
            throw new Exception("Missing server parameter.");
        } else {
            log.debug("Server: "+server);
        }

        connection = getParameter(CONNECTION);
        if (connection == null) {
            throw new Exception("Missing connection parameter.");
        } else {
            log.debug("Connection: "+connection);
        }

        String s = getParameter(INTERVAL);
        interval = s == null ? DEFAULT_INTERVAL : Integer.parseInt(s);
        log.debug("Interval: "+interval);

        runnable = new ETrustDirectoryRunnable(this, server);

        Thread thread = new Thread(runnable);
        thread.start();
    }

    public void destroy() throws Exception {
        runnable.stop();
    }

    public void process(Date date, String user, String action, String data) throws Exception {
        log.debug("["+date+"] \""+user+"\" "+action);

        ConnectionManager connectionManager = partition.getConnectionManager();
        LDAPConnection con = (LDAPConnection)connectionManager.getConnection(connection);

        ConnectionConfig connectionConfig = con.getConnectionConfig();
        String penroseUser = connectionConfig.getParameter(Context.SECURITY_PRINCIPAL);

        if (user != null && user.equals(penroseUser)) {
            log.debug("Skip changes made by "+user);
            return;
        }

        Session adminSession = createAdminSession();

        try {

            if ("ADD".equals(action)) {
                int p = data.indexOf('\"', 1);
                final String dn = data.substring(1, p);
                String attributeNames = data.substring(p+2);

                log.debug(" - dn: "+dn);
                log.debug(" - attributes: "+attributeNames);

                Directory directory = partition.getDirectory();
                Collection<Entry> entries = directory.findEntries(dn);
                if (entries == null || entries.isEmpty()) return;
    
                LDAPClient client = null;
                try {
                    client = con.createClient();

                    SearchRequest request = new SearchRequest();
                    request.setDn(dn);
                    request.setScope(SearchRequest.SCOPE_BASE);

                    SearchResponse response = new SearchResponse();

                    client.search(request, response);

                    if (!response.hasNext()) return;

                    SearchResult result = response.next();
                    adminSession.add(dn, result.getAttributes());

                } catch (Exception e) {
                    log.error(e.getMessage(), e);

                } finally {
                    if (client != null) try { client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
                }

            } else if ("REM".equals(action)) {
                String dn = data.substring(1, data.length()-1);

                log.debug(" - dn: "+dn);

                Directory directory = partition.getDirectory();
                Collection<Entry> entries = directory.findEntries(dn);
                if (entries == null || entries.isEmpty()) return;

                try {
                    adminSession.delete(dn);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

            } else if ("MOD".equals(action)) {
                int p = data.indexOf('\"', 1);
                String dn = data.substring(1, p);
                String attributeNames = data.substring(p+2);

                log.debug(" - dn: "+dn);
                log.debug(" - attributes: "+attributeNames);

                Directory directory = partition.getDirectory();
                Collection<Entry> entries = directory.findEntries(dn);
                if (entries == null || entries.isEmpty()) return;

                LDAPClient client = null;
                try {
                    client = con.createClient();

                    SearchRequest request = new SearchRequest();
                    request.setDn(dn);
                    request.setScope(SearchRequest.SCOPE_BASE);

                    SearchResponse response = new SearchResponse();

                    client.search(request, response);

                    if (!response.hasNext()) return;

                    SearchResult result = response.next();
                    Attributes attributes = result.getAttributes();

                    Collection<Modification> modifications = new ArrayList<Modification>();

                    for (Attribute attribute : attributes.getAll()) {
                        if (attribute.getName().equalsIgnoreCase("objectClass")) continue;
                        Modification modification = new Modification(Modification.REPLACE, attribute);
                        modifications.add(modification);
                    }

                    adminSession.modify(dn, modifications);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);

                } finally {
                    if (client != null) try { client.close(); } catch (Exception e) { log.error(e.getMessage(), e); }
                }

            } else if ("MODDN".equals(action)) {

                int p = data.indexOf('\"', 1);
                String dn = data.substring(1, p);

                int q = data.indexOf('\"', p+1);
                int r = data.indexOf('\"', q+1);
                String newRdn = data.substring(q+1, r);

                log.debug(" - dn: "+dn);
                log.debug(" - newRdn: "+newRdn);

                try {
                    adminSession.modrdn(dn, newRdn, true);

                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }

        } finally {
            adminSession.close();
        }
    }
}
