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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.session.Session;
import org.safehaus.penrose.session.SessionManager;

import java.util.Collection;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author Endi S. Dewata
 */
public class PersistentEntryCache extends EntryCache {

    public final static String CONNECTION = "connection";

    String connectionName;

    public void init() throws Exception {
        super.init();

        connectionName = getParameter(CONNECTION);
    }

    public EntryCacheStorage createCacheStorage(Partition partition, EntryMapping entryMapping) throws Exception {

        EntryCacheStorage cacheStorage = new PersistentEntryCacheStorage();
        cacheStorage.setPenroseConfig(penroseConfig);
        cacheStorage.setPenroseContext(penroseContext);
        cacheStorage.setCacheConfig(getCacheConfig());
        cacheStorage.setPartition(partition);
        cacheStorage.setEntryMapping(entryMapping);

        cacheStorage.init();

        return cacheStorage;
    }

    public Connection getConnection() throws Exception {
        ConnectionManager connectionManager = penroseContext.getConnectionManager();
        return (Connection)connectionManager.openConnection((Partition)null, connectionName);
    }

    public void create(Partition partition) throws Exception {
        String sql = "create table "+
                partition.getName()+
                "_mappings (id integer auto_increment, dn varchar(255) unique, primary key (id))";

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        super.create(partition);
    }

    public void drop(Partition partition) throws Exception {
        super.drop(partition);

        String sql = "drop table "+partition.getName()+"_mappings";

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void load(Penrose penrose, Partition partition) throws Exception {

        SessionManager sessionManager = penroseContext.getSessionManager();
        Collection entryMappings = partition.getRootEntryMappings();

        for (Iterator i=entryMappings.iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();

            log.debug("Loading entries under "+entryMapping.getDn());

            Session adminSession = sessionManager.newSession();
            adminSession.setBindDn(penrose.getPenroseConfig().getRootDn());

            SearchRequest request = new SearchRequest();
            request.setDn(entryMapping.getDn());
            request.setFilter("(objectClass=*)");
            request.setScope(SearchRequest.SCOPE_SUB);

            SearchResponse response = new SearchResponse();

            adminSession.search(request, response);

            while (response.hasNext()) response.next();

            adminSession.close();
        }
    }
}
