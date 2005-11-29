/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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

import org.safehaus.penrose.config.*;
import org.safehaus.penrose.interpreter.InterpreterConfig;
import org.safehaus.penrose.interpreter.InterpreterFactory;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.SchemaReader;
import org.safehaus.penrose.connector.Connector;
import org.safehaus.penrose.connector.ConnectorConfig;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.connector.ConnectionConfig;
import org.safehaus.penrose.engine.Engine;
import org.safehaus.penrose.engine.EngineConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.handler.Handler;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.apache.log4j.Logger;
import org.ietf.ldap.LDAPConnection;
import org.ietf.ldap.LDAPSearchConstraints;

import java.io.File;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @author Endi S. Dewata
 */
public class CacheManager {

    public Logger log = Logger.getLogger(CacheManager.class);

    public String homeDirectory;

    public PenroseConfig penroseConfig;
    public ConnectionManager connectionManager;
    public String jdbcConnectionName;

    public CacheManager() throws Exception {
    }

    public void init() throws Exception {

        homeDirectory = System.getProperty("penrose.home");
        log.debug("Home: "+homeDirectory);

        CacheConfig cacheConfig = penroseConfig.getEntryCacheConfig();
        jdbcConnectionName = cacheConfig.getParameter("jdbcConnection");
    }

    public void createMappingsTable() throws Exception {
        String sql = "create table penrose_mappings (id integer auto_increment, dn varchar(255) unique, primary key (id))";

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
            //log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public Connection getConnection() throws Exception {
        return (Connection)connectionManager.getConnection(jdbcConnectionName);
    }

    public void dropMappingsTable() throws Exception {
        String sql = "drop table penrose_mappings";

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
}
