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
package org.safehaus.penrose.connector;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.commons.dbcp.ConnectionFactory;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.log4j.Logger;

import javax.naming.Context;
import javax.naming.directory.InitialDirContext;
import javax.sql.DataSource;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class ConnectionManager {

    public Logger log = Logger.getLogger(ConnectionManager.class);

    public Map connectionConfigs = new TreeMap();
    public Map connections = new TreeMap();

    public void addConnectionConfig(ConnectionConfig connectionConfig) {
        connectionConfigs.put(connectionConfig.getConnectionName(), connectionConfig);
    }

    public ConnectionConfig getConnectionConfig(String connectionName) {
        return (ConnectionConfig)connectionConfigs.get(connectionName);
    }

    public Collection getConnectionConfigs() {
        return connectionConfigs.values();
    }

    public ConnectionConfig removeConnectionConfig(String connectionName) {
        return (ConnectionConfig)connectionConfigs.remove(connectionName);
    }

    public void init() throws Exception {

        for (Iterator i=connectionConfigs.values().iterator(); i.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)i.next();
            String connectionName = connectionConfig.getConnectionName();
            log.debug("Initializing connection "+connectionName+".");

            if ("JDBC".equals(connectionConfig.getConnectionType())) {

                String driver = connectionConfig.getParameter("driver");
                String url = connectionConfig.getParameter("url");
                String user = connectionConfig.getParameter("user");
                String password = connectionConfig.getParameter("password");

                Class.forName(driver);

                Properties properties = new Properties();
                for (Iterator j=connectionConfig.getParameterNames().iterator(); j.hasNext(); ) {
                    String param = (String)j.next();
                    String value = connectionConfig.getParameter(param);
                    properties.setProperty(param, value);
                }

                GenericObjectPool connectionPool = new GenericObjectPool(null);
                ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, properties);

                PoolableConnectionFactory poolableConnectionFactory =
                        new PoolableConnectionFactory(
                                connectionFactory,
                                connectionPool,
                                null, // statement pool factory
                                null, // test query
                                false, // read only
                                true // auto commit
                        );

                DataSource ds = new PoolingDataSource(connectionPool);
                connections.put(connectionName, ds);

            } else if ("JNDI".equals(connectionConfig.getConnectionType())) {

                Properties env = new Properties();
                for (Iterator j=connectionConfig.getParameterNames().iterator(); j.hasNext(); ) {
                    String param = (String)j.next();
                    String value = connectionConfig.getParameter(param);

                    if (param.equals(Context.PROVIDER_URL)) {

                        int index = value.indexOf("://");
                        index = value.indexOf("/", index+3);
                        String suffix;
                        String ldapUrl;
                        if (index >= 0) { // extract suffix from url
                            suffix = value.substring(index+1);
                            ldapUrl = value.substring(0, index);
                        } else {
                            suffix = "";
                            ldapUrl = value;
                        }
                        env.put(param, ldapUrl);

                    } else {
                        env.put(param, value);
                    }
                }

                env.put("com.sun.jndi.ldap.connect.pool", "true");

                connections.put(connectionName, env);
            }
        }
    }

    public Object getConnection(String connectionName) throws Exception {
        ConnectionConfig connectionConfig = (ConnectionConfig)connectionConfigs.get(connectionName);

        Object object = connections.get(connectionName);
        if ("JDBC".equals(connectionConfig.getConnectionType())) {
            DataSource ds = (DataSource)object;
            return ds.getConnection();

        } else if ("JNDI".equals(connectionConfig.getConnectionType())) {
            Properties env = (Properties)object;
            return new InitialDirContext(env);
        }

        return null;
    }
}