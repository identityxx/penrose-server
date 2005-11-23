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

import org.safehaus.penrose.config.ServerConfigReader;
import org.safehaus.penrose.config.ServerConfig;
import org.safehaus.penrose.config.ConfigReader;
import org.safehaus.penrose.config.Config;
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
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.util.Formatter;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.DriverManager;

/**
 * @author Endi S. Dewata
 */
public class CacheManager {

    public Logger log = Logger.getLogger(CacheManager.class);

    public String homeDirectory;

    public ServerConfig serverConfig;

    public Schema schema;
    public Collection configs = new ArrayList();

    public ConnectionManager connectionManager;
    public String jdbcConnectionName;

    public Connector connector;
    public Engine engine;

    public CacheManager() throws Exception {
    }

    public void init() throws Exception {

        homeDirectory = System.getProperty("penrose.home");
        log.debug("PENROSE_HOME: "+homeDirectory);

        loadServerConfig();
        loadSchema();
        loadConfigs();

        initConnections();
        initConnector();
        initEngine();

        EngineConfig engineConfig = serverConfig.getEngineConfig();
        CacheConfig cacheConfig = engineConfig.getCacheConfig();
        jdbcConnectionName = cacheConfig.getParameter("jdbcConnection");
    }

    public void loadServerConfig() throws Exception {
        ServerConfigReader serverConfigReader = new ServerConfigReader();
        serverConfig = serverConfigReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf"+File.separator+"server.xml");
    }

    public void loadSchema() throws Exception {
        SchemaReader reader = new SchemaReader();
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema");
        reader.readDirectory((homeDirectory == null ? "" : homeDirectory+File.separator)+"schema"+File.separator+"ext");
        schema = reader.getSchema();
    }

    public void loadConfigs() throws Exception {
        ConfigReader configReader = new ConfigReader();
        Config config = configReader.read((homeDirectory == null ? "" : homeDirectory+File.separator)+"conf");

        configs.add(config);

        File partitions = new File(homeDirectory+File.separator+"partitions");
        if (partitions.exists()) {
            File files[] = partitions.listFiles();
            for (int i=0; i<files.length; i++) {
                File partition = files[i];
                String name = partition.getName();

                config = configReader.read(partition.getAbsolutePath());
                configs.add(config);
            }
        }
    }

    public void initConnections() throws Exception {
        connectionManager = new ConnectionManager();

        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();

            Collection connectionConfigs = config.getConnectionConfigs();
            for (Iterator j=connectionConfigs.iterator(); j.hasNext(); ) {
                ConnectionConfig connectionConfig = (ConnectionConfig)j.next();
                connectionManager.addConnectionConfig(connectionConfig);
            }
        }

        connectionManager.init();
    }

    public void initConnector() throws Exception {

        ConnectorConfig connectorCfg = serverConfig.getConnectorConfig();

        connector = new Connector();
        connector.setServerConfig(serverConfig);
        connector.setConnectionManager(connectionManager);
        connector.init(connectorCfg);

        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            connector.addConfig(config);
        }
    }

    public void initEngine() throws Exception {

        InterpreterConfig interpreterConfig = serverConfig.getInterpreterConfig();
        InterpreterFactory intFactory = new InterpreterFactory(interpreterConfig);

        EngineConfig engineCfg = serverConfig.getEngineConfig();
        engine = new Engine();
        engine.setInterpreterFactory(intFactory);
        engine.setSchema(schema);
        engine.setConnector(connector);
        engine.setConnectionManager(connectionManager);
        engine.init(engineCfg);

        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            engine.addConfig(config);
        }
    }

    public void create() throws Exception {
        connector.create();

        createMappingsTable();

        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            create(config, null, entryDefinitions);
        }
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

    public void create(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            log.debug("Creating tables for "+entryDefinition.getDn());
            EngineCache cache = engine.getCache(parentDn, entryDefinition);
            cache.create();

            Collection children = config.getChildren(entryDefinition);
            create(config, entryDefinition.getDn(), children);
        }
    }

    public void load() throws Exception {
        connector.load();

        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            load(config, null, entryDefinitions);
        }
    }

    public void load(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            if (entryDefinition.isDynamic()) continue;

            log.debug("Loading entries under "+entryDefinition.getDn());
            EngineCache cache = engine.getCache(parentDn, entryDefinition);
            cache.load();
        }
    }

    public void clean() throws Exception {
        connector.clean();

        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            clean(config, null, entryDefinitions);
        }
    }

    public void clean(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            if (entryDefinition.isDynamic()) continue;

            log.debug("Cleaning tables for "+entryDefinition.getDn());
            EngineCache cache = engine.getCache(parentDn, entryDefinition);
            cache.clean();
        }
    }

    public void drop() throws Exception {

        for (Iterator i=configs.iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            Collection entryDefinitions = config.getRootEntryDefinitions();
            drop(config, null, entryDefinitions);
        }

        dropMappingsTable();

        connector.drop();
    }

    public void drop(Config config, String parentDn, Collection entryDefinitions) throws Exception {
        if (entryDefinitions == null) return;
        for (Iterator i=entryDefinitions.iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();

            log.debug("Deleting entries under "+entryDefinition.getDn());
            EngineCache cache = engine.getCache(parentDn, entryDefinition);
            cache.drop();

            Collection children = config.getChildren(entryDefinition);
            drop(config, entryDefinition.getDn(), children);
        }
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
            log.error(e.getMessage(), e);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void start() throws Exception {
        connector.start();
        engine.start();
    }

    public void stop() throws Exception {
        engine.stop();
        connector.stop();
    }

    public static void main(String args[]) throws Exception {

        if (args.length == 0) {
            System.out.println("Usage: org.safehaus.penrose.cache.CacheManager [command]");
            System.out.println();
            System.out.println("Commands:");
            System.out.println("    create - create cache tables");
            System.out.println("    load   - load data into cache tables");
            System.out.println("    clean  - clean data from cache tables");
            System.out.println("    drop   - drop cache tables");
            System.exit(0);
        }

        String command = args[0];

        CacheManager cacheManager = new CacheManager();
        cacheManager.init();

        if ("create".equals(command)) {
            cacheManager.create();

        } else if ("load".equals(command)) {
            cacheManager.load();

        } else if ("clean".equals(command)) {
            cacheManager.clean();

        } else if ("drop".equals(command)) {
            cacheManager.drop();

        } else if ("run".equals(command)) {
            cacheManager.start();
        }

        cacheManager.stop();
    }

}
