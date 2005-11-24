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
package org.safehaus.penrose.config;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Source;
import org.safehaus.penrose.mapping.SourceDefinition;
import org.safehaus.penrose.schema.Schema;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class ConfigManager {

    Logger log = Logger.getLogger(ConfigManager.class);

    private ServerConfig serverConfig;
    private Schema schema;

    ConfigValidator configValidator;
    ConfigReader configReader;

    private Map configs = new TreeMap();

    public ConfigManager() {
    }

    public void init() {
        configValidator = new ConfigValidator();
        configValidator.setServerConfig(serverConfig);
        configValidator.setSchema(schema);

        configReader = new ConfigReader();
    }

    public void load(String path) throws Exception {

        Config config = configReader.read(path);

        Collection results = configValidator.validate(config);

        for (Iterator j=results.iterator(); j.hasNext(); ) {
            ConfigValidationResult result = (ConfigValidationResult)j.next();

            if (result.getType().equals(ConfigValidationResult.ERROR)) {
                log.error("ERROR: "+result.getMessage()+" ["+result.getSource()+"]");
            } else {
                log.warn("WARNING: "+result.getMessage()+" ["+result.getSource()+"]");
            }
        }

        for (Iterator i=config.getRootEntryDefinitions().iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            String ndn = schema.normalize(entryDefinition.getDn());
            configs.put(ndn, config);
        }

    }

    public ServerConfig getServerConfig() {
        return serverConfig;
    }

    public void setServerConfig(ServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Config getConfig(Source source) throws Exception {
        String connectionName = source.getConnectionName();
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            if (config.getConnectionConfig(connectionName) != null) return config;
        }
        return null;
    }

    public Config getConfig(SourceDefinition sourceDefinition) throws Exception {
        String connectionName = sourceDefinition.getConnectionName();
        for (Iterator i=configs.values().iterator(); i.hasNext(); ) {
            Config config = (Config)i.next();
            if (config.getConnectionConfig(connectionName) != null) return config;
        }
        return null;
    }

    public Config getConfig(EntryDefinition entryDefinition) throws Exception {
        return getConfig(entryDefinition.getDn());
    }

    public Config getConfig(String dn) throws Exception {
        String ndn = schema.normalize(dn);
        for (Iterator i=configs.keySet().iterator(); i.hasNext(); ) {
            String suffix = (String)i.next();
            if (ndn.endsWith(suffix)) return (Config)configs.get(suffix);
        }
        return null;
    }

    public Collection getConfigs() {
        return configs.values();
    }

}
