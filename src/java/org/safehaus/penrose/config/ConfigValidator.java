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

import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connection.AdapterConfig;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;
import org.ietf.ldap.LDAPDN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author Endi S. Dewata
 */
public class ConfigValidator {

    Logger log = LoggerFactory.getLogger(getClass());

    private ServerConfig serverConfig;
    private Schema schema;

    public ConfigValidator() {
    }

    public Collection validate(Config config) throws Exception {
        Collection results = new ArrayList();

        results.addAll(validateSources(config));
        results.addAll(validateEntries(config));
        results.addAll(validateModules(config));

        return results;
    }

    public Collection validateSources(Config config) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=config.getConnectionConfigs().iterator(); i.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)i.next();
            //log.debug("Validating connection "+connectionConfig.getConnectionName());

            String connectionName = connectionConfig.getConnectionName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing connection name.", null, connectionConfig));
                continue;
            }

            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null || "".equals(adapterName)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing adapter name.", connectionName, connectionConfig));

            } else if (serverConfig != null) {
                AdapterConfig adapterConfig = serverConfig.getAdapterConfig(adapterName);
                if (adapterConfig == null) {
                    results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Invalid adapter name: "+adapterName, connectionName, connectionConfig));
                }
            }

            for (Iterator j=connectionConfig.getSourceDefinitions().iterator(); j.hasNext(); ) {
                SourceDefinition sourceDefinition = (SourceDefinition)j.next();
                //log.debug("Validating source "+connectionConfig.getConnectionName()+"/"+sourceDefinition.getName());

                String sourceName = sourceDefinition.getName();
                if (sourceName == null || "".equals(sourceName)) {
                    results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing source name.", connectionName, connectionConfig));
                    continue;
                }

                String conName = sourceDefinition.getConnectionName();
                if (conName == null || "".equals(conName)) {
                    results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing connection name.", connectionName+"/"+sourceName, sourceDefinition));

                } else if (!conName.equals(connectionName)) {
                    results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Invalid connection name: "+conName, connectionName+"/"+sourceName, sourceDefinition));
                }

                boolean hasPrimaryKey = false;

                for (Iterator k=sourceDefinition.getFieldDefinitions().iterator(); k.hasNext(); ) {
                    FieldDefinition fieldDefinition = (FieldDefinition)k.next();
                    //log.debug("Validating field "+connectionConfig.getConnectionName()+"/"+sourceDefinition.getName()+"."+fieldDefinition.getName());

                    String fieldName = fieldDefinition.getName();
                    if (fieldName == null || "".equals(fieldName)) {
                        results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing field name.", connectionName+"/"+sourceName, sourceDefinition));
                    }

                    hasPrimaryKey |= fieldDefinition.isPrimaryKey();
                }

                if (!hasPrimaryKey) {
                    results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing primary key.", connectionName+"/"+sourceName, sourceDefinition));
                }
            }
        }

        return results;
    }

    public Collection validateEntries(Config config) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=config.getEntryDefinitions().iterator(); i.hasNext(); ) {
            EntryDefinition entryDefinition = (EntryDefinition)i.next();
            //log.debug("Validating entry "+entryDefinition.getDn());

            String rdn = entryDefinition.getRdn();
            if (rdn == null || "".equals(rdn)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing RDN.", entryDefinition.getDn(), entryDefinition));

            } else if (!LDAPDN.isValid(rdn)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Invalid RDN: "+rdn, entryDefinition.getDn(), entryDefinition));
            }

            String parentDn = entryDefinition.getParentDn();
            if (parentDn != null && !LDAPDN.isValid(parentDn)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Invalid parent DN: "+parentDn, entryDefinition.getDn(), entryDefinition));
            }

            results.addAll(validateEntryObjectClasses(config, entryDefinition));
            results.addAll(validateEntryAttributeTypes(config, entryDefinition));
            results.addAll(validateEntrySources(config, entryDefinition));
        }

        return results;
    }

    public Collection validateEntryObjectClasses(Config config, EntryDefinition entryDefinition) {
        Collection results = new ArrayList();

        if (schema == null) return results;

        //log.debug("Validating entry "+entryDefinition.getDn()+"'s object classes");

        Collection missingObjectClasses = new TreeSet();

        Collection objectClasses = entryDefinition.getObjectClasses();
        //System.out.println("Checking "+entryDefinition.getDn()+" object classes "+objectClasses);

        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();

            ObjectClass objectClass = schema.getObjectClass(ocName);
            if (objectClass == null) {
                results.add(new ConfigValidationResult(ConfigValidationResult.WARNING, "Object class "+ocName+" is not defined in the schema.", entryDefinition.getDn(), entryDefinition));
            }

            Collection scNames = schema.getAllObjectClassNames(ocName);
            for (Iterator j=scNames.iterator(); j.hasNext(); ) {
                String scName = (String)j.next();
                if (!objectClasses.contains(scName)) {
                    //System.out.println(" - ["+scName+"] not found in "+objectClasses);
                    missingObjectClasses.add(scName);
                }
            }
        }

        for (Iterator i=missingObjectClasses.iterator(); i.hasNext(); ) {
            String scName = (String)i.next();
            results.add(new ConfigValidationResult(ConfigValidationResult.WARNING, "Missing object class "+scName+".", entryDefinition.getDn(), entryDefinition));
        }

        return results;
    }

    public Collection validateEntryAttributeTypes(Config config, EntryDefinition entryDefinition) {
        Collection results = new ArrayList();

        if (schema == null) return results;

        //log.debug("Validating entry "+entryDefinition.getDn()+"'s attributes");

        for (Iterator i=entryDefinition.getAttributeDefinitions().iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            String name = attributeDefinition.getName();
            if (name == null || "".equals(name)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing attribute name.", entryDefinition.getDn(), entryDefinition));
                continue;
            }
/*
            AttributeType attributeType = schema.getAttributeType(name);
            if (attributeType == null) {
                results.add(new ConfigValidationResult(ConfigValidationResult.WARNING, "Attribute type "+name+" is not defined in the schema.", entryDefinition.getDn(), entryDefinition));
            }
*/
        }

        Collection objectClasses = schema.getObjectClasses(entryDefinition);
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            ObjectClass objectClass = (ObjectClass)i.next();

            Collection requiredAttributes = objectClass.getRequiredAttributes();
            for (Iterator j=requiredAttributes.iterator(); j.hasNext(); ) {
                String atName = (String)j.next();

                AttributeDefinition attributeDefinition = entryDefinition.getAttributeDefinition(atName);
                if (attributeDefinition == null) {
                    results.add(new ConfigValidationResult(ConfigValidationResult.WARNING, "Attribute "+atName+" is required by "+objectClass.getName()+" object class.", entryDefinition.getDn(), entryDefinition));
                }
            }
        }
        return results;
    }

    public Collection validateEntrySources(Config config, EntryDefinition entryDefinition) {
        Collection results = new ArrayList();

        for (Iterator i=entryDefinition.getSources().iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            //log.debug("Validating entry "+entryDefinition.getDn()+"'s source "+source.getName());

            String alias = source.getName();
            if (alias == null || "".equals(alias)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing source alias.", entryDefinition.getDn(), entryDefinition));
                continue;
            }

            String sourceName = source.getSourceName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing source name.", entryDefinition.getDn()+"/"+alias, entryDefinition));
            }

            String connectionName = source.getConnectionName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing connection name.", entryDefinition.getDn()+"/"+alias, entryDefinition));

            } else {
                ConnectionConfig connectionConfig = config.getConnectionConfig(connectionName);
                if (connectionConfig == null) {
                    results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Invalid connection name: "+connectionConfig, entryDefinition.getDn()+"/"+alias, entryDefinition));

                } else if (sourceName != null && !"".equals(sourceName)) {
                    SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(sourceName);

                    if (sourceDefinition == null) {
                        results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Invalid source name: "+sourceName, entryDefinition.getDn()+"/"+alias, entryDefinition));

                    } else {
                        for (Iterator j=source.getFields().iterator(); j.hasNext(); ) {
                            Field field = (Field)j.next();
                            //log.debug("Validating entry "+entryDefinition.getDn()+"'s field "+source.getName()+"."+field.getName());

                            String fieldName = field.getName();
                            if (fieldName == null || "".equals(fieldName)) {
                                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing field name.", entryDefinition.getDn()+"/"+alias, entryDefinition));
                                continue;
                            }

                            Expression expression = field.getExpression();
                            if (expression != null) {
                                String foreach = expression.getForeach();
                                String var = expression.getVar();
                                if (foreach != null && (var == null || "".equals(var))) {
                                    results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing variable name.", entryDefinition.getDn()+"/"+alias+"."+fieldName, entryDefinition));
                                }
                            }

                            FieldDefinition fieldDefinition = sourceDefinition.getFieldDefinition(fieldName);
                            if (fieldDefinition == null) {
                                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Invalid field name: "+fieldName, entryDefinition.getDn()+"/"+alias+"."+fieldName, entryDefinition));
                            }
                        }
                    }
                }
            }
        }

        return results;
    }

    public Collection validateModules(Config config) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=config.getModuleConfigs().iterator(); i.hasNext(); ) {
            ModuleConfig moduleConfig = (ModuleConfig)i.next();
            //log.debug("Validating module "+moduleConfig.getModuleName());

            String moduleName = moduleConfig.getModuleName();
            if (moduleName == null || "".equals(moduleName)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing module name.", null, moduleConfig));
            }

            String moduleClass = moduleConfig.getModuleClass();
            if (moduleClass == null || "".equals(moduleClass)) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Missing module class name.", moduleName, moduleConfig));
            }

            try {
                Class.forName(moduleClass);
            } catch (Exception e) {
                results.add(new ConfigValidationResult(ConfigValidationResult.ERROR, "Module class not found: "+moduleClass, moduleName, moduleConfig));
            }
        }

        return results;
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
}
