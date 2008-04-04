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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleConfigManager;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.source.SourceConfigManager;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionConfigManager;
import org.safehaus.penrose.directory.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.ArrayList;
import java.util.TreeSet;

/**
 * @author Endi S. Dewata
 */
public class PartitionValidator {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public PartitionValidator() {
    }

    public Collection<PartitionValidationResult> validate(PartitionConfig partitionConfig) throws Exception {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        results.addAll(validate(partitionConfig, partitionConfig.getConnectionConfigManager()));
        results.addAll(validate(partitionConfig, partitionConfig.getSourceConfigManager()));
        results.addAll(validate(partitionConfig, partitionConfig.getDirectoryConfig()));
        results.addAll(validateModuleConfigs(partitionConfig, partitionConfig.getModuleConfigManager()));

        return results;
    }

    public Collection<PartitionValidationResult> validate(PartitionConfig partitionConfig, ConnectionConfigManager connections) throws Exception {

        String name = partitionConfig.getName();

        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        for (ConnectionConfig connectionConfig : connections.getConnectionConfigs()) {
            //log.debug("Validating connection "+connectionConfig.getName());

            String connectionName = connectionConfig.getName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing connection name.", name + ":", connectionConfig));
                continue;
            }

            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null || "".equals(adapterName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing adapter name.", name + ": " + connectionName, connectionConfig));

            } else if (penroseConfig != null) {
                AdapterConfig adapterConfig = partitionConfig.getAdapterConfig(adapterName);

                if (adapterConfig == null) {
                    adapterConfig = penroseConfig.getAdapterConfig(adapterName);
                }

                if (adapterConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid adapter name: " + adapterName, name + ": " + connectionName, connectionConfig));
                }
            }
        }

        return results;
    }

    public Collection<PartitionValidationResult> validate(PartitionConfig partitionConfig, SourceConfigManager sources) throws Exception {

        String name = partitionConfig.getName();

        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        for (SourceConfig sourceConfig : sources.getSourceConfigs()) {

            String sourceName = sourceConfig.getName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source name.", name + ":", sourceConfig));
                continue;
            }

            String connectionName = sourceConfig.getConnectionName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing connection name.", name + ": " + sourceName, sourceConfig));
                continue;
            }

            ConnectionConfig connectionConfig = partitionConfig.getConnectionConfigManager().getConnectionConfig(connectionName);
            if (connectionConfig == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid connection name: " + connectionName, name + ": " + sourceName, sourceConfig));
            }

            for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {

                String fieldName = fieldConfig.getName();
                if (fieldName == null || "".equals(fieldName)) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing field name.", name + ": " + sourceName, sourceConfig));
                }
            }

            if (!sourceConfig.getFieldConfigs().isEmpty() && sourceConfig.getPrimaryKeyFieldConfigs().isEmpty()) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing primary key(s).", name + ": " + sourceName, sourceConfig));
            }
        }

        return results;
    }

    public Collection<PartitionValidationResult> validate(PartitionConfig partitionConfig, DirectoryConfig mappings) throws Exception {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        for (EntryConfig entryConfig : mappings.getEntryConfigs()) {
            //log.debug("Validating entry "+entryConfig;

            DN dn = entryConfig.getDn();
            if (dn.isEmpty()) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing DN.", entryConfig.getDn(), entryConfig));
            }

            results.addAll(validateObjectClasses(partitionConfig, mappings, entryConfig));
            results.addAll(validateAttributeMappings(partitionConfig, mappings, entryConfig));
            results.addAll(validateSourceMappings(partitionConfig, mappings, entryConfig));
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateObjectClasses(PartitionConfig partitionConfig, DirectoryConfig mappings, EntryConfig entryConfig) {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        //log.debug("Validating entry "+entryConfig"'s object classes");

        Collection<String> missingObjectClasses = new TreeSet<String>();

        Collection<String> objectClasses = entryConfig.getObjectClasses();
        //System.out.println("Checking "+entryConfig" object classes "+objectClasses);

        SchemaManager schemaManager = penroseContext.getSchemaManager();

        for (String ocName : objectClasses) {

            ObjectClass objectClass = schemaManager.getObjectClass(ocName);
            if (objectClass == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Object class " + ocName + " is not defined in the schema.", entryConfig.getDn(), entryConfig));
            }

            Collection<String> scNames = schemaManager.getAllObjectClassNames(ocName);
            for (String scName : scNames) {
                if ("top".equals(scName)) continue;

                if (!objectClasses.contains(scName)) {
                    //System.out.println(" - ["+scName+"] not found in "+objectClasses);
                    missingObjectClasses.add(scName);
                }
            }
        }

        for (String scName : missingObjectClasses) {
            results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Missing object class " + scName + ".", entryConfig.getDn(), entryConfig));
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateAttributeMappings(PartitionConfig partitionConfig, DirectoryConfig mappings, EntryConfig entryConfig) {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        //log.debug("Validating entry "+entryConfig"'s attributes");
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        for (AttributeMapping attributeMapping : entryConfig.getAttributeMappings()) {

            String name = attributeMapping.getName();
            if (name == null || "".equals(name)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing attribute name.", entryConfig.getDn(), entryConfig));
                continue;
            }
/*
            AttributeType attributeType = schema.getAttributeType(name);
            if (attributeType == null) {
                results.add(new ConfigValidationResult(ConfigValidationResult.WARNING, "Attribute type "+name+" is not defined in the schema.", entryConfig       Expression expression = attributeMapping.getExpression();
            if (expression != null) {
                String foreach = expression.getForeach();
                String var = expression.getVar();
                if (foreach != null && (var == null || "".equals(var))) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing variable name.", entryConfig.getDn()+"/"+name, entryConfig));
                }
            }
*/

            if (attributeMapping.getVariable() != null) {
                String variable = attributeMapping.getVariable();

                int j = variable.indexOf(".");
                String sourceAlias = variable.substring(0, j);
                String fieldName = variable.substring(j + 1);

                SourceMapping sourceMapping = entryConfig.getSourceMapping(sourceAlias);
                if (sourceMapping == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Unknown source mapping: " + sourceAlias, entryConfig.getDn(), entryConfig));
                    continue;
                }

                SourceConfig sourceConfig = partitionConfig.getSourceConfigManager().getSourceConfig(sourceMapping.getSourceName());
                if (sourceConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Unknown source: " + sourceMapping.getSourceName(), entryConfig.getDn(), entryConfig));
                    continue;
                }

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                if (fieldConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Unknown field: " + variable, entryConfig.getDn(), entryConfig));
                }
            }
        }

        if (!entryConfig.getAttributeMappings().isEmpty() && entryConfig.getRdnAttributeMappings().isEmpty()) {
            results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing rdn attribute(s).", entryConfig.getDn(), entryConfig));
        }

        Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(entryConfig);
        for (ObjectClass objectClass : objectClasses) {

            Collection<String> requiredAttributes = objectClass.getRequiredAttributes();
            for (String atName : requiredAttributes) {

                Collection attributeMappings = entryConfig.getAttributeMappings(atName);
                if (attributeMappings == null || attributeMappings.isEmpty()) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Attribute " + atName + " is required by " + objectClass.getName() + " object class.", entryConfig.getDn(), entryConfig));
                }
            }
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateSourceMappings(PartitionConfig partitionConfig, DirectoryConfig mappings, EntryConfig entryConfig) {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        for (SourceMapping sourceMapping : entryConfig.getSourceMappings()) {
            //log.debug("Validating entry "+entryConfig"'s sourceMapping "+sourceMapping.getName());

            String alias = sourceMapping.getName();
            if (alias == null || "".equals(alias)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source alias.", entryConfig.getDn(), entryConfig));
                continue;
            }

            String sourceName = sourceMapping.getSourceName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source name.", entryConfig.getDn() + "/" + alias, entryConfig));
            }

            SourceConfig sourceConfig = partitionConfig.getSourceConfigManager().getSourceConfig(sourceName);
            if (sourceConfig == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid source name: " + sourceName, entryConfig.getDn() + "/" + alias, entryConfig));
                continue;

            }

            for (FieldMapping fieldMapping : sourceMapping.getFieldMappings()) {
                //log.debug("Validating entry "+entryConfig"'s fieldMapping "+source.getName()+"."+fieldMapping.getName());

                String fieldName = fieldMapping.getName();
                if (fieldName == null || "".equals(fieldName)) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing field name.", entryConfig.getDn() + "/" + alias, entryConfig));
                    continue;
                }

                Expression expression = fieldMapping.getExpression();
                if (expression != null) {
                    String foreach = expression.getForeach();
                    String var = expression.getVar();
                    if (foreach != null && (var == null || "".equals(var))) {
                        results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing variable name.", entryConfig.getDn() + "/" + alias + "." + fieldName, entryConfig));
                    }
                }

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                if (fieldConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid field name: " + fieldName, entryConfig.getDn() + "/" + alias + "." + fieldName, entryConfig));
                }
            }
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateModuleConfigs(PartitionConfig partitionConfig, ModuleConfigManager modules) throws Exception {

        String name = partitionConfig.getName();

        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        for (ModuleConfig moduleConfig : modules.getModuleConfigs()) {
            //log.debug("Validating module "+moduleConfig.getName());

            String moduleName = moduleConfig.getName();
            if (moduleName == null || "".equals(moduleName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing module name.", name + ":", moduleConfig));
            }

            String moduleClass = moduleConfig.getModuleClass();
            if (moduleClass == null || "".equals(moduleClass)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing module class name.", name + ": " + moduleName, moduleConfig));
            }
/*
            try {
                ClassLoader cl = partition.getClassLoader();
                cl.loadClass(moduleClass);
            } catch (Exception e) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Module class not found: " + moduleClass, partition.getName() + ": " + moduleName, moduleConfig));
            }
*/
        }

        return results;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public PenroseContext getPenroseContext() {
        return penroseContext;
    }

    public void setPenroseContext(PenroseContext penroseContext) {
        this.penroseContext = penroseContext;
    }
}
