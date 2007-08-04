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
import org.safehaus.penrose.module.ModuleConfigs;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.source.SourceConfigs;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionConfigs;
import org.safehaus.penrose.directory.DirectoryConfigs;
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

        results.addAll(validate(partitionConfig, partitionConfig.getConnectionConfigs()));
        results.addAll(validate(partitionConfig, partitionConfig.getSourceConfigs()));
        results.addAll(validate(partitionConfig, partitionConfig.getDirectoryConfigs()));
        results.addAll(validateModuleConfigs(partitionConfig, partitionConfig.getModuleConfigs()));

        return results;
    }

    public Collection<PartitionValidationResult> validate(PartitionConfig partitionConfig, ConnectionConfigs connections) throws Exception {

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

    public Collection<PartitionValidationResult> validate(PartitionConfig partitionConfig, SourceConfigs sources) throws Exception {

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

            ConnectionConfig connectionConfig = partitionConfig.getConnectionConfigs().getConnectionConfig(connectionName);
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

    public Collection<PartitionValidationResult> validate(PartitionConfig partitionConfig, DirectoryConfigs mappings) throws Exception {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        for (EntryMapping entryMapping : mappings.getEntryMappings()) {
            //log.debug("Validating entry "+entryMapping;

            DN dn = entryMapping.getDn();
            if (dn.isEmpty()) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing DN.", entryMapping.getDn(), entryMapping));
            }

            results.addAll(validateObjectClasses(partitionConfig, mappings, entryMapping));
            results.addAll(validateAttributeMappings(partitionConfig, mappings, entryMapping));
            results.addAll(validateSourceMappings(partitionConfig, mappings, entryMapping));
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateObjectClasses(PartitionConfig partitionConfig, DirectoryConfigs mappings, EntryMapping entryMapping) {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        //log.debug("Validating entry "+entryMapping"'s object classes");

        Collection<String> missingObjectClasses = new TreeSet<String>();

        Collection<String> objectClasses = entryMapping.getObjectClasses();
        //System.out.println("Checking "+entryMapping" object classes "+objectClasses);

        SchemaManager schemaManager = penroseContext.getSchemaManager();

        for (String ocName : objectClasses) {

            ObjectClass objectClass = schemaManager.getObjectClass(ocName);
            if (objectClass == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Object class " + ocName + " is not defined in the schema.", entryMapping.getDn(), entryMapping));
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
            results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Missing object class " + scName + ".", entryMapping.getDn(), entryMapping));
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateAttributeMappings(PartitionConfig partitionConfig, DirectoryConfigs mappings, EntryMapping entryMapping) {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        //log.debug("Validating entry "+entryMapping"'s attributes");
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        for (AttributeMapping attributeMapping : entryMapping.getAttributeMappings()) {

            String name = attributeMapping.getName();
            if (name == null || "".equals(name)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing attribute name.", entryMapping.getDn(), entryMapping));
                continue;
            }
/*
            AttributeType attributeType = schema.getAttributeType(name);
            if (attributeType == null) {
                results.add(new ConfigValidationResult(ConfigValidationResult.WARNING, "Attribute type "+name+" is not defined in the schema.", entryMapping       Expression expression = attributeMapping.getExpression();
            if (expression != null) {
                String foreach = expression.getForeach();
                String var = expression.getVar();
                if (foreach != null && (var == null || "".equals(var))) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing variable name.", entryMapping.getDn()+"/"+name, entryMapping));
                }
            }
*/

            if (attributeMapping.getVariable() != null) {
                String variable = attributeMapping.getVariable();

                int j = variable.indexOf(".");
                String sourceAlias = variable.substring(0, j);
                String fieldName = variable.substring(j + 1);

                SourceMapping sourceMapping = entryMapping.getSourceMapping(sourceAlias);
                if (sourceMapping == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Unknown source mapping: " + sourceAlias, entryMapping.getDn(), entryMapping));
                    continue;
                }

                SourceConfig sourceConfig = partitionConfig.getSourceConfigs().getSourceConfig(sourceMapping.getSourceName());
                if (sourceConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Unknown source: " + sourceMapping.getSourceName(), entryMapping.getDn(), entryMapping));
                    continue;
                }

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                if (fieldConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Unknown field: " + variable, entryMapping.getDn(), entryMapping));
                }
            }
        }

        if (!entryMapping.getAttributeMappings().isEmpty() && entryMapping.getRdnAttributeMappings().isEmpty()) {
            results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing rdn attribute(s).", entryMapping.getDn(), entryMapping));
        }

        Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(entryMapping);
        for (ObjectClass objectClass : objectClasses) {

            Collection<String> requiredAttributes = objectClass.getRequiredAttributes();
            for (String atName : requiredAttributes) {

                Collection attributeMappings = entryMapping.getAttributeMappings(atName);
                if (attributeMappings == null || attributeMappings.isEmpty()) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Attribute " + atName + " is required by " + objectClass.getName() + " object class.", entryMapping.getDn(), entryMapping));
                }
            }
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateSourceMappings(PartitionConfig partitionConfig, DirectoryConfigs mappings, EntryMapping entryMapping) {
        Collection<PartitionValidationResult> results = new ArrayList<PartitionValidationResult>();

        for (SourceMapping sourceMapping : entryMapping.getSourceMappings()) {
            //log.debug("Validating entry "+entryMapping"'s sourceMapping "+sourceMapping.getName());

            String alias = sourceMapping.getName();
            if (alias == null || "".equals(alias)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source alias.", entryMapping.getDn(), entryMapping));
                continue;
            }

            String sourceName = sourceMapping.getSourceName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source name.", entryMapping.getDn() + "/" + alias, entryMapping));
            }

            SourceConfig sourceConfig = partitionConfig.getSourceConfigs().getSourceConfig(sourceName);
            if (sourceConfig == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid source name: " + sourceName, entryMapping.getDn() + "/" + alias, entryMapping));
                continue;

            }

            for (FieldMapping fieldMapping : sourceMapping.getFieldMappings()) {
                //log.debug("Validating entry "+entryMapping"'s fieldMapping "+source.getName()+"."+fieldMapping.getName());

                String fieldName = fieldMapping.getName();
                if (fieldName == null || "".equals(fieldName)) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing field name.", entryMapping.getDn() + "/" + alias, entryMapping));
                    continue;
                }

                Expression expression = fieldMapping.getExpression();
                if (expression != null) {
                    String foreach = expression.getForeach();
                    String var = expression.getVar();
                    if (foreach != null && (var == null || "".equals(var))) {
                        results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing variable name.", entryMapping.getDn() + "/" + alias + "." + fieldName, entryMapping));
                    }
                }

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                if (fieldConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid field name: " + fieldName, entryMapping.getDn() + "/" + alias + "." + fieldName, entryMapping));
                }
            }
        }

        return results;
    }

    public Collection<PartitionValidationResult> validateModuleConfigs(PartitionConfig partitionConfig, ModuleConfigs modules) throws Exception {

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
