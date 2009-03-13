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
package org.safehaus.penrose.validation;

import org.safehaus.penrose.adapter.AdapterConfig;
import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.connection.ConnectionConfig;
import org.safehaus.penrose.connection.ConnectionConfigManager;
import org.safehaus.penrose.directory.*;
import org.safehaus.penrose.ldap.DN;
import org.safehaus.penrose.mapping.Expression;
import org.safehaus.penrose.module.ModuleConfig;
import org.safehaus.penrose.module.ModuleConfigManager;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.source.SourceConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.TreeSet;

/**
 * @author Endi S. Dewata
 */
public class Validator {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    public Validator() {
    }

    public Collection<ValidationResult> validate(PartitionConfig partitionConfig) throws Exception {
        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        results.addAll(validate(partitionConfig, partitionConfig.getConnectionConfigManager()));
        results.addAll(validate(partitionConfig, partitionConfig.getSourceConfigManager()));
        results.addAll(validate(partitionConfig, partitionConfig.getDirectoryConfig()));
        results.addAll(validateModuleConfigs(partitionConfig, partitionConfig.getModuleConfigManager()));

        return results;
    }

    public Collection<ValidationResult> validate(PartitionConfig partitionConfig, ConnectionConfigManager connections) throws Exception {

        String name = partitionConfig.getName();

        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        for (ConnectionConfig connectionConfig : connections.getConnectionConfigs()) {
            //log.debug("Validating connection "+connectionConfig.getName());

            String connectionName = connectionConfig.getName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing connection name.",
                        partitionConfig.getName(),
                        ValidationResult.CONNECTION,
                        connectionConfig.getName()
                ));
                continue;
            }

            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null || "".equals(adapterName)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing adapter name.",
                        partitionConfig.getName(),
                        ValidationResult.CONNECTION,
                        connectionConfig.getName()
                ));

            } else if (penroseConfig != null) {
                AdapterConfig adapterConfig = partitionConfig.getAdapterConfig(adapterName);

                if (adapterConfig == null) {
                    adapterConfig = penroseConfig.getAdapterConfig(adapterName);
                }

                if (adapterConfig == null) {
                    results.add(new ValidationResult(
                            ValidationResult.ERROR,
                            "Invalid adapter name: " + adapterName,
                            partitionConfig.getName(),
                            ValidationResult.CONNECTION,
                            connectionConfig.getName()
                    ));
                }
            }
        }

        return results;
    }

    public Collection<ValidationResult> validate(PartitionConfig partitionConfig, SourceConfigManager sources) throws Exception {

        String name = partitionConfig.getName();

        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        for (SourceConfig sourceConfig : sources.getSourceConfigs()) {

            String sourceName = sourceConfig.getName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing source name.",
                        partitionConfig.getName(),
                        ValidationResult.SOURCE,
                        sourceConfig.getName()
                ));
                continue;
            }

            String connectionName = sourceConfig.getConnectionName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing connection name.",
                        partitionConfig.getName(),
                        ValidationResult.SOURCE,
                        sourceConfig.getName()
                ));
                continue;
            }

            ConnectionConfig connectionConfig = partitionConfig.getConnectionConfigManager().getConnectionConfig(connectionName);
            if (connectionConfig == null) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Invalid connection name: " + connectionName,
                        partitionConfig.getName(),
                        ValidationResult.SOURCE,
                        sourceConfig.getName()
                ));
            }

            for (FieldConfig fieldConfig : sourceConfig.getFieldConfigs()) {

                String fieldName = fieldConfig.getName();
                if (fieldName == null || "".equals(fieldName)) {
                    results.add(new ValidationResult(
                            ValidationResult.ERROR,
                            "Missing field name.",
                            partitionConfig.getName(),
                            ValidationResult.SOURCE,
                            sourceConfig.getName()
                    ));
                }
            }

            if (!sourceConfig.getFieldConfigs().isEmpty() && sourceConfig.getPrimaryKeyFieldConfigs().isEmpty()) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing primary key(s).",
                        partitionConfig.getName(),
                        ValidationResult.SOURCE,
                        sourceConfig.getName()
                ));
            }
        }

        return results;
    }

    public Collection<ValidationResult> validate(PartitionConfig partitionConfig, DirectoryConfig mappings) throws Exception {
        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        for (EntryConfig entryConfig : mappings.getEntryConfigs()) {
            //log.debug("Validating entry "+entryConfig;

            DN dn = entryConfig.getDn();
            if (dn.isEmpty()) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing DN.",
                        partitionConfig.getName(),
                        ValidationResult.ENTRY,
                        entryConfig.getName()
                ));
            }

            results.addAll(validateObjectClasses(partitionConfig, mappings, entryConfig));
            results.addAll(validateAttributeMappings(partitionConfig, mappings, entryConfig));
            results.addAll(validateSourceMappings(partitionConfig, mappings, entryConfig));
        }

        return results;
    }

    public Collection<ValidationResult> validateObjectClasses(PartitionConfig partitionConfig, DirectoryConfig mappings, EntryConfig entryConfig) {
        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        //log.debug("Validating entry "+entryConfig"'s object classes");

        Collection<String> missingObjectClasses = new TreeSet<String>();

        Collection<String> objectClasses = entryConfig.getObjectClasses();
        //System.out.println("Checking "+entryConfig" object classes "+objectClasses);

        SchemaManager schemaManager = penroseContext.getSchemaManager();

        for (String ocName : objectClasses) {

            ObjectClass objectClass = schemaManager.getObjectClass(ocName);
            if (objectClass == null) {
                results.add(new ValidationResult(
                        ValidationResult.WARNING,
                        "Object class " + ocName + " is not defined in the schema.",
                        partitionConfig.getName(),
                        ValidationResult.ENTRY,
                        entryConfig.getName()
                ));
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
            results.add(new ValidationResult(
                    ValidationResult.WARNING,
                    "Missing object class " + scName + ".",
                    partitionConfig.getName(),
                    ValidationResult.ENTRY,
                    entryConfig.getName()
            ));
        }

        return results;
    }

    public Collection<ValidationResult> validateAttributeMappings(PartitionConfig partitionConfig, DirectoryConfig mappings, EntryConfig entryConfig) {
        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        //log.debug("Validating entry "+entryConfig"'s attributes");
        SchemaManager schemaManager = penroseContext.getSchemaManager();

        for (EntryAttributeConfig attributeMapping : entryConfig.getAttributeConfigs()) {

            String name = attributeMapping.getName();
            if (name == null || "".equals(name)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing attribute name.",
                        partitionConfig.getName(),
                        ValidationResult.ENTRY,
                        entryConfig.getName()
                ));
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

                EntrySourceConfig sourceMapping = entryConfig.getSourceConfig(sourceAlias);
                if (sourceMapping == null) {
                    results.add(new ValidationResult(
                            ValidationResult.ERROR,
                            "Unknown source mapping: " + sourceAlias,
                            partitionConfig.getName(),
                            ValidationResult.ENTRY,
                            entryConfig.getName()
                    ));
                    continue;
                }

                SourceConfig sourceConfig = partitionConfig.getSourceConfigManager().getSourceConfig(sourceMapping.getSourceName());
                if (sourceConfig == null) {
                    results.add(new ValidationResult(
                            ValidationResult.ERROR,
                            "Unknown source: " + sourceMapping.getSourceName(),
                            partitionConfig.getName(),
                            ValidationResult.ENTRY,
                            entryConfig.getName()
                    ));
                    continue;
                }

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                if (fieldConfig == null) {
                    results.add(new ValidationResult(
                            ValidationResult.ERROR,
                            "Unknown field: " + variable,
                            partitionConfig.getName(),
                            ValidationResult.ENTRY,
                            entryConfig.getName()
                    ));
                }
            }
        }

        if (!entryConfig.getAttributeConfigs().isEmpty() && entryConfig.getRdnAttributeConfigs().isEmpty()) {
            results.add(new ValidationResult(
                    ValidationResult.ERROR,
                    "Missing rdn attribute(s).",
                    partitionConfig.getName(),
                    ValidationResult.ENTRY,
                    entryConfig.getName()
            ));
        }

        Collection<ObjectClass> objectClasses = schemaManager.getObjectClasses(entryConfig);
        for (ObjectClass objectClass : objectClasses) {

            Collection<String> requiredAttributes = objectClass.getRequiredAttributes();
            for (String atName : requiredAttributes) {

                Collection attributeMappings = entryConfig.getAttributeConfigs(atName);
                if (attributeMappings == null || attributeMappings.isEmpty()) {
                    results.add(new ValidationResult(
                            ValidationResult.WARNING,
                            "Attribute " + atName + " is required by " + objectClass.getName() + " object class.",
                            partitionConfig.getName(),
                            ValidationResult.ENTRY,
                            entryConfig.getName()
                    ));
                }
            }
        }

        return results;
    }

    public Collection<ValidationResult> validateSourceMappings(PartitionConfig partitionConfig, DirectoryConfig mappings, EntryConfig entryConfig) {
        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        for (EntrySourceConfig sourceMapping : entryConfig.getSourceConfigs()) {
            //log.debug("Validating entry "+entryConfig"'s sourceMapping "+sourceMapping.getName());

            String alias = sourceMapping.getAlias();
            if (alias == null || "".equals(alias)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing source alias.",
                        partitionConfig.getName(),
                        ValidationResult.ENTRY,
                        entryConfig.getName()
                ));
                continue;
            }

            String sourceName = sourceMapping.getSourceName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing source name.",
                        partitionConfig.getName(),
                        ValidationResult.ENTRY,
                        entryConfig.getName()
                ));
            }

            SourceConfig sourceConfig = partitionConfig.getSourceConfigManager().getSourceConfig(sourceName);
            if (sourceConfig == null) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Invalid source name: " + sourceName,
                        partitionConfig.getName(),
                        ValidationResult.ENTRY,
                        entryConfig.getName()
                ));
                continue;

            }

            for (EntryFieldConfig fieldMapping : sourceMapping.getFieldConfigs()) {
                //log.debug("Validating entry "+entryConfig"'s fieldMapping "+source.getName()+"."+fieldMapping.getName());

                String fieldName = fieldMapping.getName();
                if (fieldName == null || "".equals(fieldName)) {
                    results.add(new ValidationResult(
                            ValidationResult.ERROR,
                            "Missing field name.",
                            partitionConfig.getName(),
                            ValidationResult.ENTRY,
                            entryConfig.getName()
                    ));
                    continue;
                }

                Expression expression = fieldMapping.getExpression();
                if (expression != null) {
                    String foreach = expression.getForeach();
                    String var = expression.getVar();
                    if (foreach != null && (var == null || "".equals(var))) {
                        results.add(new ValidationResult(
                                ValidationResult.ERROR,
                                "Missing variable name.",
                                partitionConfig.getName(),
                                ValidationResult.ENTRY,
                                entryConfig.getName()
                        ));
                    }
                }

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                if (fieldConfig == null) {
                    results.add(new ValidationResult(
                            ValidationResult.ERROR,
                            "Invalid field name: " + fieldName,
                            partitionConfig.getName(),
                            ValidationResult.ENTRY,
                            entryConfig.getName()
                    ));
                }
            }
        }

        return results;
    }

    public Collection<ValidationResult> validateModuleConfigs(PartitionConfig partitionConfig, ModuleConfigManager modules) throws Exception {

        String name = partitionConfig.getName();

        Collection<ValidationResult> results = new ArrayList<ValidationResult>();

        for (ModuleConfig moduleConfig : modules.getModuleConfigs()) {
            //log.debug("Validating module "+moduleConfig.getName());

            String moduleName = moduleConfig.getName();
            if (moduleName == null || "".equals(moduleName)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing module name.",
                        partitionConfig.getName(),
                        ValidationResult.MODULE,
                        moduleConfig.getName()
                ));
            }

            String moduleClass = moduleConfig.getModuleClass();
            if (moduleClass == null || "".equals(moduleClass)) {
                results.add(new ValidationResult(
                        ValidationResult.ERROR,
                        "Missing module class name.",
                        partitionConfig.getName(),
                        ValidationResult.MODULE,
                        moduleConfig.getName()
                ));
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
