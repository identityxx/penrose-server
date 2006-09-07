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
package org.safehaus.penrose.partition;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connector.AdapterConfig;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.module.ModuleConfig;
import org.ietf.ldap.LDAPDN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.TreeSet;

/**
 * @author Endi S. Dewata
 */
public class PartitionValidator {

    Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private SchemaManager schemaManager;

    public PartitionValidator() {
    }

    public Collection validate(Partition partition) throws Exception {
        Collection results = new ArrayList();

        results.addAll(validateConnectionConfigs(partition));
        results.addAll(validateSourceConfigs(partition));
        results.addAll(validateEntryMappings(partition));
        results.addAll(validateModuleConfigs(partition));

        return results;
    }

    public Collection validateConnectionConfigs(Partition partition) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=partition.getConnectionConfigs().iterator(); i.hasNext(); ) {
            ConnectionConfig connectionConfig = (ConnectionConfig)i.next();
            //log.debug("Validating connection "+connectionConfig.getName());

            String connectionName = connectionConfig.getName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing connection name.", partition.getName()+":", connectionConfig));
                continue;
            }

            String adapterName = connectionConfig.getAdapterName();
            if (adapterName == null || "".equals(adapterName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing adapter name.", partition.getName()+": "+connectionName, connectionConfig));

            } else if (penroseConfig != null) {
                AdapterConfig adapterConfig = penroseConfig.getAdapterConfig(adapterName);
                if (adapterConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid adapter name: "+adapterName, partition.getName()+": "+connectionName, connectionConfig));
                }
            }
        }

        return results;
    }

    public Collection validateSourceConfigs(Partition partition) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=partition.getSourceConfigs().iterator(); i.hasNext(); ) {
            SourceConfig sourceConfig = (SourceConfig)i.next();

            String sourceName = sourceConfig.getName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source name.", partition.getName()+":", sourceConfig));
                continue;
            }

            String connectionName = sourceConfig.getConnectionName();
            if (connectionName == null || "".equals(connectionName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing connection name.", partition.getName()+": "+sourceName, sourceConfig));
                continue;
            }

            ConnectionConfig connectionConfig = partition.getConnectionConfig(connectionName);
            if (connectionConfig == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid connection name: "+connectionName, partition.getName()+": "+sourceName, sourceConfig));
            }

            for (Iterator k=sourceConfig.getFieldConfigs().iterator(); k.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)k.next();

                String fieldName = fieldConfig.getName();
                if (fieldName == null || "".equals(fieldName)) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing field name.", partition.getName()+": "+sourceName, sourceConfig));
                }
            }

            if (!sourceConfig.getFieldConfigs().isEmpty() && sourceConfig.getPrimaryKeyFieldConfigs().isEmpty()) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing primary key(s).", partition.getName()+": "+sourceName, sourceConfig));
            }
        }

        return results;
    }

    public Collection validateEntryMappings(Partition partition) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=partition.getEntryMappings().iterator(); i.hasNext(); ) {
            EntryMapping entryMapping = (EntryMapping)i.next();
            //log.debug("Validating entry "+entryMapping;

            String rdn = entryMapping.getRdn();
            if (rdn == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing RDN.", entryMapping.getDn(), entryMapping));

            } else if (!LDAPDN.isValid(rdn)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid RDN: "+rdn, entryMapping.getDn(), entryMapping));
            }

            String parentDn = entryMapping.getParentDn();
            if (parentDn != null && !LDAPDN.isValid(parentDn)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid parent DN: "+parentDn, entryMapping.getDn(), entryMapping));
            }

            results.addAll(validateObjectClasses(partition, entryMapping));
            results.addAll(validateAttributeMappings(partition, entryMapping));
            results.addAll(validateSourceMappings(partition, entryMapping));
        }

        return results;
    }

    public Collection validateObjectClasses(Partition partition, EntryMapping entryMapping) {
        Collection results = new ArrayList();

        //log.debug("Validating entry "+entryMapping"'s object classes");

        Collection missingObjectClasses = new TreeSet();

        Collection objectClasses = entryMapping.getObjectClasses();
        //System.out.println("Checking "+entryMapping" object classes "+objectClasses);

        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();

            ObjectClass objectClass = schemaManager.getObjectClass(ocName);
            if (objectClass == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Object class "+ocName+" is not defined in the schema.", entryMapping.getDn(), entryMapping));
            }

            Collection scNames = schemaManager.getAllObjectClassNames(ocName);
            for (Iterator j=scNames.iterator(); j.hasNext(); ) {
                String scName = (String)j.next();
                if ("top".equals(scName)) continue;
                
                if (!objectClasses.contains(scName)) {
                    //System.out.println(" - ["+scName+"] not found in "+objectClasses);
                    missingObjectClasses.add(scName);
                }
            }
        }

        for (Iterator i=missingObjectClasses.iterator(); i.hasNext(); ) {
            String scName = (String)i.next();
            results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Missing object class "+scName+".", entryMapping.getDn(), entryMapping));
        }

        return results;
    }

    public Collection validateAttributeMappings(Partition partition, EntryMapping entryMapping) {
        Collection results = new ArrayList();

        //log.debug("Validating entry "+entryMapping"'s attributes");

        for (Iterator i=entryMapping.getAttributeMappings().iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();

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
        }

        if (!entryMapping.getAttributeMappings().isEmpty() && entryMapping.getRdnAttributes().isEmpty()) {
            results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing rdn attribute(s).", entryMapping.getDn(), entryMapping));
        }

        Collection objectClasses = schemaManager.getObjectClasses(entryMapping);
        for (Iterator i=objectClasses.iterator(); i.hasNext(); ) {
            ObjectClass objectClass = (ObjectClass)i.next();

            Collection requiredAttributes = objectClass.getRequiredAttributes();
            for (Iterator j=requiredAttributes.iterator(); j.hasNext(); ) {
                String atName = (String)j.next();

                Collection attributeMappings = entryMapping.getAttributeMappings(atName);
                if (attributeMappings == null || attributeMappings.isEmpty()) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.WARNING, "Attribute "+atName+" is required by "+objectClass.getName()+" object class.", entryMapping.getDn(), entryMapping));
                }
            }
        }
        return results;
    }

    public Collection validateSourceMappings(Partition partition, EntryMapping entryMapping) {
        Collection results = new ArrayList();

        for (Iterator i=entryMapping.getSourceMappings().iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            //log.debug("Validating entry "+entryMapping"'s sourceMapping "+sourceMapping.getName());

            String alias = sourceMapping.getName();
            if (alias == null || "".equals(alias)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source alias.", entryMapping.getDn(), entryMapping));
                continue;
            }

            String sourceName = sourceMapping.getSourceName();
            if (sourceName == null || "".equals(sourceName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing source name.", entryMapping.getDn()+"/"+alias, entryMapping));
            }

            SourceConfig sourceConfig = partition.getSourceConfig(sourceName);
            if (sourceConfig == null) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid source name: "+sourceName, entryMapping.getDn()+"/"+alias, entryMapping));
                continue;

            }

            for (Iterator j=sourceMapping.getFieldMappings().iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                //log.debug("Validating entry "+entryMapping"'s fieldMapping "+source.getName()+"."+fieldMapping.getName());

                String fieldName = fieldMapping.getName();
                if (fieldName == null || "".equals(fieldName)) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing field name.", entryMapping.getDn()+"/"+alias, entryMapping));
                    continue;
                }

                Expression expression = fieldMapping.getExpression();
                if (expression != null) {
                    String foreach = expression.getForeach();
                    String var = expression.getVar();
                    if (foreach != null && (var == null || "".equals(var))) {
                        results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing variable name.", entryMapping.getDn()+"/"+alias+"."+fieldName, entryMapping));
                    }
                }

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);
                if (fieldConfig == null) {
                    results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Invalid field name: "+fieldName, entryMapping.getDn()+"/"+alias+"."+fieldName, entryMapping));
                }
            }
        }

        return results;
    }

    public Collection validateModuleConfigs(Partition partition) throws Exception {
        Collection results = new ArrayList();

        for (Iterator i=partition.getModuleConfigs().iterator(); i.hasNext(); ) {
            ModuleConfig moduleConfig = (ModuleConfig)i.next();
            //log.debug("Validating module "+moduleConfig.getName());

            String moduleName = moduleConfig.getName();
            if (moduleName == null || "".equals(moduleName)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing module name.", partition.getName()+":", moduleConfig));
            }

            String moduleClass = moduleConfig.getModuleClass();
            if (moduleClass == null || "".equals(moduleClass)) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Missing module class name.", partition.getName()+": "+moduleName, moduleConfig));
            }

            try {
                Class.forName(moduleClass);
            } catch (Exception e) {
                results.add(new PartitionValidationResult(PartitionValidationResult.ERROR, "Module class not found: "+moduleClass, partition.getName()+": "+moduleName, moduleConfig));
            }
        }

        return results;
    }

    public PenroseConfig getPenroseConfig() {
        return penroseConfig;
    }

    public void setPenroseConfig(PenroseConfig penroseConfig) {
        this.penroseConfig = penroseConfig;
    }

    public SchemaManager getSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }
}
