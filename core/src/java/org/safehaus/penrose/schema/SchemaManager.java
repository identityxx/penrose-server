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
package org.safehaus.penrose.schema;

import org.safehaus.penrose.PenroseConfig;
import org.safehaus.penrose.schema.attributeSyntax.AttributeSyntax;
import org.safehaus.penrose.schema.attributeSyntax.AttributeSyntaxUtil;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author Endi S. Dewata
 */
public class SchemaManager {

    public Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    File home;
    File schemaDir;
    File extDir;

    SchemaReader reader = new SchemaReader();
    SchemaWriter writer = new SchemaWriter();

    private Map<String,Schema> schemas = new TreeMap<String,Schema>();
    private Map<String,Schema> builtInSchemas = new TreeMap<String,Schema>();
    private Map<String,Schema> customSchemas = new TreeMap<String,Schema>();

    private Schema schema = new Schema("DEFAULT");

    public SchemaManager(File home) throws Exception {
        this.home      = home;
        this.schemaDir = new File(home, "schema");
        this.extDir    = new File(schemaDir, "ext");
    }

    public File[] getSchemaFiles(File dir) {

        return dir.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".schema");
            }
        });
    }

    public void loadSchemas() throws Exception {

        log.debug("Built-in schema files:");
        File[] list = getSchemaFiles(schemaDir);
        if (list != null) {
            for (File schemaFile : list) {
                log.debug(" - "+schemaFile);

                Schema schema = loadSchema(schemaFile);
                addSchema(schema);
                builtInSchemas.put(schema.getName(), schema);
            }
        }

        log.debug("Custom schema files:");
        list = getSchemaFiles(extDir);
        if (list != null) {
            for (File schemaFile : list) {
                log.debug(" - "+schemaFile);

                Schema schema = loadSchema(schemaFile);
                addSchema(schema);
                customSchemas.put(schema.getName(), schema);
            }
        }
    }

    public Schema loadSchema(File schemaFile) throws Exception {
        return reader.read(schemaFile);
    }

    public void addSchema(Schema schema) {
        schemas.put(schema.getName(), schema);
        this.schema.add(schema);
    }

    public void createSchema(Schema schema) throws Exception {

        File file = new File(extDir, schema.getName()+".schema");

        writer.write(file, schema);

        addSchema(schema);
        customSchemas.put(schema.getName(), schema);
    }

    public void updateSchema(String schemaName, Schema schema) throws Exception {

        Schema oldSchema = schemas.get(schemaName);
        oldSchema.copy(schema);

        if (!schemaName.equals(schema.getName())) {
            
            File oldFile = new File(extDir, schemaName+".schema");
            FileUtil.delete(oldFile);

            schemas.remove(schemaName);
            schemas.put(schema.getName(), schema);

            if (builtInSchemas.containsKey(schemaName)) {
                builtInSchemas.remove(schemaName);
                builtInSchemas.put(schemaName, schema);
            }

            if (customSchemas.containsKey(schemaName)) {
                customSchemas.remove(schemaName);
                customSchemas.put(schemaName, schema);
            }
        }

        File newFile = new File(extDir, schema.getName()+".schema");
        writer.write(newFile, schema);

        this.schema.clear();
        for (Schema s : schemas.values()) {
            this.schema.add(s);
        }
    }

    public void storeSchema(String schemaName) throws Exception {

        Schema schema = schemas.get(schemaName);

        File file = new File(extDir, schemaName+".schema");
        writer.write(file, schema);
    }

    public void removeSchema(String schemaName) {

        File oldFile = new File(extDir, schemaName+".schema");
        FileUtil.delete(oldFile);

        schemas.remove(schemaName);
        builtInSchemas.remove(schemaName);
        customSchemas.remove(schemaName);

        schema.clear();
        for (Schema s : schemas.values()) {
            schema.add(s);
        }
    }

    public void clear() {
        schemas.clear();
        builtInSchemas.clear();
        customSchemas.clear();
        schema.clear();
    }

    public Collection<String> getSchemaNames() {
        return schemas.keySet();
    }
    
    public Collection<String> getBuiltInSchemaNames() {
        return builtInSchemas.keySet();
    }

    public Collection<String> getCustomSchemaNames() {
        return customSchemas.keySet();
    }

    public Schema getSchema(String name) {
        return schemas.get(name);
    }

    public Schema getSchema() {
        return schema;
    }

    public Collection<ObjectClass> getObjectClasses() {
        return schema.getObjectClasses();
    }

    public Collection<String> getObjectClassNames() {
        return schema.getObjectClassNames();
    }

    public ObjectClass getObjectClass(String ocName) {
        return schema.getObjectClass(ocName);
    }

    public Collection<ObjectClass> getAllObjectClasses(String ocName) {
        return schema.getAllObjectClasses(ocName);
    }
    
    public Collection<String> getAllObjectClassNames(String ocName) {
        return schema.getAllObjectClassNames(ocName);
    }

    public Collection<ObjectClass> getObjectClasses(Entry entry) {
        return getObjectClasses(entry.getEntryConfig());
    }

    public Collection<ObjectClass> getObjectClasses(EntryConfig entryConfig) {
        return schema.getObjectClasses(entryConfig);
    }

    public Collection<AttributeType> getAttributeTypes() {
        return schema.getAttributeTypes();
    }

    public Collection<String> getAttributeTypeNames() {
        return schema.getAttributeTypeNames();
    }

    public AttributeType getAttributeType(String attributeName) {
        return schema.getAttributeType(attributeName);
    }

    public String normalizeAttributeName(String attributeName) {

        boolean debug = log.isDebugEnabled();
        AttributeType attributeType = getAttributeType(attributeName);

        String normalizedAttributeName;
        if (attributeType == null) {
            if (debug) log.debug("Attribute type "+attributeName+" is undefined.");
            normalizedAttributeName = attributeName;
        } else {
            normalizedAttributeName = attributeType.getName();
        }

        if (debug) log.debug("Normalize "+attributeName+" => "+normalizedAttributeName);
        
        return normalizedAttributeName;
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

    public RDN normalize(RDN rdn) {
        if (rdn == null) return null;

        RDNBuilder rb = new RDNBuilder();

        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);
            rb.set(normalizeAttributeName(name), value);
        }

        return rb.toRdn();
    }

    public DN normalize(DN dn) throws Exception {
        if (dn == null) return null;

        DNBuilder db = new DNBuilder();
        RDNBuilder rb = new RDNBuilder();

        for (RDN rdn : dn.getRdns()) {

            rb.clear();
            for (String name : rdn.getNames()) {
                Object value = rdn.get(name);
                rb.set(normalizeAttributeName(name), value);
            }
            db.append(rb.toRdn());
        }

        return db.toDn();
    }

    public Collection<String> normalize(Collection<String> attributeNames) {
        if (attributeNames == null) return null;

        Collection<String> list = new ArrayList<String>();
        for (String name : attributeNames) {
            list.add(normalizeAttributeName(name));
        }

        return list;
    }

    public Attributes normalize(Attributes attributes) throws Exception{

        Collection<String> names = new ArrayList<String>(attributes.getNames());

        for (String name : names) {

            Collection<Object> values = attributes.getValues(name);
            attributes.remove(name);

            name = normalizeAttributeName(name);
            attributes.setValues(name, values);
        }

        return attributes;
    }

    public Collection<Modification> normalizeModifications(Collection<Modification> modifications) throws Exception {
        Collection<Modification> normalizedModifications = new ArrayList<Modification>();

        for (Modification modification : modifications) {

            int type = modification.getType();
            Attribute attribute = modification.getAttribute();
            String name = normalizeAttributeName(attribute.getName());

            switch (type) {
                case Modification.ADD:
                    log.debug("add: " + name);
                    break;
                case Modification.DELETE:
                    log.debug("delete: " + name);
                    break;
                case Modification.REPLACE:
                    log.debug("replace: " + name);
                    break;
            }

            Attribute normalizedAttribute = new Attribute(name);
            normalizedAttribute.addValues(attribute.getValues());

            Modification normalizedModification = new Modification(type, normalizedAttribute);
            normalizedModifications.add(normalizedModification);
        }

        return normalizedModifications;
    }

    public boolean isOperational(String attributeName) {
        AttributeType attributeType = getAttributeType(attributeName);
        return attributeType != null && attributeType.isOperational();
    }

    public AttributeSyntax getAttributeSyntax(String oid) {
        return AttributeSyntaxUtil.getAttributeSyntax(oid);
    }
}
