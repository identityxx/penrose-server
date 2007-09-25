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

import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.io.File;

/**
 * @author Endi S. Dewata
 */
public class SchemaManager implements SchemaManagerMBean {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private Map<String,Schema> schemas = new TreeMap<String,Schema>();
    private Schema allSchema = new Schema();

    public SchemaManager() throws Exception {
    }

    public void init(String home, SchemaConfig schemaConfig) throws Exception {
        init(new File(home), schemaConfig);
    }

    public void init(File home, SchemaConfig schemaConfig) throws Exception {

        Schema schema = getSchema(schemaConfig.getName());
        if (schema != null) return;

        SchemaReader reader = new SchemaReader();
        schema = reader.read(home, schemaConfig);

        addSchema(schema);
    }

    public void addSchema(Schema schema) {
        schemas.put(schema.getName(), schema);
        allSchema.add(schema);
    }

    public void removeSchema(String name) {
        Schema schema = schemas.remove(name);
        allSchema.remove(schema);
    }

    public void clear() {
        schemas.clear();
        allSchema.clear();
    }

    public Schema getSchema(String name) {
        return schemas.get(name);
    }

    public Schema getAllSchema() {
        return allSchema;
    }

    public Collection<ObjectClass> getObjectClasses() {
        return allSchema.getObjectClasses();
    }

    public Collection<String> getObjectClassNames() {
        return allSchema.getObjectClassNames();
    }

    public ObjectClass getObjectClass(String ocName) {
        return allSchema.getObjectClass(ocName);
    }

    public Collection<ObjectClass> getAllObjectClasses(String ocName) {
        return allSchema.getAllObjectClasses(ocName);
    }
    
    public Collection<String> getAllObjectClassNames(String ocName) {
        return allSchema.getAllObjectClassNames(ocName);
    }

    public Collection<ObjectClass> getObjectClasses(Entry entry) {
        return getObjectClasses(entry.getEntryMapping());
    }

    public Collection<ObjectClass> getObjectClasses(EntryMapping entryMapping) {
        return allSchema.getObjectClasses(entryMapping);
    }

    public Collection<AttributeType> getAttributeTypes() {
        return allSchema.getAttributeTypes();
    }

    public Collection<String> getAttributeTypeNames() {
        return allSchema.getAttributeTypeNames();
    }

    public AttributeType getAttributeType(String attributeName) {
        return allSchema.getAttributeType(attributeName);
    }

    public String normalizeAttributeName(String attributeName) {

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
        RDNBuilder rb = new RDNBuilder();

        for (String name : rdn.getNames()) {
            Object value = rdn.get(name);
            rb.set(normalizeAttributeName(name), value);
        }

        return rb.toRdn();
    }

    public DN normalize(DN dn) {
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

            Collection values = attributes.getValues(name);
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
}
