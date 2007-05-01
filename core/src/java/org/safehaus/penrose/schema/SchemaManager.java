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

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.config.PenroseConfig;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.ldap.Attributes;
import org.safehaus.penrose.ldap.Attribute;
import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.filter.FilterTool;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SchemaManager implements SchemaManagerMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private PenroseConfig penroseConfig;
    private PenroseContext penroseContext;

    private Map schemas = new TreeMap();
    private Schema allSchema = new Schema();

    private FilterTool filterTool;

    public SchemaManager() throws Exception {
        filterTool = new FilterTool(this);
    }

    public void init(SchemaConfig schemaConfig) throws Exception {
        init(penroseConfig.getHome(), schemaConfig);
    }

    public void init(String home, SchemaConfig schemaConfig) throws Exception {

        Schema schema = getSchema(schemaConfig.getName());
        if (schema != null) return;

        SchemaReader reader = new SchemaReader(home);
        schema = reader.read(schemaConfig);

        addSchema(schema);
    }

    public void addSchema(Schema schema) {
        schemas.put(schema.getName(), schema);
        allSchema.add(schema);
    }

    public void removeSchema(String name) {
        Schema schema = (Schema)schemas.remove(name);
        allSchema.remove(schema);
    }

    public void clear() {
        schemas.clear();
        allSchema.clear();
    }

    public Schema getSchema(String name) {
        return (Schema)schemas.get(name);
    }

    public Schema getAllSchema() {
        return allSchema;
    }

    public Collection getObjectClasses() {
        return allSchema.getObjectClasses();
    }

    public Collection getObjectClassNames() {
        return allSchema.getObjectClassNames();
    }

    public ObjectClass getObjectClass(String ocName) {
        return allSchema.getObjectClass(ocName);
    }

    public Collection getAllObjectClasses(String ocName) {
        return allSchema.getAllObjectClasses(ocName);
    }
    
    public Collection getAllObjectClassNames(String ocName) {
        return allSchema.getAllObjectClassNames(ocName);
    }

    public Collection getObjectClasses(EntryMapping entryMapping) {
        return allSchema.getObjectClasses(entryMapping);
    }

    public Collection getAttributeTypes() {
        return allSchema.getAttributeTypes();
    }

    public Collection getAttributeTypeNames() {
        return allSchema.getAttributeTypeNames();
    }

    public AttributeType getAttributeType(String attributeName) {
        return allSchema.getAttributeType(attributeName);
    }

    public String normalizeAttributeName(String attributeName) {

        AttributeType attributeType = getAttributeType(attributeName);
        if (attributeType == null) return attributeName;

        return attributeType.getName();
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

        for (Iterator j=rdn.getNames().iterator(); j.hasNext(); ) {
            String name = (String)j.next();
            Object value = rdn.get(name);
            rb.set(normalizeAttributeName(name), value);
        }

        return rb.toRdn();
    }

    public DN normalize(DN dn) {
        DNBuilder db = new DNBuilder();
        RDNBuilder rb = new RDNBuilder();

        for (Iterator i=dn.getRdns().iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();

            rb.clear();
            for (Iterator j=rdn.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
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
        for (Iterator i = attributeNames.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            list.add(normalizeAttributeName(name));
        }

        return list;
    }

    public Attributes normalize(Attributes attributes) throws Exception{

        Collection names = new ArrayList(attributes.getNames());

        for (Iterator i=names.iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Collection values = attributes.getValues(name);
            attributes.remove(name);

            name = normalizeAttributeName(name);
            attributes.setValues(name, values);
        }

        return attributes;
    }

    public Collection<Modification> normalizeModifications(Collection<Modification> modifications) throws Exception {
        Collection<Modification> normalizedModifications = new ArrayList<Modification>();

        for (Iterator i = modifications.iterator(); i.hasNext();) {
            Modification modification = (Modification) i.next();

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

    public FilterTool getFilterTool() {
        return filterTool;
    }

    public void setFilterTool(FilterTool filterTool) {
        this.filterTool = filterTool;
    }
}
