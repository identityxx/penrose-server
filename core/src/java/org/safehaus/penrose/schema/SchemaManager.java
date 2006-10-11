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
import org.safehaus.penrose.mapping.Row;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SchemaManager implements SchemaManagerMBean {

    Logger log = LoggerFactory.getLogger(getClass());

    private Map schemas = new TreeMap();
    private Schema allSchema = new Schema();

    public SchemaManager() {
    }

    public void load(String home, Collection schemaConfigs) throws Exception {

        for (Iterator i=schemaConfigs.iterator(); i.hasNext(); ) {
            SchemaConfig schemaConfig = (SchemaConfig)i.next();
            load(home, schemaConfig);
        }
    }

    public void load(String home, SchemaConfig schemaConfig) throws Exception {

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

    public String normalize(String dn) throws Exception {
        return allSchema.normalize(dn);
    }

    public Row normalize(Row row) throws Exception {
        return allSchema.normalize(row);
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

    public String getNormalizedAttributeName(String attributeName) {

        AttributeType attributeType = getAttributeType(attributeName);
        if (attributeType == null) return attributeName;

        return attributeType.getName();
    }
}
