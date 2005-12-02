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
package org.safehaus.penrose.schema;

import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.Row;

import java.io.File;
import java.util.TreeMap;
import java.util.Map;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class SchemaManager {

    private String home;

    private Map schemas = new TreeMap();
    private Schema schema = new Schema();

    public SchemaManager() {
    }

    public void load(SchemaConfig schemaConfig) throws Exception {

        String path = (home == null ? "" : home+File.separator)+schemaConfig.getPath();

        SchemaReader reader = new SchemaReader(path);
        Schema s = reader.read();

        schemas.put(schemaConfig.getName(), s);
        schema.add(s);
    }

    public Schema getSchema(String name) {
        return (Schema)schemas.get(name);
    }

    public Schema getSchema() {
        return schema;
    }

    public String getHome() {
        return home;
    }

    public void setHome(String home) {
        this.home = home;
    }

    public String normalize(String dn) throws Exception {
        return schema.normalize(dn);
    }

    public Row normalize(Row row) throws Exception {
        return schema.normalize(row);
    }

    public Collection getObjectClasses() {
        return schema.getObjectClasses();
    }

    public ObjectClass getObjectClass(String ocName) {
        return schema.getObjectClass(ocName);
    }

    public Collection getAllObjectClasses(String ocName) {
        return schema.getAllObjectClasses(ocName);
    }
    
    public Collection getAllObjectClassNames(String ocName) {
        return schema.getAllObjectClassNames(ocName);
    }

    public Collection getObjectClasses(EntryMapping entryMapping) {
        return schema.getObjectClasses(entryMapping);
    }

    public Collection getAttributeTypes() {
        return schema.getAttributeTypes();
    }

    public AttributeType getAttributeType(String attributeName) {
        return schema.getAttributeType(attributeName);
    }
}
