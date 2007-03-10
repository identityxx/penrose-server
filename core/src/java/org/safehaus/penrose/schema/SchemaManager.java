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
import org.safehaus.penrose.entry.DN;
import org.safehaus.penrose.entry.DNBuilder;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.entry.RDN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.naming.directory.Attributes;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.Attribute;
import javax.naming.directory.BasicAttribute;
import javax.naming.NamingEnumeration;
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

    public SchemaManager() {
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

    public String getNormalizedAttributeName(String attributeName) {

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

    public DN normalize(DN oldDn) {
        DNBuilder db = new DNBuilder();
        RDNBuilder rb = new RDNBuilder();

        for (Iterator i=oldDn.getRdns().iterator(); i.hasNext(); ) {
            RDN rdn = (RDN)i.next();

            rb.clear();
            for (Iterator j=rdn.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                Object value = rdn.get(name);
                rb.set(getNormalizedAttributeName(name), value);
            }
            db.append(rb.toRdn());
        }
        return db.toDn();
    }

    public Collection normalize(Collection attributeNames) {
        if (attributeNames == null) return null;

        Collection list = new ArrayList();
        for (Iterator i = attributeNames.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            list.add(getNormalizedAttributeName(name));
        }

        return list;
    }

    public Attributes normalize(Attributes attributes) throws Exception{

        BasicAttributes newAttributes = new BasicAttributes();

        for (NamingEnumeration e=attributes.getAll(); e.hasMore(); ) {
            Attribute attribute = (Attribute)e.next();
            String attributeName = getNormalizedAttributeName(attribute.getID());

            BasicAttribute newAttribute = new BasicAttribute(attributeName);
            for (NamingEnumeration e2=attribute.getAll(); e2.hasMore(); ) {
                Object value = e2.next();
                newAttribute.add(value);
            }

            newAttributes.put(newAttribute);
        }

        return newAttributes;
    }
}
