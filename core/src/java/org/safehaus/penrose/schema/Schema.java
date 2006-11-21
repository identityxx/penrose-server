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
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.util.EntryUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Schema implements Cloneable {

    Logger log = LoggerFactory.getLogger(getClass());

    private SchemaConfig schemaConfig;

    protected Map attributeTypes = new TreeMap();
    protected Map objectClasses = new TreeMap();

    public Schema() {
    }

    public Schema(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    public String getName() {
        return schemaConfig == null ? null : schemaConfig.getName();
    }
    
    public Collection getAttributeTypes() {
        Collection list = new TreeSet();
        list.addAll(attributeTypes.values());
        return list;
    }

    public Collection getAttributeTypeNames() {
        Collection names = new TreeSet();
        for (Iterator i=attributeTypes.values().iterator(); i.hasNext(); ) {
            AttributeType at = (AttributeType)i.next();
            names.add(at.getName());
        }
        return names;
    }

    public AttributeType getAttributeType(String name) {
        return (AttributeType)attributeTypes.get(name.toLowerCase());
    }

    public void addAttributeType(AttributeType at) {
        attributeTypes.put(at.getOid(), at);
        for (Iterator i=at.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            attributeTypes.put(name.toLowerCase(), at);
        }
    }

    public void removeAttributeTypes(Collection names) {
        for (Iterator i=names.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            removeAttributeType(name);
        }
    }

    public AttributeType removeAttributeType(String name) {
        AttributeType at = (AttributeType)attributeTypes.get(name.toLowerCase());
        if (at == null) return null;
        attributeTypes.remove(at.getOid());
        for (Iterator i=at.getNames().iterator(); i.hasNext(); ) {
            String atName = (String)i.next();
            attributeTypes.remove(atName.toLowerCase());
        }
        return at;
    }

    public Collection getObjectClasses() {
        Collection list = new TreeSet();
        list.addAll(objectClasses.values());
        return list;
    }

    public Collection getObjectClassNames() {
        Collection names = new TreeSet();
        for (Iterator i=objectClasses.values().iterator(); i.hasNext(); ) {
            ObjectClass oc = (ObjectClass)i.next();
            names.add(oc.getName());
        }
        return names;
    }

    public ObjectClass getObjectClass(String name) {
        return (ObjectClass)objectClasses.get(name.toLowerCase());
    }

    public void addObjectClass(ObjectClass oc) {
        objectClasses.put(oc.getOid(), oc);
        for (Iterator i=oc.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            objectClasses.put(name.toLowerCase(), oc);
        }
    }

    public void removeObjectClasses(Collection names) {
        for (Iterator i=names.iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            removeObjectClass(name);
        }
    }

    public ObjectClass removeObjectClass(String name) {
        ObjectClass oc = (ObjectClass)objectClasses.get(name.toLowerCase());
        if (oc == null) return null;
        objectClasses.remove(oc.getOid());
        for (Iterator i=oc.getNames().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();
            objectClasses.remove(ocName.toLowerCase());
        }
        return oc;
    }

    public Set getRequiredAttributeNames(EntryMapping entry) {
        Set set = new HashSet();
        for (Iterator i=entry.getObjectClasses().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();
            ObjectClass oc = getObjectClass(ocName);
            if (oc == null) continue;

            set.addAll(oc.getRequiredAttributes());
        }

        return set;
    }

    public Collection getAllObjectClassNames(EntryMapping entry) {
        Collection list = new ArrayList();

        for (Iterator i=entry.getObjectClasses().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();

            getAllObjectClassNames(list, ocName);
        }

        return list;
    }

    public Collection getAllObjectClassNames(String ocName) {
        Collection list = new ArrayList();
        getAllObjectClassNames(list, ocName);
        return list;
    }

    public void getAllObjectClassNames(Collection list, String ocName) {
    	if (list.contains(ocName)) return;

        ObjectClass oc = getObjectClass(ocName);
        if (oc == null) return;

    	Collection superClasses = oc.getSuperClasses();
    	for (Iterator i=superClasses.iterator(); i.hasNext(); ) {
    		String supName = (String)i.next();

            getAllObjectClassNames(list, supName);
    	}

        list.add(ocName);
    }

    public boolean isSuperClass(String parent, String child) {
        if (parent.equals(child)) return true;

        ObjectClass oc = getObjectClass(child);
        if (oc == null) return false;

        Collection superClasses = oc.getSuperClasses();
        for (Iterator i=superClasses.iterator(); i.hasNext(); ) {
            String supName = (String)i.next();
            //log.debug(" - comparing "+parent+" with "+supName+": "+supName.equals(parent));
            if (supName.equals(parent)) return true;

            boolean result = isSuperClass(parent, supName);
            if (result) return true;
        }

        return false;
    }

    public Collection getObjectClasses(EntryMapping entry) {
        Map map = new HashMap();
        for (Iterator i=entry.getObjectClasses().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();
            getAllObjectClasses(ocName, map);
        }

        return map.values();
    }

    public Collection getAllObjectClasses(String objectClassName) {
        Map map = new HashMap();
        getAllObjectClasses(objectClassName, map);
        return map.values();
    }

    public void getAllObjectClasses(String objectClassName, Map map) {
        if ("top".equalsIgnoreCase(objectClassName)) return;
        if (map.containsKey(objectClassName)) return;

        // add itself
        //log.debug("Searching for object class "+objectClassName+" ... ");
        ObjectClass objectClass = getObjectClass(objectClassName);
        if (objectClass == null) {
            //log.debug("--> NOT FOUND");
            return;
        }

        //log.debug("--> FOUND");
        map.put(objectClassName, objectClass);

        if (objectClass.getSuperClasses() == null) return;

        // add all superclasses
        for (Iterator i=objectClass.getSuperClasses().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();
            getAllObjectClasses(ocName, map);
        }
    }

    public void add(Schema schema) {
        attributeTypes.putAll(schema.attributeTypes);
        objectClasses.putAll(schema.objectClasses);
    }

    public void remove(Schema schema) {
        for (Iterator i=schema.attributeTypes.values().iterator(); i.hasNext(); ) {
            AttributeType at = (AttributeType)i.next();
            attributeTypes.remove(at.getOid());
            for (Iterator j=at.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                attributeTypes.remove(name.toLowerCase());
            }
        }

        for (Iterator i=schema.objectClasses.values().iterator(); i.hasNext(); ) {
            ObjectClass oc = (ObjectClass)i.next();
            objectClasses.remove(oc.getOid());
            for (Iterator j=oc.getNames().iterator(); j.hasNext(); ) {
                String name = (String)j.next();
                objectClasses.remove(name.toLowerCase());
            }
        }
    }

    public void clear() {
        attributeTypes.clear();
        objectClasses.clear();
    }

    /**
     * Check if pk2 is a subset of pk1.
     */
    public boolean partialMatch(Row pk1, Row pk2) throws Exception {

        for (Iterator i=pk2.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object v1 = pk1.get(name);
            Object v2 = pk2.get(name);
            //log.debug("   - comparing "+name+": ["+v1+"] ["+v2+"]");

            if (v1 == null && v2 == null) {
                continue;

            } else if (v1 == null || v2 == null) {
                return false;

            } else  if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if row is a subset of av.
     */
    public boolean partialMatch(AttributeValues av, Row row) throws Exception {

        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = av.get(name);
            Object value = row.get(name);

            if (values == null && value == null) {
                continue;

            } else if (values == null || value == null) {
                return false;

            } else {
                boolean found = false;
                for (Iterator j=values.iterator(); j.hasNext() && !found; ) {
                    Object v = j.next();
                    //log.debug("comparing ["+v+"] with ["+value+"]: "+v.toString().equalsIgnoreCase(value.toString()));
                    if (v.toString().equalsIgnoreCase(value.toString())) found = true;
                }
                if (!found) return false;
            }
        }

        return true;
    }

    public boolean match(Row pk1, Row pk2) throws Exception {

        if (!pk1.getNames().equals(pk2.getNames())) return false;

        for (Iterator i=pk2.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object v1 = pk1.get(name);
            Object v2 = pk2.get(name);

            if (v1 == null && v2 == null) {
                continue;

            } else if (v1 == null || v2 == null) {
                return false;

            } else  if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    public Row normalize(Row row) throws Exception {

        Row newRow = new Row();

        for (Iterator i=row.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = row.get(name);

            if (value == null) continue;

            if (value instanceof String) {
                value = ((String)value).toLowerCase();
            }

            //value = value.toString().toLowerCase();
            newRow.set(name, value);
        }

        return newRow;
    }

    public String normalize(String dn) throws Exception {
        String newDn = null;

        if (dn == null) return newDn;

        Collection rdns = EntryUtil.parseDn(dn);
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            Row rdn = (Row)i.next();
            Row newRdn = normalize(rdn);
            newDn = EntryUtil.append(newDn, newRdn);
        }

        return newDn;
    }

    public int hashCode() {
        return (schemaConfig == null ? 0 : schemaConfig.hashCode()) +
                (attributeTypes == null ? 0 : attributeTypes.hashCode()) +
                (objectClasses == null ? 0 : objectClasses.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        Schema schema = (Schema)object;
        if (!equals(schemaConfig, schema.schemaConfig)) return false;
        if (!equals(attributeTypes, schema.attributeTypes)) return false;
        if (!equals(objectClasses, schema.objectClasses)) return false;

        return true;
    }

    public void copy(Schema schema) {
        if (schema.schemaConfig != null) {
            if (schemaConfig == null) {
                schemaConfig = (SchemaConfig)schema.schemaConfig.clone();
            } else {
                schemaConfig.copy(schema.schemaConfig);
            }
        }

        attributeTypes.clear();
        for (Iterator i=schema.getAttributeTypes().iterator(); i.hasNext(); ) {
            AttributeType at = (AttributeType)i.next();
            addAttributeType((AttributeType)at.clone());
        }

        objectClasses.clear();
        for (Iterator i=schema.getObjectClasses().iterator(); i.hasNext(); ) {
            ObjectClass oc = (ObjectClass)i.next();
            addObjectClass((ObjectClass)oc.clone());
        }
    }

    public Object clone() {
        Schema schema = new Schema();
        schema.copy(this);
        return schema;
    }

    public SchemaConfig getSchemaConfig() {
        return schemaConfig;
    }

    public void setSchemaConfig(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }
}
