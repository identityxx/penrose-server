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
import org.safehaus.penrose.ldap.RDN;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Schema implements Cloneable {

    Logger log = LoggerFactory.getLogger(getClass());

    private SchemaConfig schemaConfig;

    protected Map<String,AttributeType> attributeTypes = new TreeMap<String,AttributeType>();
    protected Map<String,ObjectClass> objectClasses = new TreeMap<String,ObjectClass>();

    public Schema() {
    }

    public Schema(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    public String getName() {
        return schemaConfig == null ? null : schemaConfig.getName();
    }
    
    public Collection<AttributeType> getAttributeTypes() {
        Collection<AttributeType> list = new ArrayList<AttributeType>();
        list.addAll(attributeTypes.values());
        return list;
    }

    public Collection<String> getAttributeTypeNames() {
        Collection<String> names = new TreeSet<String>();
        for (AttributeType attributeType : attributeTypes.values()) {
            names.add(attributeType.getName());
        }
        return names;
    }

    public AttributeType getAttributeType(String name) {
        return attributeTypes.get(name.toLowerCase());
    }

    public void addAttributeType(AttributeType at) {
        attributeTypes.put(at.getOid(), at);
        for (String name : at.getNames()) {
            attributeTypes.put(name.toLowerCase(), at);
        }
    }

    public void removeAttributeTypes(Collection<String> names) {
        for (String name : names) {
            removeAttributeType(name);
        }
    }

    public AttributeType removeAttributeType(String name) {
        AttributeType at = attributeTypes.get(name.toLowerCase());
        if (at == null) return null;
        attributeTypes.remove(at.getOid());
        for (String atName : at.getNames()) {
            attributeTypes.remove(atName.toLowerCase());
        }
        return at;
    }

    public Collection<ObjectClass> getObjectClasses() {
        Collection<ObjectClass> list = new ArrayList<ObjectClass>();
        list.addAll(objectClasses.values());
        return list;
    }

    public Collection<String> getObjectClassNames() {
        Collection<String> names = new TreeSet<String>();
        for (ObjectClass objectClass : objectClasses.values()) {
            names.add(objectClass.getName());
        }
        return names;
    }

    public ObjectClass getObjectClass(String name) {
        return objectClasses.get(name.toLowerCase());
    }

    public void addObjectClass(ObjectClass oc) {
        objectClasses.put(oc.getOid(), oc);
        for (String name : oc.getNames()) {
            objectClasses.put(name.toLowerCase(), oc);
        }
    }

    public void removeObjectClasses(Collection<String> names) {
        for (String name : names) {
            removeObjectClass(name);
        }
    }

    public ObjectClass removeObjectClass(String name) {
        ObjectClass oc = objectClasses.get(name.toLowerCase());
        if (oc == null) return null;
        objectClasses.remove(oc.getOid());
        for (String ocName : oc.getNames()) {
            objectClasses.remove(ocName.toLowerCase());
        }
        return oc;
    }

    public Set getRequiredAttributeNames(EntryMapping entry) {
        Set<String> set = new HashSet<String>();
        for (String ocName : entry.getObjectClasses()) {
            ObjectClass oc = getObjectClass(ocName);
            if (oc == null) continue;

            set.addAll(oc.getRequiredAttributes());
        }

        return set;
    }

    public Collection<String> getAllObjectClassNames(EntryMapping entry) {
        Collection<String> list = new ArrayList<String>();

        for (String ocName : entry.getObjectClasses()) {
            getAllObjectClassNames(list, ocName);
        }

        return list;
    }

    public Collection<String> getAllObjectClassNames(String ocName) {
        Collection<String> list = new ArrayList<String>();
        getAllObjectClassNames(list, ocName);
        return list;
    }

    public void getAllObjectClassNames(Collection<String> list, String ocName) {
    	if (list.contains(ocName)) return;

        ObjectClass oc = getObjectClass(ocName);
        if (oc == null) return;

    	Collection<String> superClasses = oc.getSuperClasses();
        for (String supName : superClasses) {
            getAllObjectClassNames(list, supName);
        }

        list.add(ocName);
    }

    public boolean isSuperClass(String parent, String child) {
        if (parent.equals(child)) return true;

        ObjectClass oc = getObjectClass(child);
        if (oc == null) return false;

        Collection<String> superClasses = oc.getSuperClasses();
        for (String supName : superClasses) {
            //log.debug(" - comparing "+parent+" with "+supName+": "+supName.equals(parent));
            if (supName.equals(parent)) return true;

            boolean result = isSuperClass(parent, supName);
            if (result) return true;
        }

        return false;
    }

    public Collection<ObjectClass> getObjectClasses(EntryMapping entry) {
        Map<String,ObjectClass> map = new HashMap<String,ObjectClass>();
        for (String ocName : entry.getObjectClasses()) {
            getAllObjectClasses(ocName, map);
        }

        return map.values();
    }

    public Collection<ObjectClass> getAllObjectClasses(String objectClassName) {
        Map<String,ObjectClass> map = new HashMap<String,ObjectClass>();
        getAllObjectClasses(objectClassName, map);
        return map.values();
    }

    public void getAllObjectClasses(String objectClassName, Map<String,ObjectClass> map) {
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
        for (String ocName : objectClass.getSuperClasses()) {
            getAllObjectClasses(ocName, map);
        }
    }

    public void add(Schema schema) {
        attributeTypes.putAll(schema.attributeTypes);
        objectClasses.putAll(schema.objectClasses);
    }

    public void remove(Schema schema) {
        for (AttributeType at : schema.attributeTypes.values()) {
            attributeTypes.remove(at.getOid());
            for (String name : at.getNames()) {
                attributeTypes.remove(name.toLowerCase());
            }
        }

        for (ObjectClass oc : schema.objectClasses.values()) {
            objectClasses.remove(oc.getOid());
            for (String name : oc.getNames()) {
                objectClasses.remove(name.toLowerCase());
            }
        }
    }

    public void clear() {
        attributeTypes.clear();
        objectClasses.clear();
    }

    public boolean partialMatch(RDN pk1, RDN pk2) throws Exception {

        for (String name : pk2.getNames()) {
            Object v1 = pk1.get(name);
            Object v2 = pk2.get(name);
            //log.debug("   - comparing "+name+": ["+v1+"] ["+v2+"]");

            if (v1 == null && v2 == null) {
                continue;

            } else if (v1 == null || v2 == null) {
                return false;

            } else if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    public boolean match(RDN pk1, RDN pk2) throws Exception {

        if (!pk1.getNames().equals(pk2.getNames())) return false;

        for (String name : pk2.getNames()) {
            Object v1 = pk1.get(name);
            Object v2 = pk2.get(name);

            if (v1 == null && v2 == null) {
                continue;

            } else if (v1 == null || v2 == null) {
                return false;

            } else if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
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

    public void copy(Schema schema) throws CloneNotSupportedException {
        if (schema.schemaConfig != null) {
            if (schemaConfig == null) {
                schemaConfig = (SchemaConfig)schema.schemaConfig.clone();
            } else {
                schemaConfig.copy(schema.schemaConfig);
            }
        }

        attributeTypes.clear();
        for (AttributeType attributeType : schema.getAttributeTypes()) {
            addAttributeType((AttributeType) attributeType.clone());
        }

        objectClasses.clear();
        for (ObjectClass objectClass : schema.getObjectClasses()) {
            addObjectClass((ObjectClass) objectClass.clone());
        }
    }

    public Object clone() throws CloneNotSupportedException {
        super.clone();
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
