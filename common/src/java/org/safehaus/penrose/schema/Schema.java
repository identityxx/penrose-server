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

import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.ldap.RDN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Schema implements Serializable, Cloneable {

    private String name;

    protected Map<String,AttributeType> attributeTypes = new TreeMap<String,AttributeType>();
    protected Map<String,AttributeType> attributeTypesByName = new TreeMap<String,AttributeType>();
    protected Map<String,AttributeType> attributeTypesByOid = new TreeMap<String,AttributeType>();

    protected Map<String,ObjectClass> objectClasses = new TreeMap<String,ObjectClass>();
    protected Map<String,ObjectClass> objectClassesByName = new TreeMap<String,ObjectClass>();
    protected Map<String,ObjectClass> objectClassesByOid = new TreeMap<String,ObjectClass>();

    public Schema(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Collection<AttributeType> getAttributeTypes() {
        return attributeTypesByName.values();
    }

    public Collection<String> getAttributeTypeNames() {
        return attributeTypesByName.keySet();
    }

    public AttributeType getAttributeType(String name) {
        return attributeTypes.get(name.toLowerCase());
    }

    public void addAttributeType(AttributeType at) {

        Logger log = LoggerFactory.getLogger(getClass());

        if (log.isDebugEnabled()) log.debug("Adding attribute type "+at.getName()+" ("+at.getOid()+")");

        attributeTypesByName.put(at.getName(), at);
        attributeTypesByOid.put(at.getOid(), at);

        attributeTypes.put(at.getOid(), at);
        for (String name : at.getNames()) {
            attributeTypes.put(name.toLowerCase(), at);
        }
    }

    public void updateAttributeType(String name, AttributeType at) {
        removeAttributeType(name);
        addAttributeType(at);
    }

    public AttributeType removeAttributeType(String name) {
        AttributeType at = attributeTypes.get(name.toLowerCase());
        if (at == null) return null;

        attributeTypesByName.remove(at.getName());
        attributeTypesByOid.remove(at.getOid());

        attributeTypes.remove(at.getOid());
        for (String atName : at.getNames()) {
            attributeTypes.remove(atName.toLowerCase());
        }
        return at;
    }

    public Collection<ObjectClass> getObjectClasses() {
        return objectClassesByName.values();
    }

    public Collection<String> getObjectClassNames() {
        return objectClassesByName.keySet();
    }

    public ObjectClass getObjectClass(String name) {
        return objectClasses.get(name.toLowerCase());
    }

    public void addObjectClass(ObjectClass oc) {

        Logger log = LoggerFactory.getLogger(getClass());

        if (log.isDebugEnabled()) log.debug("Adding object class "+oc.getName()+" ("+oc.getOid()+")");

        objectClassesByName.put(oc.getName(), oc);
        objectClassesByOid.put(oc.getOid(), oc);

        objectClasses.put(oc.getOid(), oc);
        for (String name : oc.getNames()) {
            objectClasses.put(name.toLowerCase(), oc);
        }
    }

    public void updateObjectClass(String name, ObjectClass objectClass) {
        removeObjectClass(name);
        addObjectClass(objectClass);
    }

    public ObjectClass removeObjectClass(String name) {
        ObjectClass oc = objectClasses.get(name.toLowerCase());
        if (oc == null) return null;

        objectClassesByName.remove(oc.getName());
        objectClassesByOid.remove(oc.getOid());

        objectClasses.remove(oc.getOid());
        for (String ocName : oc.getNames()) {
            objectClasses.remove(ocName.toLowerCase());
        }
        return oc;
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

    public Collection<ObjectClass> getObjectClasses(EntryConfig entryConfig) {
        Map<String,ObjectClass> map = new HashMap<String,ObjectClass>();
        for (String ocName : entryConfig.getObjectClasses()) {
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
        attributeTypesByName.putAll(schema.attributeTypesByName);
        attributeTypesByOid.putAll(schema.attributeTypesByOid);
        attributeTypes.putAll(schema.attributeTypes);

        objectClassesByName.putAll(schema.objectClassesByName);
        objectClassesByOid.putAll(schema.objectClassesByOid);
        objectClasses.putAll(schema.objectClasses);
    }

    public void remove(Schema schema) {
        for (AttributeType at : schema.attributeTypesByName.values()) {
            attributeTypesByName.remove(at.getName());
            attributeTypesByOid.remove(at.getOid());

            attributeTypes.remove(at.getOid());
            for (String name : at.getNames()) {
                attributeTypes.remove(name.toLowerCase());
            }
        }

        for (ObjectClass oc : schema.objectClassesByName.values()) {
            objectClassesByName.remove(oc.getName());
            objectClassesByOid.remove(oc.getOid());

            objectClasses.remove(oc.getOid());
            for (String name : oc.getNames()) {
                objectClasses.remove(name.toLowerCase());
            }
        }
    }

    public void clear() {
        attributeTypesByName.clear();
        attributeTypesByOid.clear();
        attributeTypes.clear();

        objectClassesByName.clear();
        objectClassesByOid.clear();
        objectClasses.clear();
    }

    public boolean partialMatch(RDN pk1, RDN pk2) throws Exception {

        for (String name : pk2.getNames()) {
            Object v1 = pk1.get(name);
            Object v2 = pk2.get(name);
            //log.debug("   - comparing "+name+": ["+v1+"] ["+v2+"]");

            if (v1 == null && v2 == null) {

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

            } else if (v1 == null || v2 == null) {
                return false;

            } else if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (attributeTypesByName == null ? 0 : attributeTypesByName.hashCode()) +
                (objectClassesByName == null ? 0 : objectClassesByName.hashCode());
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Schema schema = (Schema)object;
        if (!equals(name, schema.name)) return false;
        if (!equals(attributeTypesByName, schema.attributeTypesByName)) return false;
        if (!equals(objectClassesByName, schema.objectClassesByName)) return false;

        return true;
    }

    public void copy(Schema schema) throws CloneNotSupportedException {

        name = schema.name;

        attributeTypes = new TreeMap<String,AttributeType>();
        attributeTypesByName = new TreeMap<String,AttributeType>();
        attributeTypesByOid = new TreeMap<String,AttributeType>();
        for (AttributeType attributeType : schema.getAttributeTypes()) {
            addAttributeType((AttributeType)attributeType.clone());
        }

        objectClasses = new TreeMap<String,ObjectClass>();
        objectClassesByName = new TreeMap<String,ObjectClass>();
        objectClassesByOid = new TreeMap<String,ObjectClass>();
        for (ObjectClass objectClass : schema.getObjectClasses()) {
            addObjectClass((ObjectClass)objectClass.clone());
        }
    }

    public Object clone() throws CloneNotSupportedException {
        Schema schema = (Schema)super.clone();
        schema.copy(this);
        return schema;
    }
}
