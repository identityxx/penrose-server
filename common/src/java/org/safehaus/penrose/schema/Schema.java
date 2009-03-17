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

import java.io.Serializable;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Schema implements Serializable, Cloneable {

    public final static long serialVersionUID = 1L;

    private String name;

    protected Set<AttributeType> attributeTypes = new LinkedHashSet<AttributeType>();
    protected Map<String,AttributeType> attributeTypesByNameOrOid = new HashMap<String,AttributeType>();

    protected Set<ObjectClass> objectClasses = new LinkedHashSet<ObjectClass>();
    protected Map<String,ObjectClass> objectClassesByNameOrOid = new TreeMap<String,ObjectClass>();

    public Schema() {
    }

    public Schema(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Attribute Types
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Collection<AttributeType> getAttributeTypes() {
        return attributeTypes;
    }

    public Collection<String> getAttributeTypeNames() {
        Collection<String> list = new TreeSet<String>();
        for (AttributeType at : attributeTypes) {
            list.addAll(at.getNames());
        }
        return list;
    }

    public Collection<String> getAttributeTypeOids() {
        Collection<String> list = new ArrayList<String>();
        for (AttributeType at : attributeTypes) {
            list.add(at.getOid());
        }
        return list;
    }

    public AttributeType getAttributeType(String name) {
        return attributeTypesByNameOrOid.get(name.toLowerCase());
    }

    public void addAttributeType(AttributeType at) {

        attributeTypes.add(at);

        attributeTypesByNameOrOid.put(at.getOid(), at);
        for (String name : at.getNames()) {
            attributeTypesByNameOrOid.put(name.toLowerCase(), at);
        }
    }

    public void updateAttributeType(String name, AttributeType at) {
        removeAttributeType(name);
        addAttributeType(at);
    }

    public AttributeType removeAttributeType(String name) {
        AttributeType at = getAttributeType(name.toLowerCase());
        if (at == null) return null;

        removeAttributeType(at);
        return at;
    }

    public void removeAttributeType(AttributeType at) {

        attributeTypes.remove(at);

        attributeTypesByNameOrOid.remove(at.getOid());
        for (String atName : at.getNames()) {
            attributeTypesByNameOrOid.remove(atName.toLowerCase());
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Object Classes
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public Collection<ObjectClass> getObjectClasses() {
        return objectClasses;
    }

    public Collection<String> getObjectClassNames() {
        Collection<String> list = new TreeSet<String>();
        for (ObjectClass oc : objectClasses) {
            list.addAll(oc.getNames());
        }
        return list;
    }

    public Collection<String> getObjectClassOids() {
        Collection<String> list = new ArrayList<String>();
        for (ObjectClass oc : objectClasses) {
            list.add(oc.getOid());
        }
        return list;
    }

    public ObjectClass getObjectClass(String name) {
        return objectClassesByNameOrOid.get(name.toLowerCase());
    }

    public void addObjectClass(ObjectClass oc) {

        objectClasses.add(oc);

        objectClassesByNameOrOid.put(oc.getOid(), oc);
        for (String name : oc.getNames()) {
            objectClassesByNameOrOid.put(name.toLowerCase(), oc);
        }
    }

    public void updateObjectClass(String name, ObjectClass objectClass) {
        removeObjectClass(name);
        addObjectClass(objectClass);
    }

    public ObjectClass removeObjectClass(String name) {
        ObjectClass oc = getObjectClass(name.toLowerCase());
        if (oc == null) return null;

        removeObjectClass(oc);
        return oc;
    }

    public void removeObjectClass(ObjectClass oc) {

        objectClasses.remove(oc);

        objectClassesByNameOrOid.remove(oc.getOid());
        for (String ocName : oc.getNames()) {
            objectClassesByNameOrOid.remove(ocName.toLowerCase());
        }
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

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Matching
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public boolean partialMatch(RDN rdn1, RDN rdn2) throws Exception {

        for (String name : rdn2.getNames()) {
            Object v1 = rdn1.get(name);
            Object v2 = rdn2.get(name);

            if (v1 == null && v2 == null) {

            } else if (v1 == null || v2 == null) {
                return false;

            } else if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    public boolean match(RDN rdn1, RDN rdn2) throws Exception {

        if (!rdn1.getNames().equals(rdn2.getNames())) return false;

        for (String name : rdn2.getNames()) {
            Object v1 = rdn1.get(name);
            Object v2 = rdn2.get(name);

            if (v1 == null && v2 == null) {

            } else if (v1 == null || v2 == null) {
                return false;

            } else if (!(v1.toString()).equalsIgnoreCase(v2.toString())) {
                return false;
            }
        }

        return true;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Schema
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public void add(Schema schema) {
        attributeTypes.addAll(schema.attributeTypes);
        attributeTypesByNameOrOid.putAll(schema.attributeTypesByNameOrOid);

        objectClasses.addAll(schema.objectClasses);
        objectClassesByNameOrOid.putAll(schema.objectClassesByNameOrOid);
    }

    public void remove(Schema schema) {
        for (AttributeType at : schema.attributeTypes) {
            removeAttributeType(at);
        }

        for (ObjectClass oc : schema.objectClasses) {
            removeObjectClass(oc);
        }
    }

    public void clear() {
        attributeTypes.clear();
        attributeTypesByNameOrOid.clear();

        objectClasses.clear();
        objectClassesByNameOrOid.clear();
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
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
        if (object == null) return false;
        if (object.getClass() != this.getClass()) return false;

        Schema schema = (Schema)object;
        if (!equals(name, schema.name)) return false;
        if (!equals(attributeTypes, schema.attributeTypes)) return false;
        if (!equals(objectClasses, schema.objectClasses)) return false;

        return true;
    }

    public void copy(Schema schema) throws CloneNotSupportedException {

        name = schema.name;

        attributeTypes = new LinkedHashSet<AttributeType>();
        attributeTypesByNameOrOid = new TreeMap<String,AttributeType>();
        for (AttributeType attributeType : schema.getAttributeTypes()) {
            addAttributeType((AttributeType)attributeType.clone());
        }

        objectClasses = new LinkedHashSet<ObjectClass>();
        objectClassesByNameOrOid = new TreeMap<String,ObjectClass>();
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
