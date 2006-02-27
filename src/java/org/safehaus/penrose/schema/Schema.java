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
import org.safehaus.penrose.mapping.AttributeValues;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Schema {

    Logger log = Logger.getLogger(getClass());
    /**
     * Attribute type definitions.
     */
    protected Map attributeTypes = new TreeMap();

    /**
     * Object class definitions.
     */
    protected Map objectClasses = new TreeMap();

    /**
     * @return Returns the attributeTypes.
     */
    public Collection getAttributeTypes() {
        return attributeTypes.values();
    }

    public AttributeType getAttributeType(String name) {
        return (AttributeType)attributeTypes.get(name.toLowerCase());
    }

    public void addAttributeType(AttributeType at) {
        attributeTypes.put(at.getName().toLowerCase(), at);
    }

    public AttributeType removeAttributeType(String name) {
        return (AttributeType)attributeTypes.remove(name.toLowerCase());
    }

    /**
     * @param attributeTypes
     *            The attributes to set.
     */
    public void setAttributeTypes(Map attributeTypes) {
        this.attributeTypes = attributeTypes;
    }

    /**
     * @return Returns the objectClassMap.
     */
    public Collection getObjectClasses() {
        return objectClasses.values();
    }

    public ObjectClass getObjectClass(String name) {
        return (ObjectClass)objectClasses.get(name.toLowerCase());
    }

    public void addObjectClass(ObjectClass oc) {
        objectClasses.put(oc.getName().toLowerCase(), oc);
    }

    public ObjectClass removeObjectClass(String name) {
        return (ObjectClass)objectClasses.remove(name.toLowerCase());
    }

    /**
     * @param objectClasses
     *            The objectClassMap to set.
     */
    public void setObjectClasses(Map objectClasses) {
        this.objectClasses = objectClasses;
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
        if ("top".equals(objectClassName)) return;
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
        if (dn == null) return null;
        return dn.toLowerCase();
    }
}
