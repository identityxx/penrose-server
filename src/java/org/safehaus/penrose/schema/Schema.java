/**
 * Copyright (c) 2000-2005, Identyx Corporation.
 * All rights reserved.
 */
package org.safehaus.penrose.schema;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.mapping.Row;
import org.safehaus.penrose.mapping.AttributeValues;
import org.safehaus.penrose.Penrose;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Schema {

    Logger log = LoggerFactory.getLogger(getClass());
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
    public Map getAttributeTypes() {
        return attributeTypes;
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
    public Map getObjectClasses() {
        return objectClasses;
    }

    /**
     * @param objectClasses
     *            The objectClassMap to set.
     */
    public void setObjectClasses(Map objectClasses) {
        this.objectClasses = objectClasses;
    }

    public Set getRequiredAttributeNames(EntryDefinition entry) {
        Set set = new HashSet();
        for (Iterator i=entry.getObjectClasses().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();
            ObjectClass oc = (ObjectClass)objectClasses.get(ocName);
            if (oc == null) continue;

            set.addAll(oc.getRequiredAttributes());
        }

        return set;
    }

    public List getAllObjectClassNames(EntryDefinition entry) {
        List list = new ArrayList();

        for (Iterator i=entry.getObjectClasses().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();

            getAllObjectClassNames(list, ocName);
        }

        return list;
    }

    public List getAllObjectClassNames(String ocName) {
        List list = new ArrayList();
        getAllObjectClassNames(list, ocName);
        return list;
    }

    public void getAllObjectClassNames(List list, String ocName) {
    	if (list.contains(ocName)) return;

        ObjectClass oc = (ObjectClass)objectClasses.get(ocName);
        if (oc == null) return;

    	List superClasses = oc.getSuperClasses();
    	for (Iterator i=superClasses.iterator(); i.hasNext(); ) {
    		String supName = (String)i.next();

            getAllObjectClassNames(list, supName);
    	}

        list.add(ocName);
    }

    public boolean isSuperClass(String parent, String child) {
        if (parent.equals(child)) return true;

        ObjectClass oc = (ObjectClass)objectClasses.get(child);
        if (oc == null) return false;

        List superClasses = oc.getSuperClasses();
        for (Iterator i=superClasses.iterator(); i.hasNext(); ) {
            String supName = (String)i.next();
            //log.debug(" - comparing "+parent+" with "+supName+": "+supName.equals(parent));
            if (supName.equals(parent)) return true;

            boolean result = isSuperClass(parent, supName);
            if (result) return true;
        }

        return false;
    }

    public Collection getObjectClasses(EntryDefinition entry) {
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
        ObjectClass objectClass = (ObjectClass)objectClasses.get(objectClassName);
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
            //newRow.set(name.toLowerCase(), value.toString().toLowerCase());
            if (value instanceof String) {
                value = ((String)value).toLowerCase();
            }
            
            newRow.set(name, value);
        }

        return newRow;
    }

    public String normalize(String dn) throws Exception {
        if (dn == null) return null;
        return dn.toLowerCase();
    }
}
