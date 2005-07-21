/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.schema;

import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.Penrose;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class Schema {

    protected Logger log = Logger.getLogger(Penrose.SCHEMA_LOGGER);
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
            log.debug(" - comparing "+parent+" with "+supName+": "+supName.equals(parent));
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
        log.debug("Searching for object class "+objectClassName+" ... ");
        ObjectClass objectClass = (ObjectClass)objectClasses.get(objectClassName);
        if (objectClass == null) {
            log.debug("--> NOT FOUND");
            return;
        }

        log.debug("--> FOUND");
        map.put(objectClassName, objectClass);

        if (objectClass.getSuperClasses() == null) return;

        // add all superclasses
        for (Iterator i=objectClass.getSuperClasses().iterator(); i.hasNext(); ) {
            String ocName = (String)i.next();
            getAllObjectClasses(ocName, map);
        }
    }
}
