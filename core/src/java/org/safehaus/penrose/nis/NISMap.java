package org.safehaus.penrose.nis;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Endi Sukma Dewata
 */
public class NISMap {
    
    public String name;
    public String description;

    public Map<String,NISObject> objects = new LinkedHashMap<String,NISObject>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Collection<String> getKeys() {
        Collection<String> results = new ArrayList<String>();
        results.addAll(objects.keySet());
        return results;
    }

    public Collection<NISObject> getObjects() {
        Collection<NISObject> results = new ArrayList<NISObject>();
        results.addAll(objects.values());
        return results;
    }

    public NISObject getObject(String key) {
        return objects.get(key);
    }

    public void addObject(NISObject object) {
        objects.put(object.getName(), object);
    }

    public NISObject removeObject(String key) {
        return objects.remove(key);
    }
}
