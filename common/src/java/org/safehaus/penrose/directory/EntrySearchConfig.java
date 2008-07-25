package org.safehaus.penrose.directory;

import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;

import java.io.Serializable;

/**
 * @author Endi Sukma Dewata
 */
public class EntrySearchConfig implements Serializable, Cloneable {

    protected String name;
    protected Filter filter;
    protected String script;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setStringFilter(String filter) throws Exception {
        this.filter = FilterTool.parseFilter(filter);
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public String getScript() {
        return script;
    }

    public void setScript(String script) {
        this.script = script;
    }

    public int hashCode() {
        return name == null ? 0 : name.hashCode();
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

        EntrySearchConfig searchConfig = (EntrySearchConfig)object;
        if (!equals(name, searchConfig.name)) return false;
        if (!equals(filter, searchConfig.filter)) return false;
        if (!equals(script, searchConfig.script)) return false;

        return true;
    }

    public void copy(EntrySearchConfig searchConfig) throws CloneNotSupportedException {
        name = searchConfig.name;
        filter = searchConfig.filter;
        script = searchConfig.script;
    }

    public Object clone() throws CloneNotSupportedException {
        EntrySearchConfig searchConfig = (EntrySearchConfig)super.clone();
        searchConfig.copy(this);
        return searchConfig;
    }
}
