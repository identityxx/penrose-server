package org.safehaus.penrose.mapping;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EntryData {

    private String dn;
    private AttributeValues mergedValues;
    private Collection rows;
    private Row filter;
    private AttributeValues loadedSourceValues;
    private boolean complete;

    public EntryData() {
    }

    public String getDn() {
        return dn;
    }

    public void setDn(String dn) {
        this.dn = dn;
    }

    public AttributeValues getMergedValues() {
        return mergedValues;
    }

    public void setMergedValues(AttributeValues mergedValues) {
        this.mergedValues = mergedValues;
    }

    public Collection getRows() {
        return rows;
    }

    public void setRows(Collection rows) {
        this.rows = rows;
    }

    public Row getFilter() {
        return filter;
    }

    public void setFilter(Row filter) {
        this.filter = filter;
    }

    public AttributeValues getLoadedSourceValues() {
        return loadedSourceValues;
    }

    public void setLoadedSourceValues(AttributeValues loadedSourceValues) {
        this.loadedSourceValues = loadedSourceValues;
    }

    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }
}
