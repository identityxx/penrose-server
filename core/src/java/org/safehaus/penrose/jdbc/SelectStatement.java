package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.directory.SourceRef;
import org.safehaus.penrose.directory.FieldRef;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SelectStatement extends Statement {

    protected Collection<FieldRef> fieldRefs = new ArrayList<FieldRef>();

    protected Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>(); // maintain source order
    protected Collection<JoinClause> joinClauses = new ArrayList<JoinClause>();

    protected Filter filter;
    private String where;

    protected Collection<FieldRef> orders = new ArrayList<FieldRef>();

    public Collection<FieldRef> getFieldRefs() {
        return fieldRefs;
    }

    public void addFieldRef(FieldRef fieldRef) {
        fieldRefs.add(fieldRef);
    }

    public void addFieldRefs(Collection<FieldRef> fieldRefs) {
        this.fieldRefs.addAll(fieldRefs);
    }

    public void setFieldRefs(Collection<FieldRef> fieldRefs) {
        if (this.fieldRefs == fieldRefs) return;
        this.fieldRefs.clear();
        this.fieldRefs.addAll(fieldRefs);
    }

    public Collection<String> getSourceAliases() {
        return sourceRefs.keySet();
    }

    public SourceRef getSourceRef() {
        if (sourceRefs.isEmpty()) return null;
        return sourceRefs.values().iterator().next();
    }

    public SourceRef getSourceRef(String alias) {
        return sourceRefs.get(alias);
    }

    public void addSourceRef(SourceRef sourceRef) {
        sourceRefs.put(sourceRef.getAlias(), sourceRef);
    }

    public void addSourceRef(String alias, SourceRef sourceRef) {
        sourceRefs.put(alias, sourceRef);
    }

    public void addJoin(JoinClause joinClause) {
        joinClauses.add(joinClause);
    }

    public Collection<JoinClause> getJoinClauses() {
        return joinClauses;
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public Collection<FieldRef> getOrders() {
        return orders;
    }

    public void addOrder(FieldRef fieldRef) {
        orders.add(fieldRef);
    }

    public void addOrders(Collection<FieldRef> orders) {
        this.orders.addAll(orders);
    }

    public void setOrders(Collection<FieldRef> orders) {
        if (this.orders == orders) return;
        this.orders.clear();
        this.orders.addAll(orders);
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }
}
