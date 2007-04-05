package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SelectStatement extends Statement {

    protected Collection<FieldRef> fieldRefs = new ArrayList<FieldRef>();

    protected Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>(); // maintain source order
    protected Collection<String> joinTypes = new ArrayList<String>();
    protected Collection<String> joinOns = new ArrayList<String>();

    protected Filter filter;
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
        return (SourceRef)sourceRefs.values().iterator().next();
    }

    public SourceRef getSourceRef(String alias) {
        return (SourceRef)sourceRefs.get(alias);
    }

    public void addSourceRef(SourceRef sourceRef) {
        sourceRefs.put(sourceRef.getAlias(), sourceRef);
    }

    public void addSourceRef(String alias, SourceRef sourceRef) {
        sourceRefs.put(alias, sourceRef);
    }
    
    public void addJoin(String joinType, String joinOn) {
        joinTypes.add(joinType);
        joinOns.add(joinOn);
    }
    
    public Collection getJoinTypes() {
        return joinTypes;
    }

    public void setJoinTypes(Collection<String> joinTypes) {
        if (this.joinTypes == joinTypes) return;
        this.joinTypes.clear();
        this.joinTypes.addAll(joinTypes);
    }

    public Collection getJoinOns() {
        return joinOns;
    }

    public void setJoinOns(Collection<String> joinOns) {
        if (this.joinOns == joinOns) return;
        this.joinOns.clear();
        this.joinOns.addAll(joinOns);
    }

    public Filter getFilter() {
        return filter;
    }

    public void setFilter(Filter filter) {
        this.filter = filter;
    }

    public Collection getOrders() {
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
}
