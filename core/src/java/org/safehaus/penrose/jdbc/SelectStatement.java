package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SelectStatement extends Statement {

    protected Collection fieldRefs = new ArrayList();

    protected Map sourceRefs = new LinkedHashMap(); // maintain source order
    protected Collection joinTypes = new ArrayList();
    protected Collection joinOns = new ArrayList();

    protected Filter filter;
    protected Collection orders = new ArrayList();

    public Collection getFieldRefs() {
        return fieldRefs;
    }

    public void addFieldRef(FieldRef fieldRef) {
        fieldRefs.add(fieldRef);
    }

    public void addFieldRefs(Collection fieldRefs) {
        this.fieldRefs.addAll(fieldRefs);
    }

    public void setFieldRefs(Collection fieldRefs) {
        this.fieldRefs = fieldRefs;
    }

    public Collection getSourceAliases() {
        return sourceRefs.keySet();
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

    public void setJoinTypes(Collection joinTypes) {
        this.joinTypes = joinTypes;
    }

    public Collection getJoinOns() {
        return joinOns;
    }

    public void setJoinOns(Collection joinOns) {
        this.joinOns = joinOns;
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

    public void setOrders(Collection orders) {
        this.orders = orders;
    }
}
