package org.safehaus.penrose.jdbc;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class SelectStatement extends Statement {

    protected Collection fields = new ArrayList();
    protected Collection tables = new ArrayList();
    protected Collection joinTypes = new ArrayList();
    protected Collection joinOns = new ArrayList();
    protected Collection filters = new ArrayList();
    protected Collection orders = new ArrayList();

    public Collection getFields() {
        return fields;
    }

    public void setFields(Collection fields) {
        this.fields = fields;
    }

    public Collection getTables() {
        return tables;
    }

    public void setTables(Collection tables) {
        this.tables = tables;
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

    public Collection getFilters() {
        return filters;
    }

    public void setFilters(Collection filters) {
        this.filters = filters;
    }

    public Collection getOrders() {
        return orders;
    }

    public void setOrders(Collection orders) {
        this.orders = orders;
    }

    public String toList(Collection list) {
        StringBuilder sb = new StringBuilder();

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            Object object = i.next();

            if (sb.length() > 0) sb.append(", ");
            sb.append(object.toString());
        }

        return sb.toString();
    }

    public String toFilter(Collection list) {
        StringBuilder sb = new StringBuilder();

        for (Iterator i=list.iterator(); i.hasNext(); ) {
            Object object = i.next();

            if (sb.length() > 0) sb.append(" and ");
            sb.append(object.toString());
        }

        return sb.toString();
    }

    public String getSql() throws Exception {

        log.debug("Generating select statement.");

        StringBuilder sb = new StringBuilder();
        sb.append("select distinct ");
        sb.append(toList(fields));

        sb.append(" from ");

        Iterator i=tables.iterator();
        sb.append(i.next());

        for (Iterator j=joinTypes.iterator(), k=joinOns.iterator(); i.hasNext() && j.hasNext() && k.hasNext(); ) {
            String table = (String)i.next();
            String joinType = (String)j.next();
            String joinOn = (String)k.next();
            sb.append(" ");
            sb.append(joinType);
            sb.append(" ");
            sb.append(table);
            sb.append(" on ");
            sb.append(joinOn);
        }

        if (filters.size() > 0) {
            sb.append(" where ");
            sb.append(toFilter(filters));
        }

        sb.append(" order by ");
        sb.append(toList(orders));

/*
        int totalCount = response.getTotalCount();
        long sizeLimit = request.getSizeLimit();

        if (sizeLimit == 0) {
            log.debug("Retrieving all entries.");

        } else {
            int size = sizeLimit - totalCount + 1;
            if (debug) log.debug("Retrieving "+size+" entries.");

            sb.append(" limit ");
            sb.append(size);
        }
*/

        return sb.toString();
    }
}
