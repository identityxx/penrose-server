package org.safehaus.penrose.jdbc;

import org.safehaus.penrose.filter.Filter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SelectStatement extends Statement {

    protected Collection<String> columnNames = new ArrayList<String>();

    protected Map<String,StatementSource> sources = new LinkedHashMap<String,StatementSource>(); // maintain source order
    protected Collection<JoinClause> joinClauses = new ArrayList<JoinClause>();

    protected Filter filter;
    private String where;

    protected Collection<String> orders = new ArrayList<String>();

    public Collection<String> getColumnNames() {
        return columnNames;
    }

    public void addColumn(String columnName) {
        columnNames.add(columnName);
    }

    public void setColumnNames(Collection<String> columnNames) {
        if (this.columnNames == columnNames) return;
        this.columnNames.clear();
        if (columnNames == null) return;
        this.columnNames.addAll(columnNames);
    }

    public Collection<String> getSourceAliases() {
        return sources.keySet();
    }

    public StatementSource getSource(String alias) {
        return sources.get(alias);
    }

    public void addSourceName(String alias, String partitionName, String sourceName) {

        StatementSource source = new StatementSource();
        source.setAlias(alias);
        source.setPartitionName(partitionName);
        source.setSourceName(sourceName);

        sources.put(alias, source);
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

    public Collection<String> getOrders() {
        return orders;
    }

    public void addOrder(String order) {
        orders.add(order);
    }

    public void addOrders(Collection<String> orders) {
        this.orders.addAll(orders);
    }

    public void setOrders(Collection<String> orders) {
        if (this.orders == orders) return;
        this.orders.clear();
        if (orders == null) return;
        this.orders.addAll(orders);
    }

    public String getWhere() {
        return where;
    }

    public void setWhere(String where) {
        this.where = where;
    }
}
