package org.safehaus.penrose.jdbc.source;

import org.safehaus.penrose.ldap.SearchRequest;
import org.safehaus.penrose.ldap.SearchResponse;
import org.safehaus.penrose.jdbc.SelectStatement;
import org.safehaus.penrose.jdbc.JoinClause;
import org.safehaus.penrose.jdbc.connection.RequestBuilder;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.FilterTool;
import org.safehaus.penrose.filter.SimpleFilter;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class NewSearchRequestBuilder extends RequestBuilder {

    JDBCJoinSource joinSource;

    SearchRequest request;
    SearchResponse response;

    NewSearchFilterBuilder filterBuilder;

    public NewSearchRequestBuilder(
            JDBCJoinSource joinSource,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        this.joinSource = joinSource;

        this.request = request;
        this.response = response;

        filterBuilder = new NewSearchFilterBuilder(
                joinSource
        );
    }

    public String generateJoinType(String alias) {
        return joinSource.getJoinType(alias);
    }

    public Filter generateJoinCondition(String alias) throws Exception {

        String joinCondition = joinSource.getJoinCondition(alias);
        String[] list = joinCondition.split(" ");

        return new SimpleFilter(list[0], list[1], list[2]);
    }

    public Filter generateJoinCondition(String alias, String newAlias) throws Exception {

        JDBCSource source = joinSource.getSource(alias);

        Filter filter = null;

        if (alias.equals(joinSource.getPrimarySourceAlias())) {

            for (Field field : source.getPrimaryKeyFields()) {

                String lhs = newAlias + "." + field.getOriginalName();
                String rhs = alias + "." + field.getOriginalName();

                SimpleFilter sf = new SimpleFilter(lhs, "=", rhs);
                filter = FilterTool.appendAndFilter(filter, sf);
            }

        } else {

            String joinCondition = joinSource.getJoinCondition(alias);
            joinCondition = joinCondition.replaceAll(alias+".", newAlias+".");

            String[] list = joinCondition.split(" ");

            filter = new SimpleFilter(list[0], list[1], list[2]);
        }

        return filter;
    }

    public String generateJoinFilter(JDBCSource source, String alias) throws Exception {

        String filter = source.getFilter();
        String table = source.getTable();

        if (filter != null) {
            filter = filter.replaceAll(table+"\\.", alias+"\\.");
        }

        return filter;
    }

    public SelectStatement generate() throws Exception {

        SelectStatement statement = new SelectStatement();

        if (debug) log.debug("Generating select statement.");

        if (debug) log.debug("Columns:");

        for (Field field : joinSource.getFields()) {
            String column = field.getVariable()+" "+field.getName();
            if (debug) log.debug(" - "+column);

            statement.addColumn(column);

            if (field.isPrimaryKey()) {
                statement.addOrder(field.getName());
            }
        }

        if (debug) log.debug("Order by "+statement.getOrders());

        Collection<String> whereClauses = new ArrayList<String>();

        for (String alias : joinSource.getSourceAliases()) {

            JDBCSource source = joinSource.getSource(alias);
            if (debug) log.debug("Table "+alias+": "+source.getTable());

            statement.addSource(alias, source.getPartition().getName(), source.getName());

            if (joinSource.isPrimarySourceAlias(alias)) {
                String where = source.getFilter();
                if (where != null) {
                    where = where.replaceAll(source.getName()+".", alias+".");
                    whereClauses.add(where);
                }

            } else { // add filter of first source
                String joinType = generateJoinType(alias);
                if (debug) log.debug(" - join type: "+joinType);

                Filter joinCondition = generateJoinCondition(alias);
                if (debug) log.debug(" - join on: "+joinCondition);

                String joinWhere = generateJoinFilter(source, alias);
                if (debug) log.debug(" - join filter: "+joinWhere);

                JoinClause joinClause = new JoinClause();
                joinClause.setType(joinType);
                joinClause.setCondition(joinCondition);
                joinClause.setWhere(joinWhere);

                statement.addJoin(joinClause);
            }
        }

        Filter filter = filterBuilder.convert(request.getFilter());
        if (debug) log.debug("Filter: "+filter);

        statement.setFilter(filter);

        for (String newAlias : filterBuilder.getNewAliases()) {
            String alias = filterBuilder.getOriginalAlias(newAlias);
            JDBCSource source = joinSource.getSource(alias);

            if (debug) log.debug("Table "+newAlias+": "+source.getTable());
            statement.addSource(newAlias, source.getPartition().getName(), source.getName());

            String joinType = generateJoinType(alias);
            if (debug) log.debug(" - join type: "+joinType);

            Filter joinCondition = generateJoinCondition(alias, newAlias);
            if (debug) log.debug("   - join on: "+joinCondition);
            if (joinCondition == null) continue;

            String joinWhere = generateJoinFilter(source, newAlias);
            if (debug) log.debug("   - join filter: "+joinWhere);

            JoinClause joinClause = new JoinClause();
            joinClause.setType(joinType);
            joinClause.setCondition(joinCondition);
            joinClause.setWhere(joinWhere);

            statement.addJoin(joinClause);
        }

        if (!whereClauses.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for (String whereClause : whereClauses) {
                if (sb.length() > 0) sb.append(" and ");
                sb.append("(");
                sb.append(whereClause);
                sb.append(")");
            }
            statement.setWhereClause(sb.toString());
        }

        requests.add(statement);

        return statement;
    }
}