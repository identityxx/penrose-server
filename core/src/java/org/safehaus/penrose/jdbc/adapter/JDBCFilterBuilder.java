package org.safehaus.penrose.jdbc.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.jdbc.Assignment;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCFilterBuilder {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    protected Map<String,SourceRef> sourceRefs = new LinkedHashMap<String,SourceRef>(); // need to maintain order

    private String sql;
    private Collection<Assignment> assignments = new ArrayList<Assignment>();

    private String quote;
    public boolean extractValues = true;

    public JDBCFilterBuilder() throws Exception {
    }

    public JDBCFilterBuilder(boolean extractValues) throws Exception {
        this.extractValues = extractValues;
    }

    public void generate(Filter filter) throws Exception {
        StringBuilder sb = new StringBuilder();
        generate(filter, sb);
        sql = sb.toString();
    }

    public void generate(Filter filter, StringBuilder sb) throws Exception {

        if (filter instanceof NotFilter) {
            generate((NotFilter)filter, sb);

        } else if (filter instanceof AndFilter) {
            generate((AndFilter)filter, sb);

        } else if (filter instanceof OrFilter) {
            generate((OrFilter)filter, sb);

        } else if (filter instanceof SimpleFilter) {
            generate((SimpleFilter)filter, sb);

        } else if (filter instanceof PresentFilter) {
            generate((PresentFilter)filter, sb);
        }
    }

    public void generate(
            SimpleFilter filter,
            StringBuilder sb
    ) throws Exception {

        String name = filter.getAttribute();
        String operator = filter.getOperator();
        Object value = filter.getValue();

        if (debug) {
            String v;
            if (value instanceof byte[]) {
                v = new String((byte[])value);
            } else {
                v = value.toString();
            }
            
            log.debug("Simple Filter: "+name+" "+operator+" "+v);
        }

        int i = name.indexOf('.');
        String lsourceName = name.substring(0, i);
        String lfieldName = name.substring(i+1);

        if (lfieldName.startsWith("primaryKey.")) lfieldName = lfieldName.substring(11);

        SourceRef lsourceRef = sourceRefs.get(lsourceName);
        Source ls = lsourceRef.getSource();

        Field lField = ls.getField(lfieldName);
        if (lField == null) throw new Exception("Unknown field: "+name);

        StringBuilder sb1 = new StringBuilder();
        sb1.append(lsourceName);
        sb1.append(".");

        if (quote != null) sb1.append(quote);
        sb1.append(lField.getOriginalName());
        if (quote != null) sb1.append(quote);

        String lhs = sb1.toString();

        Field rField;
        String rhs;

        if (extractValues) {
            rhs = "?";
            assignments.add(new Assignment(lField, value));

        } else {
            rhs = value.toString();

            int j = rhs.indexOf('.');
            String rsourceName = rhs.substring(0, j);
            String rfieldName = rhs.substring(j+1);

            SourceRef rsourceRef = sourceRefs.get(rsourceName);
            Source rs = rsourceRef.getSource();

            rField = rs.getField(rfieldName);
            if (rField == null) throw new Exception("Unknown field: "+rhs);

            StringBuilder sb2 = new StringBuilder();
            sb2.append(rsourceName);
            sb2.append(".");

            if (quote != null) sb2.append(quote);
            sb2.append(rField.getOriginalName());
            if (quote != null) sb2.append(quote);

            rhs = sb2.toString();
        }

        if ("VARCHAR".equals(lField.getType()) && !lField.isCaseSensitive()) {
            sb.append("lower(");
            sb.append(lhs);
            sb.append(")");
            sb.append(" ");
            sb.append(operator);
            sb.append(" ");
            sb.append("lower(");
            sb.append(rhs);
            sb.append(")");

        } else {
            sb.append(lhs);
            sb.append(" ");
            sb.append(operator);
            sb.append(" ");
            sb.append(rhs);
        }

    }

    public void generate(
            PresentFilter filter,
            StringBuilder sb
    ) throws Exception {

        String name = filter.getAttribute();

        sb.append(name);
        sb.append(" is not null");
    }

    public void generate(
            NotFilter filter,
            StringBuilder sb
    ) throws Exception {

        StringBuilder sb2 = new StringBuilder();

        Filter f = filter.getFilter();

        generate(
                f,
                sb2
        );

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");
    }

    public void generate(
            AndFilter filter,
            StringBuilder sb
    ) throws Exception {

        StringBuilder sb2 = new StringBuilder();
        for (Filter f : filter.getFilters()) {

            StringBuilder sb3 = new StringBuilder();

            generate(
                    f,
                    sb3
            );

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" and ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0) return;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");
    }

    public void generate(
            OrFilter filter,
            StringBuilder sb
    ) throws Exception {

        StringBuilder sb2 = new StringBuilder();
        for (Filter f : filter.getFilters()) {

            StringBuilder sb3 = new StringBuilder();

            generate(
                    f,
                    sb3
            );

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" or ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0) return;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");
    }

    public Collection<String> getSourceAliases() {
        return sourceRefs.keySet();
    }

    public SourceRef getSourceRef(String alias) {
        return sourceRefs.get(alias);
    }

    public void addSourceRef(String alias, SourceRef sourceRef) {
        sourceRefs.put(alias, sourceRef);
    }

    public Collection<Assignment> getAssignments() {
        return assignments;
    }

    public void setAssignments(Collection<Assignment> assignments) {
        this.assignments = assignments;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getQuote() {
        return quote;
    }

    public void setQuote(String quote) {
        this.quote = quote;
    }
}
