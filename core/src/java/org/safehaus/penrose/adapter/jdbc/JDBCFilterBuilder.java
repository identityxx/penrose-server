package org.safehaus.penrose.adapter.jdbc;

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

    protected Source source;
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

    public JDBCFilterBuilder(
            Source source
    ) throws Exception {
        this.source = source;
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

        Field lField;
        String lhs;

        if (source == null) {
            int i = name.indexOf('.');
            String sourceName = name.substring(0, i);
            String fieldName = name.substring(i+1);

            if (fieldName.startsWith("primaryKey.")) fieldName = fieldName.substring(11);

            SourceRef sourceRef = sourceRefs.get(sourceName);
            Source s = sourceRef.getSource();

            lField = s.getField(fieldName);
            if (lField == null) throw new Exception("Unknown field: "+name);

            StringBuilder sb1 = new StringBuilder();
            sb1.append(sourceName);
            sb1.append(".");

            if (quote != null) sb1.append(quote);
            sb1.append(lField.getOriginalName());
            if (quote != null) sb1.append(quote);

            lhs = sb1.toString();

        } else {
            int i = name.indexOf('.');
            String fieldName = i >= 0 ? name.substring(i+1) : name;

            lField = source.getField(fieldName);
            if (lField == null) throw new Exception("Unknown field: "+name);

            StringBuilder sb1 = new StringBuilder();
            if (quote != null) sb1.append(quote);
            sb1.append(lField.getOriginalName());
            if (quote != null) sb1.append(quote);

            lhs = sb1.toString();
        }

        Field rField;
        String rhs;

        if (extractValues) {
            rhs = "?";
            assignments.add(new Assignment(lField, value));

        } else {
            rhs = value.toString();

            if (source == null) {
                int i = rhs.indexOf('.');
                String sourceName = rhs.substring(0, i);
                String fieldName = rhs.substring(i+1);

                SourceRef sourceRef = sourceRefs.get(sourceName);
                Source s = sourceRef.getSource();

                rField = s.getField(fieldName);
                if (rField == null) throw new Exception("Unknown field: "+rhs);

                StringBuilder sb1 = new StringBuilder();
                sb1.append(sourceName);
                sb1.append(".");

                if (quote != null) sb1.append(quote);
                sb1.append(rField.getOriginalName());
                if (quote != null) sb1.append(quote);

                rhs = sb1.toString();

            } else {
                int i = rhs.indexOf('.');
                String fieldName = i >= 0 ? rhs.substring(i+1) : rhs;

                rField = source.getField(fieldName);
                if (rField == null) throw new Exception("Unknown field: "+rhs);

                StringBuilder sb1 = new StringBuilder();
                if (quote != null) sb1.append(quote);
                sb1.append(rField.getOriginalName());
                if (quote != null) sb1.append(quote);

                rhs = sb1.toString();
            }
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

    public Source getSource() {
        return source;
    }

    public void setSource(Source source) {
        this.source = source;
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
