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

    Logger log = LoggerFactory.getLogger(getClass());

    protected Source source;
    protected Map sourceRefs = new LinkedHashMap(); // need to maintain order

    private String sql;
    private Collection<Assignment> assignments = new ArrayList<Assignment>();

    public JDBCFilterBuilder() throws Exception {
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

        log.debug("Simple Filter: "+name+" "+operator+" "+value);
        
        Field field;

        if (source == null) {
            int i = name.indexOf('.');
            String sourceName = name.substring(0, i);
            String fieldName = name.substring(i+1);

            SourceRef sourceRef = (SourceRef)sourceRefs.get(sourceName);
            Source s = sourceRef.getSource();
            field = s.getField(fieldName);
            name = sourceName+"."+ field.getOriginalName();

        } else {
            field = source.getField(name);
            name = field.getOriginalName();
        }

        generate(
                field,
                name,
                operator,
                "?",
                sb
        );

        assignments.add(new Assignment(field, value));
    }

    public void generate(
            Field field,
            String lhs,
            String operator,
            String rhs,
            StringBuilder sb
    ) throws Exception {

        if ("VARCHAR".equals(field.getType()) && !field.isCaseSensitive()) {
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
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

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
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

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

    public Collection getSourceAliases() {
        return sourceRefs.keySet();
    }

    public SourceRef getSourceRef(String alias) {
        return (SourceRef)sourceRefs.get(alias);
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
}
