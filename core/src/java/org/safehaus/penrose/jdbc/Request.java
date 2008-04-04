package org.safehaus.penrose.jdbc;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Request implements Serializable, Cloneable {

    protected String sql;
    protected Collection<Object> parameters = new ArrayList<Object>();

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    public Collection<Object> getParameters() {
        return parameters;
    }

    public void addParameter(Object parameter) {
        parameters.add(parameter);
    }
    
    public void setParameters(Collection<Object> parameters) {
        this.parameters.clear();
        if (parameters == null) return;
        this.parameters.addAll(parameters);
    }
}
