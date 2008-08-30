package org.safehaus.penrose.jdbc.source;

import java.util.Collection;
import java.util.LinkedHashSet;

/**
 * @author Endi Sukma Dewata
 */
public class SQLOperation {
    
    private String action;
    private Collection<String> parameters = new LinkedHashSet<String>();
    private String statement;

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public Collection<String> getParameters() {
        return parameters;
    }

    public void setParameters(Collection<String> parameters) {
        this.parameters.clear();
        if (parameters == null) return;
        this.parameters.addAll(parameters);
    }

    public void addParameter(String parameter) {
        parameters.add(parameter);
    }

    public void removeParameter(String parameter) {
        parameters.remove(parameter);
    }

    public String getStatement() {
        return statement;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }
}
