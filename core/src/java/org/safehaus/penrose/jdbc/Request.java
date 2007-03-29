package org.safehaus.penrose.jdbc;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Request {

    protected Statement statement;
    protected Collection parameters;

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public Collection getParameters() {
        return parameters;
    }

    public void setParameters(Collection parameters) {
        this.parameters = parameters;
    }
}
