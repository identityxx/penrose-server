package org.safehaus.penrose.jdbc;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class Request {

    protected Statement statement;

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }
}
