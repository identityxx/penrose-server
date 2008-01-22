package org.safehaus.penrose.jdbc;

import java.io.Serializable;

/**
 * @author Endi S. Dewata
 */
public class Request implements Serializable, Cloneable {

    protected Statement statement;

    public Statement getStatement() {
        return statement;
    }

    public void setStatement(Statement statement) {
        this.statement = statement;
    }

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }
}
