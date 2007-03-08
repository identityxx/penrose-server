package org.safehaus.penrose.sql;

/**
 * @author Endi S. Dewata
 */
public class SQLWhere {

    private SQLOrExpr expression;

    public SQLOrExpr getExpression() {
        return expression;
    }

    public void setExpression(SQLOrExpr expression) {
        this.expression = expression;
    }

    public String toString() {
        return expression.toString();
    }
}
