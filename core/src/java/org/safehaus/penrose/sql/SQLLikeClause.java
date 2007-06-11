package org.safehaus.penrose.sql;

/**
 * @author Endi Sukma Dewata
 */
public class SQLLikeClause {

    private boolean not;
    private String pattern;

    public boolean isNot() {
        return not;
    }

    public void setNot(boolean not) {
        this.not = not;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();

        if (not) {
            sb.append("not ");
        }

        sb.append("like ");
        sb.append(pattern);

        return sb.toString();
    }
}
