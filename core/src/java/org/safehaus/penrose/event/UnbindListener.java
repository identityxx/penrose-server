package org.safehaus.penrose.event;

/**
 * @author Endi S. Dewata
 */
public interface UnbindListener {

    public boolean beforeUnbind(UnbindEvent event) throws Exception;
    public void afterUnbind(UnbindEvent event) throws Exception;
}
