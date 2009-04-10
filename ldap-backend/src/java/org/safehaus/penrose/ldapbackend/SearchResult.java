package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface SearchResult {

    public DN getDn() throws Exception;
    public Attributes getAttributes() throws Exception;
    public Collection<Control> getControls() throws Exception;
}
