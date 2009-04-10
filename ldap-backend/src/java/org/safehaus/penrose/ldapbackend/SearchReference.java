package org.safehaus.penrose.ldapbackend;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SearchReference {

    public DN getDn() throws Exception;
    public Collection<String> getUrls() throws Exception;
    public Collection<Control> getControls() throws Exception;
}
