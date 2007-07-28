package org.safehaus.penrose.service;

import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public interface ServiceConfigsMBean {

    public Collection<String> getServiceNames() throws Exception;
}
