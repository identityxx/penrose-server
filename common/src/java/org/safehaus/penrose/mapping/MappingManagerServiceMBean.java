package org.safehaus.penrose.mapping;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface MappingManagerServiceMBean {

    public Collection<String> getMappingNames() throws Exception;
    public void createMapping(MappingConfig mappingConfig) throws Exception;
    public void updateMapping(String name, MappingConfig connectionConfig) throws Exception;
    public void removeMapping(String name) throws Exception;
}
