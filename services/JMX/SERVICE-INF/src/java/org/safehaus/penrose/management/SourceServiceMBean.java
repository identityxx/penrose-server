package org.safehaus.penrose.management;

/**
 * @author Endi Sukma Dewata
 */
public interface SourceServiceMBean {

    public Long getCount() throws Exception;

    public void createCache() throws Exception;
    public void loadCache() throws Exception;
    public void cleanCache() throws Exception;
    public void dropCache() throws Exception;
}
