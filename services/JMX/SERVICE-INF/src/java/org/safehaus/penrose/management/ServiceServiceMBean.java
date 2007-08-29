package org.safehaus.penrose.management;

/**
 * @author Endi Sukma Dewata
 */
public interface ServiceServiceMBean {

    public String getStatus() throws Exception;
    public void start() throws Exception;
    public void stop() throws Exception;
}
