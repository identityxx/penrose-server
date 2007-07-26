package org.safehaus.penrose.test.service;

import junit.framework.TestCase;
import org.safehaus.penrose.service.ServiceConfig;

/**
 * @author Endi S. Dewata
 */
public class ServiceConfigTest extends TestCase {

    public void testClone() throws Exception {
        ServiceConfig s1 = new ServiceConfig();
        s1.setName("name");
        s1.setDescription("description");
        s1.setEnabled(true);
        s1.setParameter("param1", "value1");
        s1.setParameter("param2", "value2");
        s1.setServiceClass("class");

        ServiceConfig s2 = (ServiceConfig)s1.clone();
        assertEquals(s1.getName(), s2.getName());
        assertEquals(s1.getDescription(), s2.getDescription());
        assertEquals(s1.isEnabled(), s2.isEnabled());
        assertEquals(s1.getParameters(), s2.getParameters());
        assertEquals(s1.getServiceClass(), s2.getServiceClass());
    }

    public void testEquals() {
        ServiceConfig s1 = new ServiceConfig();
        s1.setName("name");
        s1.setDescription("description");
        s1.setEnabled(true);
        s1.setParameter("param1", "value1");
        s1.setParameter("param2", "value2");
        s1.setServiceClass("class");

        ServiceConfig s2 = new ServiceConfig();
        s2.setName("name");
        s2.setDescription("description");
        s2.setEnabled(true);
        s2.setParameter("param1", "value1");
        s2.setParameter("param2", "value2");
        s2.setServiceClass("class");

        assertEquals(s1, s2);
    }
}
