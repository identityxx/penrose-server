package org.safehaus.penrose.test.module;

import junit.framework.TestCase;
import org.safehaus.penrose.module.ModuleConfig;

/**
 * @author Endi S. Dewata
 */
public class ModuleConfigTest extends TestCase {

    public void testClone() throws Exception {
        ModuleConfig m1 = new ModuleConfig();
        m1.setName("name");
        m1.setDescription("description");
        m1.setEnabled(true);
        m1.setModuleClass("class");
        m1.setParameter("param1", "value1");
        m1.setParameter("param2", "value2");

        ModuleConfig m2 = (ModuleConfig)m1.clone();
        assertEquals(m1.getName(), m2.getName());
        assertEquals(m1.getDescription(), m2.getDescription());
        assertEquals(m1.isEnabled(), m2.isEnabled());
        assertEquals(m1.getModuleClass(), m2.getModuleClass());
        assertEquals(m1.getParameters(), m2.getParameters());
    }

    public void testEquals() {
        ModuleConfig m1 = new ModuleConfig();
        m1.setName("name");
        m1.setDescription("description");
        m1.setEnabled(true);
        m1.setModuleClass("class");
        m1.setParameter("param1", "value1");
        m1.setParameter("param2", "value2");

        ModuleConfig m2 = new ModuleConfig();
        m2.setName("name");
        m2.setDescription("description");
        m2.setEnabled(true);
        m2.setModuleClass("class");
        m2.setParameter("param1", "value1");
        m2.setParameter("param2", "value2");

        assertEquals(m1, m2);
    }
}
