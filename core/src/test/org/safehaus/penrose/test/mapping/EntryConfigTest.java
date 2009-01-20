package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.directory.EntryFieldConfig;
import org.safehaus.penrose.directory.EntryConfig;
import org.safehaus.penrose.directory.EntryAttributeConfig;

/**
 * @author Endi Sukma Dewata
 */
public class EntryConfigTest extends TestCase {

    public void testClone() throws Exception {
        EntryConfig e1 = new EntryConfig("dc=Example,dc=com");
        e1.setName("name");
        e1.setParentName("parentName");
        e1.addObjectClass("objectClass");
        e1.setDescription("description");

        EntryAttributeConfig am = new EntryAttributeConfig();
        am.setName("name");
        am.setConstant("constant");
        am.setRdn(true);
        e1.addAttributeConfig(am);

        EntrySourceConfig sm = new EntrySourceConfig();
        sm.setAlias("name");
        sm.setSourceName("sourceName");
        sm.addFieldConfig(new EntryFieldConfig("name", EntryFieldConfig.CONSTANT, "value"));
        e1.addSourceConfig(sm);

        EntryConfig e2 = (EntryConfig)e1.clone();
        assertEquals(e1.getName(), e2.getName());
        assertEquals(e1.getParentName(), e2.getParentName());
        assertEquals(e1.getDescription(), e2.getDescription());
        assertEquals(e1.getDn(), e2.getDn());

        assertFalse(e2.getObjectClasses().isEmpty());
        assertEquals(e1.getObjectClasses(), e2.getObjectClasses());

        assertFalse(e2.getAttributeConfigs().isEmpty());
        assertEquals(e1.getAttributeConfigs(), e2.getAttributeConfigs());

        assertFalse(e2.getSourceConfigs().isEmpty());
        assertEquals(e1.getSourceConfigs(), e2.getSourceConfigs());
    }
}
