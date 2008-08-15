package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.directory.EntrySourceConfig;
import org.safehaus.penrose.directory.EntryFieldConfig;

/**
 * @author Endi S. Dewata
 */
public class SourceMappingTest extends TestCase {

    public void testClone() throws Exception {
        EntrySourceConfig s1 = new EntrySourceConfig();
        s1.setAlias("name");
        s1.setSourceName("sourceName");
        s1.addFieldConfig(new EntryFieldConfig("name", EntryFieldConfig.CONSTANT, "value"));
        s1.setParameter("name", "value");
        s1.setReadOnly(true);
        s1.setAdd(EntrySourceConfig.REQUIRED);
        s1.setBind(EntrySourceConfig.REQUIRED);
        s1.setDelete(EntrySourceConfig.REQUIRED);
        s1.setModify(EntrySourceConfig.REQUIRED);
        s1.setModrdn(EntrySourceConfig.REQUIRED);
        s1.setSearch(EntrySourceConfig.REQUIRED);

        EntrySourceConfig s2 = (EntrySourceConfig)s1.clone();
        assertEquals(s1.getAlias(), s2.getAlias());
        assertEquals(s1.getSourceName(), s2.getSourceName());
        assertEquals(s1.isReadOnly(), s2.isReadOnly());
        assertEquals(s1.getAdd(), s2.getAdd());
        assertEquals(s1.getBind(), s2.getBind());
        assertEquals(s1.getDelete(), s2.getDelete());
        assertEquals(s1.getModify(), s2.getModify());
        assertEquals(s1.getModrdn(), s2.getModrdn());
        assertEquals(s1.getSearch(), s2.getSearch());

        assertFalse(s1.getFieldConfigs().isEmpty());
        assertFalse(s2.getFieldConfigs().isEmpty());
        assertEquals(s1.getFieldConfigs(), s2.getFieldConfigs());

        assertFalse(s1.getParameters().isEmpty());
        assertFalse(s2.getParameters().isEmpty());
        assertEquals(s1.getParameters(), s2.getParameters());
    }

    public void testEquals() {
        EntrySourceConfig s1 = new EntrySourceConfig();
        s1.setAlias("name");
        s1.setSourceName("sourceName");
        s1.addFieldConfig(new EntryFieldConfig("name", EntryFieldConfig.CONSTANT, "value"));
        s1.setParameter("name", "value");
        s1.setReadOnly(true);
        s1.setAdd(EntrySourceConfig.REQUIRED);
        s1.setBind(EntrySourceConfig.REQUIRED);
        s1.setDelete(EntrySourceConfig.REQUIRED);
        s1.setModify(EntrySourceConfig.REQUIRED);
        s1.setModrdn(EntrySourceConfig.REQUIRED);
        s1.setSearch(EntrySourceConfig.REQUIRED);

        EntrySourceConfig s2 = new EntrySourceConfig();
        s2.setAlias("name");
        s2.setSourceName("sourceName");
        s2.addFieldConfig(new EntryFieldConfig("name", EntryFieldConfig.CONSTANT, "value"));
        s2.setParameter("name", "value");
        s2.setReadOnly(true);
        s2.setAdd(EntrySourceConfig.REQUIRED);
        s2.setBind(EntrySourceConfig.REQUIRED);
        s2.setDelete(EntrySourceConfig.REQUIRED);
        s2.setModify(EntrySourceConfig.REQUIRED);
        s2.setModrdn(EntrySourceConfig.REQUIRED);
        s2.setSearch(EntrySourceConfig.REQUIRED);

        assertEquals(s1, s2);
    }
}
