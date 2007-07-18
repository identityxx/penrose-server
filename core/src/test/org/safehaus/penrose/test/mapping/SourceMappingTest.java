package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;

/**
 * @author Endi S. Dewata
 */
public class SourceMappingTest extends TestCase {

    public void testClone() throws Exception {
        SourceMapping s1 = new SourceMapping();
        s1.setName("name");
        s1.setSourceName("sourceName");
        s1.addFieldMapping(new FieldMapping("name", FieldMapping.CONSTANT, "value"));
        s1.setParameter("name", "value");
        s1.setReadOnly(true);
        s1.setAdd(SourceMapping.REQUIRED);
        s1.setBind(SourceMapping.REQUIRED);
        s1.setDelete(SourceMapping.REQUIRED);
        s1.setModify(SourceMapping.REQUIRED);
        s1.setModrdn(SourceMapping.REQUIRED);
        s1.setSearch(SourceMapping.REQUIRED);

        SourceMapping s2 = (SourceMapping)s1.clone();
        assertEquals(s1.getName(), s2.getName());
        assertEquals(s1.getSourceName(), s2.getSourceName());
        assertEquals(s1.isReadOnly(), s2.isReadOnly());
        assertEquals(s1.getAdd(), s2.getAdd());
        assertEquals(s1.getBind(), s2.getBind());
        assertEquals(s1.getDelete(), s2.getDelete());
        assertEquals(s1.getModify(), s2.getModify());
        assertEquals(s1.getModrdn(), s2.getModrdn());
        assertEquals(s1.getSearch(), s2.getSearch());

        assertFalse(s1.getFieldMappings().isEmpty());
        assertFalse(s2.getFieldMappings().isEmpty());
        assertEquals(s1.getFieldMappings(), s2.getFieldMappings());

        assertFalse(s1.getParameters().isEmpty());
        assertFalse(s2.getParameters().isEmpty());
        assertEquals(s1.getParameters(), s2.getParameters());
    }

    public void testEquals() {
        SourceMapping s1 = new SourceMapping();
        s1.setName("name");
        s1.setSourceName("sourceName");
        s1.addFieldMapping(new FieldMapping("name", FieldMapping.CONSTANT, "value"));
        s1.setParameter("name", "value");
        s1.setReadOnly(true);
        s1.setAdd(SourceMapping.REQUIRED);
        s1.setBind(SourceMapping.REQUIRED);
        s1.setDelete(SourceMapping.REQUIRED);
        s1.setModify(SourceMapping.REQUIRED);
        s1.setModrdn(SourceMapping.REQUIRED);
        s1.setSearch(SourceMapping.REQUIRED);

        SourceMapping s2 = new SourceMapping();
        s2.setName("name");
        s2.setSourceName("sourceName");
        s2.addFieldMapping(new FieldMapping("name", FieldMapping.CONSTANT, "value"));
        s2.setParameter("name", "value");
        s2.setReadOnly(true);
        s2.setAdd(SourceMapping.REQUIRED);
        s2.setBind(SourceMapping.REQUIRED);
        s2.setDelete(SourceMapping.REQUIRED);
        s2.setModify(SourceMapping.REQUIRED);
        s2.setModrdn(SourceMapping.REQUIRED);
        s2.setSearch(SourceMapping.REQUIRED);

        assertEquals(s1, s2);
    }
}
