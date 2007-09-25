package org.safehaus.penrose.test.mapping;

import junit.framework.TestCase;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.directory.FieldMapping;
import org.safehaus.penrose.directory.EntryMapping;
import org.safehaus.penrose.directory.AttributeMapping;

/**
 * @author Endi Sukma Dewata
 */
public class EntryMappingTest extends TestCase {

    public void testClone() throws Exception {
        EntryMapping e1 = new EntryMapping("dc=Example,dc=com");
        e1.setId("id");
        e1.setParentId("parentId");
        e1.addObjectClass("objectClass");
        e1.setDescription("description");

        AttributeMapping am = new AttributeMapping();
        am.setName("name");
        am.setConstant("constant");
        am.setRdn(true);
        e1.addAttributeMapping(am);

        SourceMapping sm = new SourceMapping();
        sm.setName("name");
        sm.setSourceName("sourceName");
        sm.addFieldMapping(new FieldMapping("name", FieldMapping.CONSTANT, "value"));
        e1.addSourceMapping(sm);

        EntryMapping e2 = (EntryMapping)e1.clone();
        assertEquals(e1.getId(), e2.getId());
        assertEquals(e1.getParentId(), e2.getParentId());
        assertEquals(e1.getDescription(), e2.getDescription());
        assertEquals(e1.getDn(), e2.getDn());

        assertFalse(e2.getObjectClasses().isEmpty());
        assertEquals(e1.getObjectClasses(), e2.getObjectClasses());

        assertFalse(e2.getAttributeMappings().isEmpty());
        assertEquals(e1.getAttributeMappings(), e2.getAttributeMappings());

        assertFalse(e2.getSourceMappings().isEmpty());
        assertEquals(e1.getSourceMappings(), e2.getSourceMappings());
    }
}
