package org.safehaus.penrose.management.schema;

import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SchemaServiceMBean {

    public Collection<AttributeType> getAttributeTypes() throws Exception;
    public Collection<String> getAttributeTypeNames() throws Exception;
    public AttributeType getAttributeType(String name) throws Exception;

    public Collection<ObjectClass> getObjectClasses() throws Exception;
    public Collection<String> getObjectClassNames() throws Exception;
    public ObjectClass getObjectClass(String name) throws Exception;
}
