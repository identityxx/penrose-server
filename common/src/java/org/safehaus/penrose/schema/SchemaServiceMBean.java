package org.safehaus.penrose.schema;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SchemaServiceMBean {

    public void addAttributeType(AttributeType attributeType) throws Exception;
    public Collection<AttributeType> getAttributeTypes() throws Exception;
    public Collection<String> getAttributeTypeNames() throws Exception;
    public AttributeType getAttributeType(String name) throws Exception;
    public void removeAttributeType(String name) throws Exception;

    public void addObjectClass(ObjectClass objectClass) throws Exception;
    public Collection<ObjectClass> getObjectClasses() throws Exception;
    public Collection<String> getObjectClassNames() throws Exception;
    public ObjectClass getObjectClass(String name) throws Exception;
    public void removeObjectClass(String name) throws Exception;

    public void store() throws Exception;
}
