package org.safehaus.penrose.schema;

import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.attributeSyntax.AttributeSyntax;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public interface SchemaManagerServiceMBean {

    public Collection<String> getSchemaNames() throws Exception;
    public Collection<String> getBuiltInSchemaNames() throws Exception;
    public Collection<String> getCustomSchemaNames() throws Exception;

    public Schema getSchema() throws Exception;
    public Schema getSchema(String schemaName) throws Exception;
    public void createSchema(Schema schema) throws Exception;
    public void updateSchema(String schemaName, Schema schema) throws Exception;
    public void removeSchema(String schemaName) throws Exception;

    public Collection<ObjectClass> getObjectClasses() throws Exception;
    public Collection<String> getObjectClassNames() throws Exception;
    public ObjectClass getObjectClass(String ocName) throws Exception;

    public Collection<AttributeType> getAttributeTypes() throws Exception;
    public Collection<String> getAttributeTypeNames() throws Exception;
    public AttributeType getAttributeType(String attributeName)  throws Exception;

    public Collection<ObjectClass> getAllObjectClasses(String ocName) throws Exception;
    public Collection<String> getAllObjectClassNames(String ocName) throws Exception;
    public Schema getMergedSchema() throws Exception;

    public AttributeSyntax getAttributeSyntax(String oid) throws Exception;
}
