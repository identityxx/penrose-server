package org.safehaus.penrose.schema;

import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;
import org.safehaus.penrose.schema.SchemaManagerServiceMBean;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.attributeSyntax.AttributeSyntax;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaManagerClient extends BaseClient implements SchemaManagerServiceMBean {

    public SchemaManagerClient(PenroseClient client) throws Exception {
        super(client, "SchemaManager", getStringObjectName());
    }

    public static String getStringObjectName() {
        return "Penrose:name=SchemaManager";
    }

    public Collection<String> getSchemaNames() throws Exception {
        return (Collection<String>)getAttribute("SchemaNames");
    }
    
    public Collection<String> getBuiltInSchemaNames() throws Exception {
        return (Collection<String>)getAttribute("BuiltInSchemaNames");
    }

    public Collection<String> getCustomSchemaNames() throws Exception {
        return (Collection<String>)getAttribute("CustomSchemaNames");
    }

    public Schema getSchema() throws Exception {
        return (Schema)getAttribute("Schema");
    }

    public Schema getSchema(String schemaName) throws Exception {
        return (Schema)invoke(
                "getSchema",
                new Object[] { schemaName },
                new String[] { String.class.getName() }
        );
    }

    public void createSchema(Schema schema) throws Exception {
        invoke(
                "createSchema",
                new Object[] { schema },
                new String[] { Schema.class.getName() }
        );
    }

    public void updateSchema(String schemaName, Schema schema) throws Exception {
        invoke(
                "updateSchema",
                new Object[] { schemaName, schema },
                new String[] { String.class.getName(), Schema.class.getName() }
        );
    }

    public void removeSchema(String schemaName) throws Exception {
        invoke(
                "removeSchema",
                new Object[] { schemaName },
                new String[] { String.class.getName() }
        );
    }

    public SchemaClient getSchemaClient(String schemaName) throws Exception {
        return new SchemaClient(client, schemaName);
    }
    
    public Collection<ObjectClass> getObjectClasses() throws Exception {
        return (Collection<ObjectClass>)getAttribute("ObjectClasses");
    }

    public Collection<String> getObjectClassNames() throws Exception {
        return (Collection<String>)getAttribute("ObjectClassNames");
    }

    public ObjectClass getObjectClass(String ocName) throws Exception {
        return (ObjectClass) invoke(
                "getObjectClass",
                new Object[] { ocName },
                new String[] { String.class.getName() }
        );
    }

    public Collection<AttributeType> getAttributeTypes() throws Exception {
        return (Collection<AttributeType>)getAttribute("AttributeTypes");
    }

    public Collection<String> getAttributeTypeNames() throws Exception {
        return (Collection<String>)getAttribute("AttributeTypeNames");
    }

    public AttributeType getAttributeType(String attributeName)  throws Exception {
        return (AttributeType) invoke(
                "getAttributeType",
                new Object[] { attributeName },
                new String[] { String.class.getName() }
        );
    }

    public Collection<ObjectClass> getAllObjectClasses(String ocName) throws Exception {
        return (Collection<ObjectClass>) invoke(
                "getAllObjectClasses",
                new Object[] { ocName },
                new String[] { String.class.getName() }
        );
    }

    public Collection<String> getAllObjectClassNames(String ocName) throws Exception {
        return (Collection<String>) invoke(
                "getAllObjectClassNames",
                new Object[] { ocName },
                new String[] { String.class.getName() }
        );
    }

    public Schema getMergedSchema() throws Exception {
        return (Schema)getAttribute("MergedSchema");
    }

    public AttributeSyntax getAttributeSyntax(String oid) throws Exception {
        return (AttributeSyntax) invoke(
                "getAttributeSyntax",
                new Object[] { oid },
                new String[] { String.class.getName() }
        );
    }
}
