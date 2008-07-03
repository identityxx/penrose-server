package org.safehaus.penrose.management.schema;

import org.safehaus.penrose.management.BaseClient;
import org.safehaus.penrose.management.PenroseClient;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.ObjectClass;

import java.util.Collection;
import java.util.ArrayList;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaClient extends BaseClient implements SchemaServiceMBean {

    public SchemaClient(PenroseClient client, String name) throws Exception {
        super(client, name, getStringObjectName(name));
    }

    public static String getStringObjectName(String name) {
        return "Penrose:type=schema,name="+name;
    }

    public Collection<AttributeType> getAttributeTypes() throws Exception {
        return (Collection<AttributeType>)getAttribute("AttributeTypes");
    }

    public Collection<String> getAttributeTypeNames() throws Exception {
        return (Collection<String>)getAttribute("AttributeTypeNames");
    }

    public AttributeType getAttributeType(String name) throws Exception {
        return (AttributeType)invoke(
                "getAttributeType",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public Collection<ObjectClass> getObjectClasses() throws Exception {
        return (Collection<ObjectClass>)getAttribute("ObjectClasses");
    }

    public Collection<String> getObjectClassNames() throws Exception {
        return (Collection<String>)getAttribute("ObjectClassNames");
    }

    public ObjectClass getObjectClass(String name) throws Exception {
        return (ObjectClass)invoke(
                "getObjectClass",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }
}
