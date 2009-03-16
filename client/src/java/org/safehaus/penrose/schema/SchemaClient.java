package org.safehaus.penrose.schema;

import org.safehaus.penrose.client.BaseClient;
import org.safehaus.penrose.client.PenroseClient;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaClient extends BaseClient implements SchemaServiceMBean {

    public SchemaClient(PenroseClient client, String name) throws Exception {
        super(client, name, getStringObjectName(name));
    }

    public static String getStringObjectName(String name) {
        return "Penrose:type=Schema,name="+name;
    }

    public Collection<AttributeType> getAttributeTypes() throws Exception {
        return (Collection<AttributeType>)getAttribute("AttributeTypes");
    }

    public Collection<String> getAttributeTypeNames() throws Exception {
        return (Collection<String>)getAttribute("AttributeTypeNames");
    }

    public void addAttributeType(AttributeType attributeType) throws Exception {
        invoke(
                "addAttributeType",
                new Object[] { attributeType },
                new String[] { AttributeType.class.getName() }
        );
    }

    public AttributeType getAttributeType(String name) throws Exception {
        return (AttributeType)invoke(
                "getAttributeType",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void updateAttributeType(String name, AttributeType attributeType) throws Exception {
        invoke(
                "updateAttributeType",
                new Object[] { name, attributeType },
                new String[] { String.class.getName(), AttributeType.class.getName() }
        );
    }

    public void removeAttributeType(String name) throws Exception {
        invoke(
                "removeAttributeType",
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

    public void addObjectClass(ObjectClass objectClass) throws Exception {
        invoke(
                "addObjectClass",
                new Object[] { objectClass },
                new String[] { ObjectClass.class.getName() }
        );
    }

    public ObjectClass getObjectClass(String name) throws Exception {
        return (ObjectClass)invoke(
                "getObjectClass",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void updateObjectClass(String name, ObjectClass objectClass) throws Exception {
        invoke(
                "updateObjectClass",
                new Object[] { name, objectClass },
                new String[] { String.class.getName(), ObjectClass.class.getName() }
        );
    }

    public void removeObjectClass(String name) throws Exception {
        invoke(
                "removeObjectClass",
                new Object[] { name },
                new String[] { String.class.getName() }
        );
    }

    public void store() throws Exception {
        invoke("store");
    }
}
