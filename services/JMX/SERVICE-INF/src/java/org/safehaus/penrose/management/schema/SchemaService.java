package org.safehaus.penrose.management.schema;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.schema.*;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaService extends BaseService implements SchemaServiceMBean {

    SchemaManager schemaManager;
    String schemaName;

    public SchemaService(
            PenroseJMXService jmxService,
            SchemaManager schemaManager,
            String schemaName
    ) throws Exception {

        this.jmxService = jmxService;
        this.schemaManager = schemaManager;
        this.schemaName = schemaName;
    }

    public String getObjectName() {
        return SchemaClient.getStringObjectName(schemaName);
    }

    public Object getObject() {
        return getSchema();
    }

    public Schema getSchema() {
        return schemaManager.getSchema(schemaName);
    }

    public Collection<AttributeType> getAttributeTypes() throws Exception {
        Collection<AttributeType> list = new ArrayList<AttributeType>();
        Schema schema = getSchema();
        list.addAll(schema.getAttributeTypes());
        return list;
    }

    public Collection<String> getAttributeTypeNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        Schema schema = getSchema();
        list.addAll(schema.getAttributeTypeNames());
        return list;
    }

    public void addAttributeType(AttributeType attributeType) throws Exception {
        Schema schema = getSchema();
        schema.addAttributeType(attributeType);
    }

    public AttributeType getAttributeType(String name) throws Exception {
        Schema schema = getSchema();
        return schema.getAttributeType(name);
    }

    public void updateAttributeType(String name, AttributeType attributeType) throws Exception {
        Schema schema = getSchema();
        schema.updateAttributeType(name, attributeType);
    }

    public void removeAttributeType(String name) throws Exception {
        Schema schema = getSchema();
        schema.removeAttributeType(name);
    }

    public Collection<ObjectClass> getObjectClasses() throws Exception {
        Collection<ObjectClass> list = new ArrayList<ObjectClass>();
        Schema schema = getSchema();
        list.addAll(schema.getObjectClasses());
        return list;
    }

    public Collection<String> getObjectClassNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        Schema schema = getSchema();
        list.addAll(schema.getObjectClassNames());
        return list;
    }

    public void addObjectClass(ObjectClass objectClass) throws Exception {
        Schema schema = getSchema();
        schema.addObjectClass(objectClass);
    }

    public ObjectClass getObjectClass(String name) throws Exception {
        Schema schema = getSchema();
        return schema.getObjectClass(name);
    }

    public void updateObjectClass(String name, ObjectClass objectClass) throws Exception {
        Schema schema = getSchema();
        schema.updateObjectClass(name, objectClass);
    }

    public void removeObjectClass(String name) throws Exception {
        Schema schema = getSchema();
        schema.removeObjectClass(name);
    }

    public void store() throws Exception {
        schemaManager.storeSchema(schemaName);
    }
}
