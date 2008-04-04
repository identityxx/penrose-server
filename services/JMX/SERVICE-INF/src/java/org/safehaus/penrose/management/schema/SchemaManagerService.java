package org.safehaus.penrose.management.schema;

import org.safehaus.penrose.management.BaseService;
import org.safehaus.penrose.management.PenroseJMXService;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.schema.ObjectClass;

import java.util.ArrayList;
import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaManagerService extends BaseService implements SchemaManagerServiceMBean {

    SchemaManager schemaManager;

    public SchemaManagerService(PenroseJMXService jmxService, SchemaManager schemaManager) {
        super(SchemaManagerServiceMBean.class);

        this.jmxService = jmxService;
        this.schemaManager = schemaManager;
    }

    public Object getObject() {
        return schemaManager;
    }

    public String getObjectName() {
        return SchemaManagerClient.getStringObjectName();
    }

    public Collection<String> getSchemaNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(schemaManager.getSchemaNames());
        return list;
    }

    public Collection<String> getBuiltInSchemaNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(schemaManager.getBuiltInSchemaNames());
        return list;
    }

    public Collection<String> getCustomSchemaNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(schemaManager.getCustomSchemaNames());
        return list;
    }

    public Schema getSchema(String schemaName) throws Exception {
        return schemaManager.getSchema(schemaName);
    }

    public void createSchema(Schema schema) throws Exception {
        schemaManager.createSchema(schema);
    }

    public void updateSchema(String schemaName, Schema schema) throws Exception {
        schemaManager.updateSchema(schemaName, schema);
    }

    public void removeSchema(String schemaName) throws Exception {
        schemaManager.removeSchema(schemaName);
    }

    public Collection<ObjectClass> getObjectClasses() throws Exception {
        Collection<ObjectClass> list = new ArrayList<ObjectClass>();
        list.addAll(schemaManager.getObjectClasses());
        return list;
    }

    public Collection<String> getObjectClassNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(schemaManager.getObjectClassNames());
        return list;
    }

    public ObjectClass getObjectClass(String ocName) throws Exception {
        return schemaManager.getObjectClass(ocName);
    }

    public Collection<AttributeType> getAttributeTypes() throws Exception {
        Collection<AttributeType> list = new ArrayList<AttributeType>();
        list.addAll(schemaManager.getAttributeTypes());
        return list;
    }

    public Collection<String> getAttributeTypeNames() throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(schemaManager.getAttributeTypeNames());
        return list;
    }

    public AttributeType getAttributeType(String attributeName) throws Exception {
        return schemaManager.getAttributeType(attributeName);
    }

    public Collection<ObjectClass> getAllObjectClasses(String ocName) throws Exception {
        Collection<ObjectClass> list = new ArrayList<ObjectClass>();
        list.addAll(schemaManager.getAllObjectClasses(ocName));
        return list;
    }

    public Collection<String> getAllObjectClassNames(String ocName) throws Exception {
        Collection<String> list = new ArrayList<String>();
        list.addAll(schemaManager.getAllObjectClassNames(ocName));
        return list;
    }

    public Schema getMergedSchema() throws Exception {
        return schemaManager.getMergedSchema();
    }

    public SchemaService getSchemaService(String schemaName) throws Exception {

        SchemaService service = new SchemaService(jmxService, schemaManager, schemaName);
        service.init();

        return service;
    }

    public void register() throws Exception {
        super.register();

        for (String schemaName : schemaManager.getSchemaNames()) {
            SchemaService partitionService = getSchemaService(schemaName);
            partitionService.register();
        }
    }

    public void unregister() throws Exception {
        for (String schemaName : schemaManager.getSchemaNames()) {
            SchemaService partitionService = getSchemaService(schemaName);
            partitionService.unregister();
        }

        super.unregister();
    }
}
