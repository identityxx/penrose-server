package org.safehaus.penrose.nis.connection;

import org.safehaus.penrose.nis.*;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.schema.Schema;
import org.safehaus.penrose.schema.SchemaManager;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class NISConnection extends Connection {

    public String[] OBJECT_CLASSES = {
            "posixAccount",
            "shadowAccount",
            "ipHost",
            "posixGroup",
            "ipService",
            "oncRpc",
            "nisNetId",
            "ipProtocol",
            "nisMailAlias",
            "nisNetgroup",
            "ieee802Device",
            "bootableDevice",
            "ipNetwork",
            "automountMap",
            "automount",
            "nisMap"
    };

    public NISConnection() throws Exception {
    }

    public void init() throws Exception {
    }

    public void validate() throws Exception {
        try {
            NISClient client = createClient();
            client.connect();
            client.close();
        } catch (Exception e) {
            throw new Exception(e.getMessage());
        }
    }

    public NISClient createClient() throws Exception {

        Map<String,String> parameters = getParameters();

        String method = parameters.get(NIS.METHOD);
        if (method == null) method = NIS.DEFAULT_METHOD;

        NISClient client;

        if (NIS.LOCAL.equals(method)) {
            client = new NISLocalClient();

        } else if (NIS.JNDI.equals(method)) {
            client = new NISJNDIClient();

        } else { // if (NIS.YP.equals(method)) {
            client = new NISYPClient();
        }

        client.init(parameters);

        return client;
    }

    public Collection<String> getObjectClasses() {
        Collection<String> list = new TreeSet<String>();
        list.addAll(Arrays.asList(OBJECT_CLASSES));
        return list;
    }

    public Schema getSchema() throws Exception {

        Schema schema = new Schema("nis");

        SchemaManager schemaManager = getPartition().getSchemaManager();

        for (String ocName : getObjectClasses()) {
            ObjectClass oc = schemaManager.getObjectClass(ocName);
            if (oc == null) continue;

            schema.addObjectClass(oc);

            for (String atName : oc.getRequiredAttributes()) {
                AttributeType at = schemaManager.getAttributeType(atName);
                if (at == null) continue;

                schema.addAttributeType(at);
            }

            for (String atName : oc.getOptionalAttributes()) {
                AttributeType at = schemaManager.getAttributeType(atName);
                if (at == null) continue;

                schema.addAttributeType(at);
            }
        }

        return schema;
    }
}