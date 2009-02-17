package org.safehaus.penrose.schema;

import org.safehaus.penrose.ldap.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;

/**
 * @author Endi Sukma Dewata
 */
public class SchemaUtil {

    public Logger log = LoggerFactory.getLogger(getClass());
    boolean debug = log.isDebugEnabled();

    public Schema getSchema(LDAPClient client) throws Exception {

        SearchResult rootDSE = client.getRootDSE();

        if (debug) log.debug("Searching Schema ...");

        try {
            Attribute schemaNamingContext = rootDSE.getAttributes().get("schemaNamingContext");
            Attribute subschemaSubentry = rootDSE.getAttributes().get("subschemaSubentry");

            String schemaDn;

            if (schemaNamingContext != null) {
                schemaDn = (String)schemaNamingContext.getValue();
                if (debug) log.debug("Active Directory Schema: "+schemaDn);
                return getActiveDirectorySchema(client, schemaDn);

            } else if (subschemaSubentry != null) {
                schemaDn = (String)subschemaSubentry.getValue();
                if (debug) log.debug("Standard LDAP Schema: "+schemaDn);
                return getLDAPSchema(client, schemaDn);

            } else {
                schemaDn = "cn=schema";
                if (debug) log.debug("Default Schema: "+schemaDn);
                return getLDAPSchema(client, schemaDn);
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw e;
        }
    }

    public Schema getLDAPSchema(LDAPClient client, String schemaDn) throws Exception {

        Schema schema = new Schema("ldap");

        if (debug) log.debug("Searching "+schemaDn+" ...");

        SearchRequest request = new SearchRequest();
        request.setDn(schemaDn);
        request.setScope(SearchRequest.SCOPE_BASE);
        request.setAttributes(new String[] { "attributeTypes", "objectClasses" });

        SearchResponse response = new SearchResponse();

        client.search(request, response);

        SearchResult sr = response.next();

        Attributes attributes = sr.getAttributes();

        //log.debug("Object Classes:");
        for (Object value : attributes.getValues("objectClasses")) {
            ObjectClass oc = parseObjectClass((String)value);
            if (oc == null) continue;

            //log.debug(" - "+oc.getName());
            schema.addObjectClass(oc);
        }

        //log.debug("Attribute Types:");
        for (Object value : attributes.getValues("attributeTypes")) {
            AttributeType at = parseAttributeType((String)value);
            if (at == null) continue;

            //log.debug(" - "+at.getName());
            schema.addAttributeType(at);
        }

        return schema;
    }

    public AttributeType parseAttributeType(String line) throws Exception {
        try {
            line = "attributetype "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Schema schema = parser.parse();
            return schema.getAttributeTypes().iterator().next();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public ObjectClass parseObjectClass(String line) throws Exception {
        try {
            line = "objectclass "+line;
            SchemaParser parser = new SchemaParser(new StringReader(line));
            Schema schema = parser.parse();
            return schema.getObjectClasses().iterator().next();

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }

    public Schema getActiveDirectorySchema(LDAPClient client, String schemaDn) throws Exception {

        if (debug) log.debug("Searching "+schemaDn+" ...");

        Schema schema = new Schema("ad");

        getActiveDirectoryAttributeTypes(client, schema, schemaDn);
        getActiveDirectoryObjectClasses(client, schema, schemaDn);

        return schema;
    }

    public synchronized void getActiveDirectoryAttributeTypes(LDAPClient client, Schema schema, String schemaDn) throws Exception {

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        SearchRequest request = new SearchRequest();
        request.setDn(schemaDn);
        request.setScope(SearchRequest.SCOPE_ONE);
        request.setFilter("(objectClass=attributeSchema)");

        SearchResponse response = new SearchResponse();

        client.search(request, response);

        while (response.hasNext()) {
            SearchResult sr = response.next();
            Attributes attributes = sr.getAttributes();

            String atName = (String)attributes.getValue("lDAPDisplayName");

            AttributeType at = new AttributeType();
            at.setName(atName);

            at.setOid((String)attributes.getValue("attributeID"));
            at.setDescription((String)attributes.getValue("adminDescription"));
            at.setSyntax((String)attributes.getValue("attributeSyntax"));
            at.setSingleValued(Boolean.valueOf((String)attributes.getValue("isSingleValued")));

            schema.addAttributeType(at);
        }
    }

    public synchronized void getActiveDirectoryObjectClasses(LDAPClient client, Schema schema, String schemaDn) throws Exception {

        if (debug) log.debug("Search \""+ schemaDn +"\"");

        SearchRequest request = new SearchRequest();
        request.setDn(schemaDn);
        request.setScope(SearchRequest.SCOPE_ONE);
        request.setFilter("(objectClass=classSchema)");

        SearchResponse response = new SearchResponse();

        client.search(request, response);

        while (response.hasNext()) {
            SearchResult sr = response.next();
            Attributes attributes = sr.getAttributes();

            ObjectClass oc = new ObjectClass();
            oc.setName((String)attributes.getValue("lDAPDisplayName"));
            oc.setOid((String)attributes.getValue("governsID"));
            oc.setDescription((String)attributes.getValue("adminDescription"));

            for (Object value : attributes.getValues("mustContain")) {
                oc.addRequiredAttribute((String)value);
            }

            for (Object value : attributes.getValues("systemMustContain")) {
                oc.addRequiredAttribute((String)value);
            }

            for (Object value : attributes.getValues("mayContain")) {
                oc.addOptionalAttribute((String)value);
            }

            for (Object value : attributes.getValues("systemMayContain")) {
                oc.addOptionalAttribute((String)value);
            }

            schema.addObjectClass(oc);
        }
    }

}
