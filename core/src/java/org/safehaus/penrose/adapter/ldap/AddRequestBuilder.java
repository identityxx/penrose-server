package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.mapping.EntryMapping;
import org.safehaus.penrose.mapping.SourceMapping;
import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.session.AddRequest;
import org.safehaus.penrose.session.AddResponse;
import org.safehaus.penrose.naming.PenroseContext;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.PasswordUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @author Endi S. Dewata
 */
public class AddRequestBuilder {

    Logger log = LoggerFactory.getLogger(getClass());

    LDAPAdapter adapter;

    Partition partition;
    EntryMapping entryMapping;

    Collection sourceMappings;
    SourceMapping primarySourceMapping;

    AttributeValues sourceValues;
    Interpreter interpreter;

    AddRequest request;
    AddResponse response;

    Collection requests = new ArrayList();

    public AddRequestBuilder(
            LDAPAdapter adapter,
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            AttributeValues sourceValues,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        this.adapter = adapter;

        this.partition = partition;
        this.entryMapping = entryMapping;

        this.sourceMappings = sourceMappings;
        primarySourceMapping = (SourceMapping)sourceMappings.iterator().next();

        this.sourceValues = sourceValues;

        this.request = request;
        this.response = response;

        PenroseContext penroseContext = adapter.getPenroseContext();
        interpreter = penroseContext.getInterpreterManager().newInstance();
    }

    public Collection generate() throws Exception {

        SourceMapping sourceMapping = (SourceMapping)sourceMappings.iterator().next();
        generatePrimaryRequest(sourceMapping);

        return requests;
    }

    public void generatePrimaryRequest(SourceMapping sourceMapping) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceMapping.getName();
        if (debug) log.debug("Processing source "+sourceName);

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

        AddRequest newRequest = new AddRequest();

        interpreter.set(sourceValues);

        Attributes attributes = request.getAttributes();
        for (Iterator i=attributes.getAll().iterator(); i.hasNext(); ) {
            Attribute attribute = (Attribute)i.next();

            String attributeName = attribute.getName();
            Object attributeValue = attribute.getValue(); // use only the first value

            interpreter.set(attributeName, attributeValue);
        }

        Attributes ldapAttributes = new Attributes();
        RDNBuilder rb = new RDNBuilder();

        Collection fieldMappings = sourceMapping.getFieldMappings();
        for (Iterator k=fieldMappings.iterator(); k.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)k.next();

            Object value = interpreter.eval(entryMapping, fieldMapping);
            if (value == null) continue;

            String fieldName = fieldMapping.getName();
            if (debug) log.debug(" - Field: "+fieldName+": "+value);

            FieldConfig fieldConfig = sourceConfig.getFieldConfig(fieldName);

            Attribute ldapAttribute = new Attribute(fieldConfig.getOriginalName());

            if ("unicodePwd".equals(fieldConfig.getOriginalName())) {
                ldapAttribute.addValue(PasswordUtil.toUnicodePassword(value));
            } else {
                ldapAttribute.addValue(value);
            }
            
            ldapAttributes.add(ldapAttribute);

            if (fieldConfig.isPrimaryKey()) {
                rb.set(fieldConfig.getOriginalName(), value);
            }
        }

        newRequest.setDn(adapter.getDn(sourceConfig, rb.toRdn()));

        String objectClasses = sourceConfig.getParameter(LDAPAdapter.OBJECT_CLASSES);
        Attribute ocAttribute = new Attribute("objectClass");
        for (StringTokenizer st = new StringTokenizer(objectClasses, ","); st.hasMoreTokens(); ) {
            String objectClass = st.nextToken().trim();
            ocAttribute.addValue(objectClass);
        }

        ldapAttributes.add(ocAttribute);

        newRequest.setAttributes(ldapAttributes);

        requests.add(newRequest);
    }
}
