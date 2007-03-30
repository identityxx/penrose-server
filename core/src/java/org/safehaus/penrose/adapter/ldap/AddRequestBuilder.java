package org.safehaus.penrose.adapter.ldap;

import org.safehaus.penrose.mapping.FieldMapping;
import org.safehaus.penrose.entry.*;
import org.safehaus.penrose.ldap.AddRequest;
import org.safehaus.penrose.ldap.AddResponse;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.source.SourceRef;
import org.safehaus.penrose.source.FieldRef;
import org.safehaus.penrose.source.Source;

import java.util.Collection;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * @author Endi S. Dewata
 */
public class AddRequestBuilder extends RequestBuilder {

    Collection sources;
    AttributeValues sourceValues;

    Interpreter interpreter;

    AddRequest request;
    AddResponse response;

    public AddRequestBuilder(
            String suffix,
            Collection sources,
            AttributeValues sourceValues,
            Interpreter interpreter,
            AddRequest request,
            AddResponse response
    ) throws Exception {

        this.suffix = suffix;

        this.sources = sources;
        this.sourceValues = sourceValues;

        this.interpreter = interpreter;

        this.request = request;
        this.response = response;
    }

    public Collection generate() throws Exception {

        SourceRef sourceRef = (SourceRef) sources.iterator().next();
        generatePrimaryRequest(sourceRef);

        return getRequests();
    }

    public void generatePrimaryRequest(SourceRef sourceRef) throws Exception {

        boolean debug = log.isDebugEnabled();

        String sourceName = sourceRef.getAlias();

        if (debug) log.debug("Processing source "+sourceName);

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

        for (Iterator k= sourceRef.getFieldRefs().iterator(); k.hasNext(); ) {
            FieldRef fieldRef = (FieldRef)k.next();

            FieldMapping fieldMapping = fieldRef.getFieldMapping();

            Object value = interpreter.eval(fieldMapping);
            if (value == null) continue;

            String fieldName = fieldRef.getName();
            if (debug) log.debug(" - Field: "+fieldName+": "+value);


            Attribute ldapAttribute = new Attribute(fieldRef.getOriginalName());

            if ("unicodePwd".equals(fieldRef.getOriginalName())) {
                ldapAttribute.addValue(PasswordUtil.toUnicodePassword(value));
            } else {
                ldapAttribute.addValue(value);
            }
            
            ldapAttributes.add(ldapAttribute);

            if (fieldRef.isPrimaryKey()) {
                rb.set(fieldRef.getOriginalName(), value);
            }
        }

        Source source = sourceRef.getSource();
        newRequest.setDn(getDn(source, rb.toRdn()));

        String objectClasses = source.getParameter(LDAPAdapter.OBJECT_CLASSES);
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
