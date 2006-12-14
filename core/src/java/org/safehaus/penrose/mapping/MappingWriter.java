package org.safehaus.penrose.mapping;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.util.BinaryUtil;
import org.safehaus.penrose.acl.ACI;
import org.safehaus.penrose.schema.ObjectClass;
import org.safehaus.penrose.schema.AttributeType;
import org.safehaus.penrose.Penrose;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import org.dom4j.Element;
import org.dom4j.tree.DefaultElement;
import org.dom4j.tree.DefaultAttribute;
import org.dom4j.tree.DefaultText;

import java.io.File;
import java.io.FileWriter;
import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class MappingWriter {

    Logger log = LoggerFactory.getLogger(getClass());

    public MappingWriter() {
    }

    public void write(String directory, Partition partition) throws Exception {
        File dir = new File(directory);
        dir.mkdirs();

        File file = new File(dir, "mapping.xml");

        FileWriter fw = new FileWriter(file);
        OutputFormat format = OutputFormat.createPrettyPrint();
        format.setTrimText(false);

        XMLWriter writer = new XMLWriter(fw, format);
        writer.startDocument();

        writer.startDTD(
                "mapping",
                "-//Penrose/DTD Mapping "+Penrose.SPECIFICATION_VERSION+"//EN",
                "http://penrose.safehaus.org/dtd/mapping.dtd"
        );

        writer.write(toElement(partition));
        writer.close();
    }

    public Element toElement(Partition partition) throws Exception {
        Element mappingElement = new DefaultElement("mapping");

        for (Iterator i = partition.getRootEntryMappings().iterator(); i.hasNext();) {
            EntryMapping entry = (EntryMapping)i.next();
            toElement(partition, entry, mappingElement);
        }

        return mappingElement;
    }

    public Element toElement(Partition partition, EntryMapping entryMapping, Element configElement) throws Exception {

        Element entryElement = new DefaultElement("entry");
        entryElement.add(new DefaultAttribute("dn", entryMapping.getDn()));
        if (!entryMapping.isEnabled()) entryElement.add(new DefaultAttribute("enabled", "false"));
        configElement.add(entryElement);

        for (Iterator i=entryMapping.getObjectClasses().iterator(); i.hasNext(); ) {
            String objectClass = (String)i.next();
            Element objectClassElement = new DefaultElement("oc");
            objectClassElement.setText(objectClass);
            entryElement.add(objectClassElement);
        }

        Collection attributes = entryMapping.getAttributeMappings();
        for (Iterator i = attributes.iterator(); i.hasNext(); ) {
            AttributeMapping attribute = (AttributeMapping)i.next();

            Element child = toElement(attribute);
            if (child == null) continue;

            entryElement.add(child);
        }

        for (Iterator i = entryMapping.getSourceMappings().iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            entryElement.add(toElement(sourceMapping));
        }

        for (Iterator i = entryMapping.getRelationships().iterator(); i.hasNext(); ) {
            Relationship relationship = (Relationship)i.next();
            entryElement.add(toElement(relationship));
        }

        for (Iterator i = entryMapping.getACL().iterator(); i.hasNext(); ) {
            ACI aci = (ACI)i.next();
            entryElement.add(toElement(aci));
        }

        for (Iterator i = entryMapping.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = entryMapping.getParameter(name);
            if ("".equals(value)) continue;

            Element parameter = new DefaultElement("parameter");

            Element paramName = new DefaultElement("param-name");
            paramName.add(new DefaultText(name));
            parameter.add(paramName);

            Element paramValue = new DefaultElement("param-value");
            paramValue.add(new DefaultText(value));
            parameter.add(paramValue);

            entryElement.add(parameter);
        }

        Collection children = partition.getChildren(entryMapping);
        for (Iterator i = children.iterator(); i.hasNext(); ) {
            EntryMapping child = (EntryMapping)i.next();
            toElement(partition, child, configElement);
        }

        return entryElement;
    }

    public Element toElement(FieldMapping fieldMapping) throws Exception {
        Element element = new DefaultElement("field");
        element.add(new DefaultAttribute("name", fieldMapping.getName()));

        if (fieldMapping.getConstant() != null) {
            Object value = fieldMapping.getConstant();
            if (value instanceof byte[]) {
                Element e = element.addElement("binary");
                e.addText(BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (fieldMapping.getVariable() != null) {
            Element scriptElement = new DefaultElement("variable");
            scriptElement.setText(fieldMapping.getVariable());
            element.add(scriptElement);

        } else if (fieldMapping.getExpression() != null) {
            element.add(toElement(fieldMapping.getExpression()));

        } else {
            return null;
        }

        return element;
    }

    public Element toElement(AttributeMapping attributeMapping) throws Exception {
        Element element = new DefaultElement("at");
        element.add(new DefaultAttribute("name", attributeMapping.getName()));
        if (attributeMapping.isPK()) element.add(new DefaultAttribute("rdn", "true"));
        if (attributeMapping.isOperational()) element.add(new DefaultAttribute("operational", "true"));
        //if (!AttributeMapping.DEFAULT_TYPE.equals(attributeMapping.getType())) element.addAttribute("type", attributeMapping.getType());
        if (attributeMapping.getLength() != AttributeMapping.DEFAULT_LENGTH) element.addAttribute("length", ""+attributeMapping.getLength());
        if (attributeMapping.getPrecision() != AttributeMapping.DEFAULT_PRECISION) element.addAttribute("precision", ""+attributeMapping.getPrecision());

        if (attributeMapping.getConstant() != null) {
            Object value = attributeMapping.getConstant();
            if (value instanceof byte[]) {
                Element e = element.addElement("binary");
                e.addText(BinaryUtil.encode(BinaryUtil.BASE64, (byte[])value));
            } else {
                Element e = element.addElement("constant");
                e.addText((String)value);
            }

        } else if (attributeMapping.getVariable() != null) {
            Element scriptElement = new DefaultElement("variable");
            scriptElement.setText(attributeMapping.getVariable());
            element.add(scriptElement);

        } else if (attributeMapping.getExpression() != null) {
            element.add(toElement(attributeMapping.getExpression()));

        } else {
            return null;
        }

        return element;
    }

    public Element toElement(Expression expression) {
        Element element = new DefaultElement("expression");
        if (expression.getForeach() != null) element.add(new DefaultAttribute("foreach", expression.getForeach()));
        if (expression.getVar() != null) element.add(new DefaultAttribute("var", expression.getVar()));

        element.setText(expression.getScript());
        return element;
    }

    public Element toElement(SourceMapping sourceMapping) throws Exception {
        Element element = new DefaultElement("source");

        element.add(new DefaultAttribute("name", sourceMapping.getName()));
        if (!sourceMapping.isRequired()) element.add(new DefaultAttribute("required", "false"));
        if (sourceMapping.isReadOnly()) element.add(new DefaultAttribute("readOnly", "true"));
        if (!sourceMapping.isIncludeOnAdd()) element.add(new DefaultAttribute("includeOnAdd", "false"));
        if (!sourceMapping.isIncludeOnModify()) element.add(new DefaultAttribute("includeOnModify", "false"));
        if (!sourceMapping.isIncludeOnModRdn()) element.add(new DefaultAttribute("includeOnModRdn", "false"));
        if (!sourceMapping.isIncludeOnDelete()) element.add(new DefaultAttribute("includeOnDelete", "false"));
        if (sourceMapping.isProxy()) element.add(new DefaultAttribute("proxy", "true"));

        Element sourceName = new DefaultElement("source-name");
        sourceName.add(new DefaultText(sourceMapping.getSourceName()));
        element.add(sourceName);

        // fields
        for (Iterator i=sourceMapping.getFieldMappings().iterator(); i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();

            Element child = toElement(fieldMapping);
            if (child == null) continue;

            element.add(child);
        }

        // parameters
        for (Iterator i = sourceMapping.getParameterNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            String value = sourceMapping.getParameter(name);
            if ("".equals(value)) continue;

            Element parameterElement = new DefaultElement("parameter");

            Element nameElement = new DefaultElement("param-name");
            nameElement.add(new DefaultText(name));
            parameterElement.add(nameElement);

            Element valueElement = new DefaultElement("param-value");
            valueElement.add(new DefaultText(value));
            parameterElement.add(valueElement);

            element.add(parameterElement);
        }

        return element;
    }

    public Element toElement(Relationship relationship) {
        Element element = new DefaultElement("relationship");

        Element expressionElement = new DefaultElement("expression");
        expressionElement.add(new DefaultText(relationship.getExpression()));
        element.add(expressionElement);

        return element;
    }

    public Element toElement(ACI aci) {
        Element element = new DefaultElement("aci");

        if (!ACI.SUBJECT_ANYBODY.equals(aci.getSubject())) {
            element.add(new DefaultAttribute("subject", aci.getSubject()));
        }

        if (aci.getDn() != null && !"".equals(aci.getDn())) {
            Element dnElement = new DefaultElement("dn");
            dnElement.add(new DefaultText(aci.getDn()));
            element.add(dnElement);
        }

        if (!ACI.TARGET_OBJECT.equals(aci.getTarget())) {
            Element targetElement = new DefaultElement("target");
            targetElement.add(new DefaultText(aci.getTarget()));
            element.add(targetElement);
        }

        if (aci.getAttributes() != null && !"".equals(aci.getAttributes())) {
            Element attributesElement = new DefaultElement("attributes");
            attributesElement.add(new DefaultText(aci.getAttributes()));
            element.add(attributesElement);
        }

        if (!ACI.SCOPE_SUBTREE.equals(aci.getScope())) {
            Element scopeElement = new DefaultElement("scope");
            scopeElement.add(new DefaultText(aci.getScope()));
            element.add(scopeElement);
        }

        if (!ACI.ACTION_GRANT.equals(aci.getAction())) {
            Element actionElement = new DefaultElement("action");
            actionElement.add(new DefaultText(aci.getAction()));
            element.add(actionElement);
        }

        Element permissionElement = new DefaultElement("permission");
        permissionElement.add(new DefaultText(aci.getPermission()));
        element.add(permissionElement);

        return element;
    }

    public Element toElement(ObjectClass oc) {
        Element element = new DefaultElement("objectclass");

        Element oidElement = new DefaultElement("oid");
        oidElement.add(new DefaultText(oc.getOid()));
        element.add(oidElement);

        Element namesElement = new DefaultElement("names");
        for (Iterator i=oc.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Element nameElement = new DefaultElement("name");
            nameElement.add(new DefaultText(name));
            namesElement.add(nameElement);
        }
        element.add(namesElement);

        Element descElement = new DefaultElement("description");
        if (oc.getDescription() != null) descElement.add(new DefaultText(oc.getDescription()));
        element.add(descElement);

        Element obsoleteElement = new DefaultElement("obsolete");
        obsoleteElement.add(new DefaultText(Boolean.toString(oc.isObsolete())));
        element.add(obsoleteElement);

        Element superclassesElement = new DefaultElement("superclasses");
        for (Iterator i=oc.getSuperClasses().iterator(); i.hasNext(); ) {
            String superClass = (String)i.next();
            Element ocElement = new DefaultElement("oc");
            ocElement.add(new DefaultText(superClass));
            superclassesElement.add(ocElement);
        }
        element.add(superclassesElement);

        Element typeElement = new DefaultElement("type");
        typeElement.add(new DefaultText(oc.getType()));
        element.add(typeElement);

        Element requiredAttributesElement = new DefaultElement("required-attributes");
        for (Iterator i=oc.getRequiredAttributes().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Element atElement = new DefaultElement("at");
            atElement.add(new DefaultText(name));
            requiredAttributesElement.add(atElement);
        }
        element.add(requiredAttributesElement);

        Element optionalAttributesElement = new DefaultElement("optional-attributes");
        for (Iterator i=oc.getOptionalAttributes().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Element atElement = new DefaultElement("at");
            atElement.add(new DefaultText(name));
            optionalAttributesElement.add(atElement);
        }
        element.add(optionalAttributesElement);

        return element;
    }

    public Element toElement(AttributeType at) {
        Element element = new DefaultElement("attributetype");

        Element oidElement = new DefaultElement("oid");
        oidElement.add(new DefaultText(at.getOid()));
        element.add(oidElement);

        Element namesElement = new DefaultElement("names");
        for (Iterator i=at.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Element nameElement = new DefaultElement("name");
            nameElement.add(new DefaultText(name));
            namesElement.add(nameElement);
        }
        element.add(namesElement);

        Element descElement = new DefaultElement("description");
        if (at.getDescription() != null) descElement.add(new DefaultText(at.getDescription()));
        element.add(descElement);

        Element obsoleteElement = new DefaultElement("obsolete");
        obsoleteElement.add(new DefaultText(Boolean.toString(at.isObsolete())));
        element.add(obsoleteElement);

        Element superclassElement = new DefaultElement("superclass");
        superclassElement.add(new DefaultText(at.getSuperClass()));
        element.add(superclassElement);

        Element equalityElement = new DefaultElement("equality");
        equalityElement.add(new DefaultText(at.getEquality()));
        element.add(equalityElement);

        Element orderingElement = new DefaultElement("ordering");
        orderingElement.add(new DefaultText(at.getOrdering()));
        element.add(orderingElement);

        Element substringElement = new DefaultElement("substring");
        substringElement.add(new DefaultText(at.getSubstring()));
        element.add(substringElement);

        Element syntaxElement = new DefaultElement("syntax");
        syntaxElement.add(new DefaultText(at.getSyntax()));
        element.add(syntaxElement);

        Element singleValuedElement = new DefaultElement("single-valued");
        singleValuedElement.add(new DefaultText(Boolean.toString(at.isSingleValued())));
        element.add(singleValuedElement);

        Element collectiveElement = new DefaultElement("collective");
        collectiveElement.add(new DefaultText(Boolean.toString(at.isCollective())));
        element.add(collectiveElement);

        Element modifiableElement = new DefaultElement("modifiable");
        modifiableElement.add(new DefaultText(Boolean.toString(at.isModifiable())));
        element.add(modifiableElement);

        Element usageElement = new DefaultElement("usage");
        usageElement.add(new DefaultText(at.getUsage()));
        element.add(usageElement);

        return element;
    }
}
