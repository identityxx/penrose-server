package org.safehaus.penrose.adapter.jdbc;

import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.entry.*;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.Iterator;

/**
 * @author Endi S. Dewata
 */
public class JDBCFormatter {

    public static Logger log = LoggerFactory.getLogger(JDBCFormatter.class);

    public static int printHeader(SourceConfig sourceConfig) throws Exception {

        StringBuilder resultHeader = new StringBuilder();
        resultHeader.append("|");

        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)j.next();

            String name = fieldConfig.getName();
            int length = fieldConfig.getLength() > 15 ? 15 : fieldConfig.getLength();

            resultHeader.append(" ");
            resultHeader.append(Formatter.rightPad(name, length));
            resultHeader.append(" |");
        }

        int width = resultHeader.length();

        log.debug(Formatter.displaySeparator(width));
        log.debug(resultHeader.toString());
        log.debug(Formatter.displaySeparator(width));

        return width;
    }

    public static void printEntry(Entry entry) throws Exception {

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Entry: "+entry.getDn(), 80));

        for (Iterator i=entry.getSourceNames().iterator(); i.hasNext(); ) {
            String sourceName = (String)i.next();
            Attributes sourceValues = entry.getSourceValues(sourceName);

            for (Iterator j=sourceValues.getAll().iterator(); j.hasNext(); ) {
                Attribute attribute = (Attribute)j.next();
                String fieldName = sourceName+"."+attribute.getName();

                for (Iterator k=attribute.getValues().iterator(); k.hasNext(); ) {
                    Object value = k.next();
                    String className = value.getClass().getName();
                    className = className.substring(className.lastIndexOf(".")+1);

                    log.debug(Formatter.displayLine(" - "+fieldName+": "+value+" ("+className+")", 80));
                }
            }
        }

        log.debug(Formatter.displaySeparator(80));
    }

    public static void printRow(SourceConfig sourceConfig, AttributeValues av) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        for (Iterator i=av.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection c = av.get(name);

            Object value;
            if (c == null) {
                value = null;
            } else if (c.size() == 1) {
                value = c.iterator().next().toString();
            } else {
                value = c.toString();
            }

            rb.set(name, value);
        }

        printRow(sourceConfig, rb.toRdn());
    }

    public static void printRow(SourceConfig sourceConfig, RDN rdn) throws Exception {
        StringBuilder resultFields = new StringBuilder();
        resultFields.append("|");

        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)j.next();

            Object value = rdn.get(fieldConfig.getName());
            int length = fieldConfig.getLength() > 15 ? 15 : fieldConfig.getLength();

            resultFields.append(" ");
            resultFields.append(Formatter.rightPad(value == null ? "null" : value.toString(), length));
            resultFields.append(" |");
        }

        log.debug(resultFields.toString());
    }

    public static void printFooter(SourceConfig sourceConfig) throws Exception {

        StringBuilder resultHeader = new StringBuilder();
        resultHeader.append("|");

        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)j.next();

            String name = fieldConfig.getName();
            int length = fieldConfig.getLength() > 15 ? 15 : fieldConfig.getLength();

            resultHeader.append(" ");
            resultHeader.append(Formatter.rightPad(name, length));
            resultHeader.append(" |");
        }

        int width = resultHeader.length();
        log.debug(Formatter.displaySeparator(width));
    }

}
