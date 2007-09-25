package org.safehaus.penrose.jdbc.scheduler;

import org.safehaus.penrose.ldap.*;
import org.safehaus.penrose.source.Source;
import org.safehaus.penrose.source.Field;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.engine.TransformEngine;

import java.util.Collection;

/**
 * @author Endi Sukma Dewata
 */
public class SplitSearchResponse extends SearchResponse {

    Collection<Source> targets;
    Interpreter interpreter;

    public SplitSearchResponse(
            Collection<Source> targets,
            Interpreter interpreter
    ) {
        this.targets = targets;
        this.interpreter = interpreter;
    }

    public void add(SearchResult result) throws Exception {
        SourceValues sourceValues = result.getSourceValues();

        interpreter.set(sourceValues);

        for (Source target : targets) {
            SourceValues sv = new SourceValues();
            Attributes attributes = new Attributes();
            Attributes primaryAttributes = new Attributes();

            //if (debug) log.debug("Target "+target.getName()+":");

            for (Field field : target.getFields()) {
                String fieldName = field.getName();

                Object value = interpreter.eval(field);
                //if (debug) log.debug(" - "+fieldName+": "+value);

                if (value == null) {
                    if (field.isPrimaryKey()) {
                        primaryAttributes = null;
                        break;
                    }
                    continue;
                }

                if (value instanceof Collection) {
                    Collection<Object> list = (Collection<Object>) value;
                    attributes.addValues(fieldName, list);
                    if (field.isPrimaryKey()) primaryAttributes.addValues(fieldName, list);
                } else {
                    attributes.addValue(fieldName, value);
                    if (field.isPrimaryKey()) primaryAttributes.addValue(fieldName, value);
                }

            }

            if (primaryAttributes == null) continue;

            // propagate new values to the next source
            sv.add(target.getName(), attributes);
            interpreter.set(sv);

            Collection<RDN> rdns = TransformEngine.convert(primaryAttributes);

            for (RDN rdn : rdns) {
                DN newDn = new DN(rdn);

                if (debug) {
                    log.debug("Adding " + target.getName() + ": " + newDn);
                    attributes.print();
                }

                try {
                    target.add(newDn, attributes);
                } catch (Exception e) {
                    errorLog.error(e.getMessage());
                }
            }
        }

        interpreter.clear();
    }
}
