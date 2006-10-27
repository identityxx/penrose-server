/**
 * Copyright (c) 2000-2006, Identyx Corporation.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */
package org.safehaus.penrose.engine;

import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.interpreter.Interpreter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.connector.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.source.SourceConfig;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EngineFilterTool {

    Logger log = LoggerFactory.getLogger(getClass());

    public Engine engine;

    public EngineFilterTool(Engine engine) {
        this.engine = engine;
    }

    public Filter toSourceFilter(
            Partition partition,
            AttributeValues parentValues,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            Filter filter
    ) throws Exception {

        log.debug("Converting filter "+filter+" for "+sourceMapping.getName());

        if (filter instanceof NotFilter) {
            return toSourceFilter(partition, parentValues, entryMapping, sourceMapping, (NotFilter) filter);

        } else if (filter instanceof AndFilter) {
            return toSourceFilter(partition, parentValues, entryMapping, sourceMapping, (AndFilter) filter);

        } else if (filter instanceof OrFilter) {
            return toSourceFilter(partition, parentValues, entryMapping, sourceMapping, (OrFilter) filter);

        } else if (filter instanceof SimpleFilter) {
            return toSourceFilter(partition, parentValues, entryMapping, sourceMapping, (SimpleFilter) filter);

        } else if (filter instanceof SubstringFilter) {
            return toSourceFilter(partition, parentValues, entryMapping, sourceMapping, (SubstringFilter) filter);
        }

        return null;
    }

    public Filter toSourceFilter(
            Partition partition,
            AttributeValues parentValues,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SimpleFilter filter
    ) throws Exception {

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        String attributeValue = filter.getValue();

        if (attributeName.equals("objectClass")) {
            if (attributeValue.equals("*"))
                return null;
        }

        Interpreter interpreter = engine.getInterpreterManager().newInstance();
        interpreter.set(attributeName, attributeValue);

        if (parentValues != null) {
            interpreter.set(parentValues);
        }

        Collection fields = sourceMapping.getFieldMappings();
        Filter newFilter = null;

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();

            String v = (String)interpreter.eval(entryMapping, fieldMapping);
            if (v == null) continue;

            //System.out.println("Adding filter "+field.getName()+"="+v);
            SimpleFilter f = new SimpleFilter(fieldMapping.getName(), operator, v);

            newFilter = FilterTool.appendAndFilter(newFilter, f);
        }

        interpreter.clear();

        return newFilter;
    }

    public Filter toSourceFilter(
            Partition partition,
            AttributeValues parentValues,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SubstringFilter filter)
            throws Exception {

        String attributeName = filter.getAttribute();

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        String variable = attributeMapping.getVariable();
        log.debug("variable: "+variable);

        if (variable == null) return null;

        int index = variable.indexOf(".");
        String sourceName = variable.substring(0, index);
        String fieldName = variable.substring(index+1);
        log.debug("sourceName: "+sourceName);
        log.debug("fieldName: "+fieldName);

        if (!sourceName.equals(sourceMapping.getName())) return null;

        SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping.getSourceName());
        if (sourceConfig == null) throw new Exception("Unknown source: "+sourceMapping.getSourceName());

        ConnectionManager connectionManager = engine.getConnectionManager();
        Connection connection = connectionManager.getConnection(partition, sourceConfig.getConnectionName());
        if (connection == null) throw new Exception("Unknown connection: "+sourceConfig.getConnectionName());

        Adapter adapter = connection.getAdapter();
        Filter newFilter = adapter.convert(entryMapping, filter);

        return newFilter;
    }

    public Filter toSourceFilter(Partition partition, AttributeValues parentValues, EntryMapping entry, SourceMapping sourceMapping, NotFilter filter)
            throws Exception {

        Filter f = filter.getFilter();

        Filter newFilter = toSourceFilter(partition, parentValues, entry, sourceMapping, f);

        return new NotFilter(newFilter);
    }

    public Filter toSourceFilter(Partition partition, AttributeValues parentValues, EntryMapping entry, SourceMapping sourceMapping, AndFilter filter)
            throws Exception {

        Collection filters = filter.getFilters();

        AndFilter af = new AndFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = toSourceFilter(partition, parentValues, entry, sourceMapping, f);
            if (nf == null) continue;

            af.addFilter(nf);
        }

        if (af.size() == 0) return null;

        return af;
    }

    public Filter toSourceFilter(Partition partition, AttributeValues parentValues, EntryMapping entry, SourceMapping sourceMapping, OrFilter filter)
            throws Exception {

        Collection filters = filter.getFilters();

        OrFilter of = new OrFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = toSourceFilter(partition, parentValues, entry, sourceMapping, f);
            if (nf == null) continue;

            of.addFilter(nf);
        }

        if (of.size() == 0) return null;

        return of;
    }

}
