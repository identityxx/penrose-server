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
import org.safehaus.penrose.connection.Connection;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.partition.PartitionConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.ldap.SourceValues;
import org.safehaus.penrose.directory.Entry;
import org.safehaus.penrose.directory.AttributeMapping;
import org.safehaus.penrose.directory.FieldMapping;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Collection;

/**
 * @author Endi S. Dewata
 */
public class EngineFilterTool {

    public Logger log = LoggerFactory.getLogger(getClass());
    public boolean debug = log.isDebugEnabled();

    public Engine engine;

    public EngineFilterTool(Engine engine) {
        this.engine = engine;
    }

    public Filter convert(
            Partition partition,
            SourceValues parentValues,
            Entry entry,
            SourceMapping sourceMapping,
            Filter filter
    ) throws Exception {

        if (debug) log.debug("Converting filter "+filter+" for "+sourceMapping.getName());

        if (filter instanceof NotFilter) {
            return convert(partition, parentValues, entry, sourceMapping, (NotFilter) filter);

        } else if (filter instanceof AndFilter) {
            return convert(partition, parentValues, entry, sourceMapping, (AndFilter) filter);

        } else if (filter instanceof OrFilter) {
            return convert(partition, parentValues, entry, sourceMapping, (OrFilter) filter);

        } else if (filter instanceof SimpleFilter) {
            return convert(partition, parentValues, entry, sourceMapping, (SimpleFilter) filter);

        } else if (filter instanceof SubstringFilter) {
            return convert(partition, parentValues, entry, sourceMapping, (SubstringFilter) filter);
        }

        return null;
    }

    public Filter convert(
            Partition partition,
            SourceValues parentValues,
            Entry entry,
            SourceMapping sourceMapping,
            SimpleFilter filter
    ) throws Exception {

        String attributeName = filter.getAttribute();
        String operator = filter.getOperator();
        Object attributeValue = filter.getValue();

        if (attributeName.equalsIgnoreCase("objectClass")) {
            if (attributeValue.equals("*"))
                return null;
        }

        Interpreter interpreter = partition.newInterpreter();
        interpreter.set(attributeName, attributeValue);

        if (parentValues != null) {
            interpreter.set(parentValues);
        }

        Collection fields = sourceMapping.getFieldMappings();
        Filter newFilter = null;

        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldMapping fieldMapping = (FieldMapping)i.next();

            String v = (String)interpreter.eval(fieldMapping);
            if (v == null) continue;

            //System.out.println("Adding filter "+field.getName()+"="+v);
            SimpleFilter f = new SimpleFilter(fieldMapping.getName(), operator, v);

            newFilter = FilterTool.appendAndFilter(newFilter, f);
        }

        interpreter.clear();

        return newFilter;
    }

    public Filter convert(
            Partition partition,
            SourceValues parentValues,
            Entry entry,
            SourceMapping sourceMapping,
            SubstringFilter filter)
            throws Exception {

        String attributeName = filter.getAttribute();
        Collection<Object> substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entry.getAttributeMapping(attributeName);
        String variable = attributeMapping.getVariable();
        if (debug) log.debug("variable: "+variable);

        if (variable == null) return null;

        int index = variable.indexOf(".");
        String sourceName = variable.substring(0, index);
        String fieldName = variable.substring(index+1);
        
        if (debug) {
            log.debug("sourceName: "+sourceName);
            log.debug("fieldName: "+fieldName);
        }

        if (!sourceName.equals(sourceMapping.getName())) return null;

        PartitionConfig partitionConfig = partition.getPartitionConfig();
        SourceConfig sourceConfig = partitionConfig.getSourceConfigs().getSourceConfig(sourceMapping.getSourceName());
        if (sourceConfig == null) throw new Exception("Unknown source: "+sourceMapping.getSourceName());

        Connection connection = partition.getConnection(sourceConfig.getConnectionName());
        if (connection == null) throw new Exception("Unknown connection: "+sourceConfig.getConnectionName());

        Adapter adapter = connection.getAdapter();
        Filter newFilter = null; // adapter.convert(entry, filter);

        return newFilter;
    }

    public Filter convert(Partition partition, SourceValues parentValues, Entry entry, SourceMapping sourceMapping, NotFilter filter)
            throws Exception {

        Filter f = filter.getFilter();

        Filter newFilter = convert(partition, parentValues, entry, sourceMapping, f);

        return new NotFilter(newFilter);
    }

    public Filter convert(Partition partition, SourceValues parentValues, Entry entry, SourceMapping sourceMapping, AndFilter filter)
            throws Exception {

        Collection filters = filter.getFilters();

        AndFilter af = new AndFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = convert(partition, parentValues, entry, sourceMapping, f);
            if (nf == null) continue;

            af.addFilter(nf);
        }

        if (af.size() == 0) return null;

        return af;
    }

    public Filter convert(Partition partition, SourceValues parentValues, Entry entry, SourceMapping sourceMapping, OrFilter filter)
            throws Exception {

        Collection filters = filter.getFilters();

        OrFilter of = new OrFilter();
        for (Iterator i=filters.iterator(); i.hasNext(); ) {
            Filter f = (Filter)i.next();

            Filter nf = convert(partition, parentValues, entry, sourceMapping, f);
            if (nf == null) continue;

            of.addFilter(nf);
        }

        if (of.size() == 0) return null;

        return of;
    }

}
