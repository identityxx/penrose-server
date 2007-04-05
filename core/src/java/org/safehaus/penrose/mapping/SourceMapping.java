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
package org.safehaus.penrose.mapping;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class SourceMapping implements Cloneable {

    Logger log = LoggerFactory.getLogger(getClass());

    public final static String FILTER     = "filter";

	/**
	 * Name.
	 */
	private String name = "DEFAULT";

    /**
     * Source name.
     */
    private String sourceName;

	/**
	 * Fields.
	 */
	private Map<String,Collection<FieldMapping>> fieldMappings = new LinkedHashMap<String,Collection<FieldMapping>>();

    /**
     * Parameters.
     */
    private Properties parameters = new Properties();

    private boolean required = true;
    private boolean readOnly = false;

    private boolean includeOnAdd = true;
    private boolean includeOnModify = true;
    private boolean includeOnModRdn = true;
    private boolean includeOnDelete = true;

	public SourceMapping() {
	}

    public SourceMapping(String name, String sourceName) {
        this.name = name;
        this.sourceName = sourceName;
    }
    
	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

    public Collection<FieldMapping> getFieldMappings(String name) {
        return (Collection<FieldMapping>)fieldMappings.get(name.toLowerCase());
    }

	public Collection getFieldMappings() {
        Collection<FieldMapping> results = new ArrayList<FieldMapping>();
        for (Iterator i=fieldMappings.values().iterator(); i.hasNext(); ) {
            Collection<FieldMapping> list = (Collection<FieldMapping>)i.next();
            results.addAll(list);
        }
		return results;
	}

	public void addFieldMapping(FieldMapping fieldMapping) {
        String name = fieldMapping.getName().toLowerCase();
        Collection<FieldMapping> list = (Collection<FieldMapping>)fieldMappings.get(name);
        if (list == null) {
            list = new ArrayList<FieldMapping>();
            fieldMappings.put(name, list);
        }
        list.add(fieldMapping);
	}

    public void removeFieldMappings(String name) {
        fieldMappings.remove(name.toLowerCase());
    }

    public void removeFieldMapping(FieldMapping fieldMapping) {
        Collection<FieldMapping> list = getFieldMappings(fieldMapping.getName());
        if (list == null) return;

        list.remove(fieldMapping);
        if (list.isEmpty()) removeFieldMappings(fieldMapping.getName());
    }

    public void removeFieldMappings() {
        fieldMappings.clear();
    }
    
    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public void removeParameters() {
        parameters.clear();
    }

    public void setParameter(String name, String value) {
        parameters.put(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public Collection getParameterNames() {
        return parameters.keySet();
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public boolean isIncludeOnDelete() {
        return includeOnDelete;
    }

    public void setIncludeOnDelete(boolean includeOnDelete) {
        this.includeOnDelete = includeOnDelete;
    }

    public boolean isIncludeOnAdd() {
        return includeOnAdd;
    }

    public void setIncludeOnAdd(boolean includeOnAdd) {
        this.includeOnAdd = includeOnAdd;
    }

    public boolean isIncludeOnModify() {
        return includeOnModify;
    }

    public void setIncludeOnModify(boolean includeOnModify) {
        this.includeOnModify = includeOnModify;
    }

    public boolean isIncludeOnModRdn() {
        return includeOnModRdn;
    }

    public void setIncludeOnModRdn(boolean includeOnModRdn) {
        this.includeOnModRdn = includeOnModRdn;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    public int hashCode() {
        return (name == null ? 0 : name.hashCode()) +
                (sourceName == null ? 0 : sourceName.hashCode()) +
                (fieldMappings == null ? 0 : fieldMappings.hashCode()) +
                (parameters == null ? 0 : parameters.hashCode()) +
                (required ? 0 : 1) +
                (readOnly ? 0 : 1) +
                (includeOnAdd ? 0 : 1) +
                (includeOnModify? 0 : 1) +
                (includeOnModRdn? 0 : 1) +
                (includeOnDelete ? 0 : 1);
    }

    boolean equals(Object o1, Object o2) {
        if (o1 == null && o2 == null) return true;
        if (o1 != null) return o1.equals(o2);
        return o2.equals(o1);
    }

    public boolean equals(Object object) {
        if (this == object) return true;
        if((object == null) || (object.getClass() != this.getClass())) return false;

        SourceMapping sourceMapping = (SourceMapping)object;
        if (!equals(name, sourceMapping.name)) return false;
        if (!equals(sourceName, sourceMapping.sourceName)) return false;
        if (!equals(fieldMappings, sourceMapping.fieldMappings)) return false;
        if (!equals(parameters, sourceMapping.parameters)) return false;
        if (required != sourceMapping.required) return false;
        if (readOnly != sourceMapping.readOnly) return false;
        if (includeOnAdd != sourceMapping.includeOnAdd) return false;
        if (includeOnModify != sourceMapping.includeOnModify) return false;
        if (includeOnModRdn != sourceMapping.includeOnModRdn) return false;
        if (includeOnDelete != sourceMapping.includeOnDelete) return false;

        return true;
    }

    public void copy(SourceMapping sourceMapping) {
        name = sourceMapping.name;
        sourceName = sourceMapping.sourceName;

        removeFieldMappings();
        for (Iterator i=sourceMapping.fieldMappings.values().iterator(); i.hasNext(); ) {
            Collection<FieldMapping> list = (Collection<FieldMapping>)i.next();
            for (Iterator j=list.iterator(); j.hasNext(); ) {
                FieldMapping fieldMapping = (FieldMapping)j.next();
                addFieldMapping((FieldMapping)fieldMapping.clone());
            }
        }

        removeParameters();
        parameters.putAll(sourceMapping.parameters);

        required = sourceMapping.required;
        readOnly = sourceMapping.readOnly;
        includeOnAdd = sourceMapping.includeOnAdd;
        includeOnModify = sourceMapping.includeOnModify;
        includeOnModRdn = sourceMapping.includeOnModRdn;
        includeOnDelete = sourceMapping.includeOnDelete;
    }

    public Object clone() {
        SourceMapping sourceMapping = new SourceMapping();
        sourceMapping.copy(this);
        return sourceMapping;
    }
}