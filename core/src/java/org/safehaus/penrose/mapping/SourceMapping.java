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

    public final static String REQUIRED   = "required";
    public final static String REQUISITE  = "requisite";
    public final static String SUFFICIENT = "sufficient";
    public final static String IGNORE     = "ignore";

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
    private Map<String,String> parameters = new LinkedHashMap<String,String>();

    private boolean readOnly = false;

    private String search;
    private String bind;

    private String add;
    private String delete;
    private String modify;
    private String modrdn;

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
        return fieldMappings.get(name.toLowerCase());
    }

	public Collection<FieldMapping> getFieldMappings() {
        Collection<FieldMapping> results = new ArrayList<FieldMapping>();
        for (Collection<FieldMapping> list : fieldMappings.values()) {
            results.addAll(list);
        }
        return results;
	}

	public void addFieldMapping(FieldMapping fieldMapping) {
        String name = fieldMapping.getName().toLowerCase();
        Collection<FieldMapping> list = fieldMappings.get(name);
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

    public Map<String,String> getParameters() {
        return parameters;
    }

    public String getParameter(String name) {
        return parameters.get(name);
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

    public Collection<String> getParameterNames() {
        return parameters.keySet();
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
                (readOnly ? 0 : 1) +
                (search == null ? 0 : search.hashCode()) +
                (bind == null ? 0 : bind.hashCode()) +
                (add == null ? 0 : add.hashCode()) +
                (delete == null ? 0 : delete.hashCode()) +
                (modify == null ? 0 : modify.hashCode()) +
                (modrdn == null ? 0 : modrdn.hashCode());
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
        if (readOnly != sourceMapping.readOnly) return false;
        if (!equals(search, sourceMapping.search)) return false;
        if (!equals(bind, sourceMapping.bind)) return false;
        if (!equals(add, sourceMapping.add)) return false;
        if (!equals(delete, sourceMapping.delete)) return false;
        if (!equals(modify, sourceMapping.modify)) return false;
        if (!equals(modrdn, sourceMapping.modrdn)) return false;

        return true;
    }

    public void copy(SourceMapping sourceMapping) throws CloneNotSupportedException {
        name = sourceMapping.name;
        sourceName = sourceMapping.sourceName;

        removeFieldMappings();
        for (Collection<FieldMapping> list : sourceMapping.fieldMappings.values()) {
            for (FieldMapping fieldMapping : list) {
                addFieldMapping((FieldMapping) fieldMapping.clone());
            }
        }

        removeParameters();
        parameters.putAll(sourceMapping.parameters);

        readOnly = sourceMapping.readOnly;

        search = sourceMapping.search;
        bind = sourceMapping.bind;
        add = sourceMapping.add;
        delete = sourceMapping.delete;
        modify = sourceMapping.modify;
        modrdn = sourceMapping.modrdn;
    }

    public Object clone() throws CloneNotSupportedException {
        SourceMapping sourceMapping = (SourceMapping)super.clone();
        sourceMapping.copy(this);
        return sourceMapping;
    }

    public String getBind() {
        return bind;
    }

    public void setBind(String bind) {
        this.bind = bind;
    }

    public String getAdd() {
        return add;
    }

    public void setAdd(String add) {
        this.add = add;
    }

    public String getDelete() {
        return delete;
    }

    public void setDelete(String delete) {
        this.delete = delete;
    }

    public String getModify() {
        return modify;
    }

    public void setModify(String modify) {
        this.modify = modify;
    }

    public String getModrdn() {
        return modrdn;
    }

    public void setModrdn(String modrdn) {
        this.modrdn = modrdn;
    }

    public String getSearch() {
        return search;
    }

    public void setSearch(String search) {
        this.search = search;
    }
}