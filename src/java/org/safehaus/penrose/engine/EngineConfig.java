/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.engine;

import java.io.Serializable;
import java.util.Properties;

/**
 * @author Endi S. Dewata
 */
public class EngineConfig implements Serializable {

    private String engineName;
    private String engineClass;
    private String description;

    private String addHandlerClass;
    private String bindHandlerClass;
    private String compareHandlerClass;
    private String deleteHandlerClass;
    private String modifyHandlerClass;
    private String modRdnHandlerClass;
    private String searchHandlerClass;

    private Properties properties = new Properties();

    public String getEngineClass() {
        return engineClass;
    }

    public void setEngineClass(String engineClass) {
        this.engineClass = engineClass;
    }

    public void setParameter(String name, String value) {
        properties.setProperty(name, value);
    }

    public String getParameter(String name) {
        return properties.getProperty(name);
    }

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getAddHandlerClass() {
        return addHandlerClass;
    }

    public void setAddHandlerClass(String addHandlerClass) {
        this.addHandlerClass = addHandlerClass;
    }

    public String getBindHandlerClass() {
        return bindHandlerClass;
    }

    public void setBindHandlerClass(String bindHandlerClass) {
        this.bindHandlerClass = bindHandlerClass;
    }

    public String getCompareHandlerClass() {
        return compareHandlerClass;
    }

    public void setCompareHandlerClass(String compareHandlerClass) {
        this.compareHandlerClass = compareHandlerClass;
    }

    public String getDeleteHandlerClass() {
        return deleteHandlerClass;
    }

    public void setDeleteHandlerClass(String deleteHandlerClass) {
        this.deleteHandlerClass = deleteHandlerClass;
    }

    public String getModifyHandlerClass() {
        return modifyHandlerClass;
    }

    public void setModifyHandlerClass(String modifyHandlerClass) {
        this.modifyHandlerClass = modifyHandlerClass;
    }

    public String getModRdnHandlerClass() {
        return modRdnHandlerClass;
    }

    public void setModRdnHandlerClass(String modRdnHandlerClass) {
        this.modRdnHandlerClass = modRdnHandlerClass;
    }

    public String getSearchHandlerClass() {
        return searchHandlerClass;
    }

    public void setSearchHandlerClass(String searchHandlerClass) {
        this.searchHandlerClass = searchHandlerClass;
    }
}
