/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;

import java.util.Collection;
import java.util.Properties;
import java.io.Serializable;


/**
 * @author Administrator
 */
public class CacheConfig implements Serializable {
	
    public final static String CACHE_EXPIRATION = "cacheExpiration";
    public final static String LOAD_ON_STARTUP  = "loadOnStartup";
    public final static String DRIVER           = "driver";
    public final static String URL              = "url";
    public final static String USER             = "user";
    public final static String PASSWORD         = "password";

    private String cacheName;
    private String cacheClass;
    private String description;

	/**
	 * Parameters.
	 */
	public Properties parameters = new Properties();

	public Collection getParameterNames() {
		return parameters.keySet();
	}

    public void setParameter(String name, String value) {
        parameters.setProperty(name, value);
    }

    public void removeParameter(String name) {
        parameters.remove(name);
    }

    public String getParameter(String name) {
        return parameters.getProperty(name);
    }

    public String getCacheClass() {
        return cacheClass;
    }

    public void setCacheClass(String cacheClass) {
        this.cacheClass = cacheClass;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }
}