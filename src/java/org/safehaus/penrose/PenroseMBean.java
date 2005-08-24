/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose;

import java.io.IOException;
import java.util.*;

import org.slf4j.Logger;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.acl.ACLEngine;

/**
 * @author Administrator
 */
public interface PenroseMBean {
	
	// ------------------------------------------------
	// Functional Methods
	// ------------------------------------------------
	public int init() throws Exception;
	public void addConfig(Config config) throws Exception;
	public void loadSchema(String filename) throws Exception;
	public void stop();

	/*
    public int bind(PenroseConnection connection, String dn, String password) throws Exception;
	public int unbind(PenroseConnection connection) throws Exception;
    public SearchResults search(PenroseConnection connection, String base, int scope,
            String filter, Collection attributeNames)
            throws Exception;
    public SearchResults search(PenroseConnection connection, String base, int scope,
            int deref, String filter, Collection attributeNames)
            throws Exception;
    public int add(PenroseConnection connection, LDAPEntry entry) throws Exception;
    public int delete(PenroseConnection connection, String dn) throws Exception;
    public int modify(PenroseConnection connection, String dn, List modifications) throws Exception;
	public int compare(PenroseConnection connection, String dn, String attributeName,
			String attributeValue) throws Exception;
	*/
	
	// ------------------------------------------------
	// Listeners
	// ------------------------------------------------
	/*
	public void addConnectionListener(ConnectionListener l);
	public void removeConnectionListener(ConnectionListener l);
	public void addBindListener(BindListener l);
	public void removeBindListener(BindListener l);
	public void addSearchListener(SearchListener l);
	public void removeSearchListener(SearchListener l);
	public void addCompareListener(CompareListener l);
	public void removeCompareListener(CompareListener l);
	public void addAddListener(AddListener l);
	public void removeAddListener(AddListener l);
	public void addDeleteListener(DeleteListener l);
	public void removeDeleteListener(DeleteListener l);
	public void addModifyListener(ModifyListener l);
	public void removeModifyListener(ModifyListener l);
	*/
	
	// ------------------------------------------------
	// Getters and Setters
	// ------------------------------------------------
	public void setSuffix(String suffixes[]);
	public ACLEngine getACLEngine();
	public void setACLEngine(ACLEngine aclEngine);
	public Collection getEngines();
	public PenroseConnectionPool getConnectionPool();
	public void setConnectionPool(PenroseConnectionPool connectionPool);
	public Logger getLog();
	public void setLog(Logger log);
	public List getNormalizedSuffixes();
	public void setNormalizedSuffixes(List normalizedSuffixes);
	public String getRootDn();
	public void setRootDn(String rootDn);
	public String getRootPassword();
	public void setRootPassword(String rootPassword);
	public boolean isStopRequested();
	public void setStopRequested(boolean stopRequested);
	public List getSuffixes();
	public void setSuffixes(List suffixes);
	public String readConfigFile(String filename) throws IOException;
	public void writeConfigFile(String filename, String content) throws IOException;

}
