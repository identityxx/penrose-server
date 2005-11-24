/**
 * Copyright (c) 2000-2005, Identyx Corporation.
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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.PasswordUtil;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.config.Config;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.connector.ConnectionConfig;
import org.safehaus.penrose.connector.ConnectionManager;
import org.safehaus.penrose.interpreter.Interpreter;

import javax.naming.NamingException;
import javax.naming.NamingEnumeration;
import javax.naming.directory.*;
import java.util.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Endi S. Dewata
 */
public class PersistentEntryCache extends EntryCache {

    Config config;
    int entryId;

    ConnectionManager connectionManager;
    String jdbcConnectionName;
    String jndiConnectionName;

    public void init() throws Exception {
        super.init();

        //log.debug("-------------------------------------------------------------------------------");
        //log.debug("Initializing PersistentEngineCache:");

        connectionManager = engine.getConnectionManager();
        jdbcConnectionName = getParameter("jdbcConnection");
        jndiConnectionName = getParameter("jndiConnection");

        config = engine.getConfig(entryDefinition.getDn());

        entryId = getEntryId();
        if (entryId == 0) {
            registerEntry();
            entryId = getEntryId();
        }
    }

    public Connection getJDBCConnection() throws Exception {
        return (Connection)connectionManager.getConnection(jdbcConnectionName);
    }

    public DirContext getJNDIConnection() throws Exception {
        return (DirContext)connectionManager.getConnection(jndiConnectionName);
    }

    public void create() throws Exception {

        String dn = entryDefinition.getDn();
        log.debug("Entry "+dn+" ("+entryId+")");

        Collection attributeDefinitions = entryDefinition.getNonRdnAttributes();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            createAttributeTable(attributeDefinition);
        }

        Collection sources = config.getEffectiveSources(entryDefinition);

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
            SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

            Collection fields = sourceDefinition.getFieldDefinitions();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                createFieldTable(source, fieldDefinition);
            }
        }

        if (!entryDefinition.isDynamic()) {
            Interpreter interpreter = engine.getInterpreterFactory().newInstance();
            AttributeValues attributeValues = engine.computeAttributeValues(entryDefinition, interpreter);
            interpreter.clear();

            Entry entry = new Entry(dn, entryDefinition, attributeValues);
            Row rdn = entry.getRdn();

            put(rdn, entry);
        }

    }

    public int getEntryId() throws Exception {
        String dn = entryDefinition.getDn();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        int id = 0;

        try {
            String sql = "select id from penrose_mappings where dn=?";
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.setObject(1, dn);

            rs = ps.executeQuery();
            if (rs.next()) {
                id = rs.getInt(1);
            }

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return id;
    }

    public void registerEntry() throws Exception {
        String dn = entryDefinition.getDn();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            String sql = "insert into penrose_mappings values (null, ?)";
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.setObject(1, dn);

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createAttributeTable(AttributeDefinition attributeDefinition) throws Exception {

        String tableName = "penrose_"+entryId+"_attribute_"+attributeDefinition.getName();

        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection rdns = entryDefinition.getRdnAttributes();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            AttributeDefinition ad = (AttributeDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(ad.getName());
            columns.append(" ");
            columns.append(getColumnTypeDeclaration(ad));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(ad.getName());
        }

        columns.append(", ");
        columns.append(attributeDefinition.getName());
        columns.append(" ");
        columns.append(getColumnTypeDeclaration(attributeDefinition));

        primaryKeys.append(", ");
        primaryKeys.append(attributeDefinition.getName());

        columns.append(", primary key (");
        columns.append(primaryKeys);
        columns.append(")");

        StringBuffer create = new StringBuffer();
        create.append("create table ");
        create.append(tableName);

        String sql = create+" ("+columns+")";

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public String getColumnTypeDeclaration(AttributeDefinition attributeDefinition) {
        StringBuffer sb = new StringBuffer();
        sb.append(attributeDefinition.getType());

        if ("VARCHAR".equals(attributeDefinition.getType()) && attributeDefinition.getLength() > 0) {
            sb.append("(");
            sb.append(attributeDefinition.getLength());
            sb.append(")");
        }

        return sb.toString();
    }

    public String getColumnTypeDeclaration(FieldDefinition fieldDefinition) {
        StringBuffer sb = new StringBuffer();
        sb.append(fieldDefinition.getType());

        if ("VARCHAR".equals(fieldDefinition.getType()) && fieldDefinition.getLength() > 0) {
            sb.append("(");
            sb.append(fieldDefinition.getLength());
            sb.append(")");
        }

        return sb.toString();
    }

    public void createFieldTable(Source source, FieldDefinition fieldDefinition) throws Exception {

        String tableName = "penrose_"+entryId+"_field_"+source.getName()+"_"+fieldDefinition.getName();

        StringBuffer columns = new StringBuffer();
        StringBuffer primaryKeys = new StringBuffer();

        Collection rdns = entryDefinition.getRdnAttributes();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();

            if (columns.length() > 0) columns.append(", ");
            columns.append(attributeDefinition.getName());
            columns.append(" ");
            columns.append(getColumnTypeDeclaration(attributeDefinition));

            if (primaryKeys.length() > 0) primaryKeys.append(", ");
            primaryKeys.append(attributeDefinition.getName());
        }

        columns.append(", value ");
        columns.append(getColumnTypeDeclaration(fieldDefinition));

        primaryKeys.append(", value");

        columns.append(", primary key (");
        columns.append(primaryKeys);
        columns.append(")");

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(tableName);
        sb.append(" (");
        sb.append(columns);
        sb.append(")");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void clean() throws Exception {
        if (!entryDefinition.isDynamic()) return;

        String dn = entryDefinition.getDn();
        Row rdn = Entry.getRdn(dn);
        remove(rdn);

    }

    public void drop() throws Exception {
        if (!entryDefinition.isDynamic()) {
            String dn = entryDefinition.getDn();
            Row rdn = Entry.getRdn(dn);
            remove(rdn);
        }

        Collection sources = config.getEffectiveSources(entryDefinition);

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();
            dropEntrySourceTable(source);
        }

        Collection attributeDefinitions = entryDefinition.getNonRdnAttributes();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            dropAttributeTable(attributeDefinition);
        }
    }

    public void dropEntrySourceTable(Source source) throws Exception {

        ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
        SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

        Collection fields = sourceDefinition.getFieldDefinitions();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldDefinition fieldDefinition = (FieldDefinition)i.next();
            dropFieldTable(source, sourceDefinition, fieldDefinition);
        }
    }

    public void dropFieldTable(Source source, SourceDefinition sourceDefinition, FieldDefinition fieldDefinition) throws Exception {

        String tableName = "penrose_"+entryId+"_field_"+source.getName()+"_"+fieldDefinition.getName();

        String sql = "drop table "+tableName;

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void dropAttributeTable(AttributeDefinition attributeDefinition) throws Exception {

        String tableName = "penrose_"+entryId+"_attribute_"+attributeDefinition.getName();

        String sql = "drop table "+tableName;

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void load() throws Exception {

        if (!entryDefinition.isDynamic()) return;

        Collection entries = config.getChildren(entryDefinition);
        load(entries);
    }

    public void load(Collection entries) throws Exception {
        if (entries == null) return;

        for (Iterator i = entries.iterator(); i.hasNext();) {
            EntryDefinition ed = (EntryDefinition) i.next();
            String dn = ed.getDn();

            engine.search(null, new AttributeValues(), ed, null, null);

            //Collection children = config.getChildren(ed);
            //load(children);
        }
    }

    public Object get(Object pk) throws Exception {
        Row rdn = (Row)pk;
        String dn = rdn+","+parentDn;
        Entry entry = null;

        DirContext ctx = null;

        try {
            log.debug("Getting "+dn);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.OBJECT_SCOPE);

            ctx = getJNDIConnection();
            NamingEnumeration ne = ctx.search(dn, "(objectClass=*)", sc);

            if (!ne.hasMore()) return null;

            SearchResult sr = (SearchResult)ne.next();
            log.debug("Found:");

            Attributes attributes = sr.getAttributes();
            AttributeValues attributeValues = new AttributeValues();
            for (NamingEnumeration ne2 = attributes.getAll(); ne2.hasMore(); ) {
                Attribute attribute = (Attribute)ne2.next();
                String name = attribute.getID();

                for (NamingEnumeration ne3 = attribute.getAll(); ne3.hasMore(); ) {
                    Object value = ne3.next();
                    log.debug(" - "+name+": "+value);
                    attributeValues.add(name, value);
                }
            }

            entry = new Entry(dn, entryDefinition, attributeValues);

            AttributeValues sourceValues = entry.getSourceValues();
            Collection sources = config.getEffectiveSources(entryDefinition);

            for (Iterator i=sources.iterator(); i.hasNext(); ) {
                Source source = (Source)i.next();

                ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
                SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

                Collection fields = sourceDefinition.getFieldDefinitions();
                for (Iterator j=fields.iterator(); j.hasNext(); ) {
                    FieldDefinition fieldDefinition = (FieldDefinition)j.next();

                    Collection values = getField(source, fieldDefinition, rdn);
                    log.debug(" - "+source.getName()+"."+fieldDefinition.getName()+": "+values);

                    sourceValues.set(source.getName()+"."+fieldDefinition.getName(), values);
                }
            }

        } catch (NamingException e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return entry;
    }

    public Collection get(Filter filter) throws Exception {
        Collection results = new ArrayList();
        DirContext ctx = null;

        try {
            log.debug("Searching "+filter);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.ONELEVEL_SCOPE);
            sc.setReturningAttributes(new String[] { "dn" });

            ctx = getJNDIConnection();
            NamingEnumeration ne = ctx.search(parentDn, filter.toString(), sc);

            while (ne.hasMore()) {
                SearchResult sr = (SearchResult)ne.next();
                String dn = sr.getName()+","+parentDn;
                Attributes attributes = sr.getAttributes();

                log.debug(" - "+dn);
                results.add(dn);
            }

        } catch (NamingException e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        return results;
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();
        return results;
    }

    public Map search(Collection filters) throws Exception {

        Map values = new TreeMap();

        return values;
    }

    public void put(Object key, Object object) throws Exception {

        Entry entry = (Entry)object;
        Row rdn = entry.getRdn();
        String dn = entry.getDn();

        log.debug("Storing "+dn);

        AttributeValues attributeValues = entry.getAttributeValues();
        Attributes attrs = new BasicAttributes();

        for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Collection values = attributeValues.get(name);
            if (values.isEmpty()) continue;

            Attribute attr = new BasicAttribute(name);
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                log.debug(" - "+name+": "+value);

                if ("unicodePwd".equals(name)) {
                    attr.add(PasswordUtil.toUnicodePassword(value.toString()));
                } else {
                    attr.add(value.toString());
                }
            }
            attrs.put(attr);
        }

        DirContext ctx = null;
        try {
            ctx = getJNDIConnection();
            ctx.createSubcontext(dn, attrs);
        } catch (NamingException e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }
/*
        Collection attributeDefinitions = entryDefinition.getNonRdnAttributes();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            insertAttribute(attributeDefinition);
        }
*/
        AttributeValues sourceValues = entry.getSourceValues();
        Collection sources = config.getEffectiveSources(entryDefinition);

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
            SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

            Collection fields = sourceDefinition.getFieldDefinitions();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();

                deleteField(source, fieldDefinition, rdn);

                Collection values = sourceValues.get(source.getName()+"."+fieldDefinition.getName());
                if (values == null) continue;
                
                for (Iterator k=values.iterator(); k.hasNext(); ) {
                    Object value = k.next();
                    insertField(source, fieldDefinition, rdn, value);
                }
            }
        }
    }

    public void insertField(
            Source source,
            FieldDefinition fieldDefinition,
            Row rdn,
            Object value
            ) throws Exception {

        String tableName = "penrose_"+entryId+"_field_"+source.getName()+"_"+fieldDefinition.getName();

        StringBuffer columns = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection rdns = entryDefinition.getRdnAttributes();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            Object attributeValue = rdn.get(attributeDefinition.getName());

            if (columns.length() > 0) columns.append(", ");
            columns.append("?");

            parameters.add(attributeValue);
        }

        columns.append(", ?");
        parameters.add(value);

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ");
        sb.append(tableName);
        sb.append(" values (");
        sb.append(columns);
        sb.append(")");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public Collection getField(Source source, FieldDefinition fieldDefinition, Row rdn) throws Exception {

        String tableName = "penrose_"+entryId+"_field_"+source.getName()+"_"+fieldDefinition.getName();

        StringBuffer whereClause = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection rdns = entryDefinition.getRdnAttributes();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            Object attributeValue = rdn.get(attributeDefinition.getName());

            if (whereClause.length() > 0) whereClause.append(" and ");
            whereClause.append(attributeDefinition.getName());
            whereClause.append("=?");

            parameters.add(attributeValue);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("select value from ");
        sb.append(tableName);
        sb.append(" where ");
        sb.append(whereClause);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        Collection values = new ArrayList();

        try {
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object param = i.next();
                ps.setObject(counter, param);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters:", 80));
                counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Object param = i.next();
                    log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            rs = ps.executeQuery();

            log.debug("Results:");
            while (rs.next()) {
                Object value = rs.getObject(1);
                values.add(value);

                log.debug(" - "+value);
            }

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return values;
    }

    public void remove(Object key) throws Exception {
        Row rdn = (Row)key;
        String dn = parentDn == null ? entryDefinition.getDn() : rdn+","+parentDn;
        DirContext ctx = null;
        try {
            log.debug("Removing "+dn);
            SearchControls sc = new SearchControls();
            sc.setSearchScope(SearchControls.SUBTREE_SCOPE);
            sc.setReturningAttributes(new String[] { "dn" });

            ctx = getJNDIConnection();
            NamingEnumeration ne = ctx.search(dn, "(objectClass=*)", sc);

            ArrayList dns = new ArrayList();
            while (ne.hasMore()) {
                SearchResult sr = (SearchResult)ne.next();
                String rdn2 = sr.getName();
                String dn2 = "".equals(rdn2) ? dn : rdn2+","+dn;
                dns.add(0, dn2);
            }

            for (Iterator i=dns.iterator(); i.hasNext(); ) {
                String dn2 = (String)i.next();
                log.debug(" - "+dn2);

                ctx.destroySubcontext(dn2);
            }

        } catch (NamingException e) {
            log.error(e.getMessage());

        } finally {
            if (ctx != null) try { ctx.close(); } catch (Exception e) {}
        }

        Collection sources = config.getEffectiveSources(entryDefinition);

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            Source source = (Source)i.next();

            ConnectionConfig connectionConfig = config.getConnectionConfig(source.getConnectionName());
            SourceDefinition sourceDefinition = connectionConfig.getSourceDefinition(source.getSourceName());

            Collection fields = sourceDefinition.getFieldDefinitions();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldDefinition fieldDefinition = (FieldDefinition)j.next();
                deleteField(source, fieldDefinition, rdn);
            }
        }
    }

    public void deleteField(
            Source source,
            FieldDefinition fieldDefinition,
            Row rdn) throws Exception {

        String tableName = "penrose_"+entryId+"_field_"+source.getName()+"_"+fieldDefinition.getName();

        StringBuffer whereClause = new StringBuffer();
        Collection parameters = new ArrayList();

        Collection rdns = entryDefinition.getRdnAttributes();
        for (Iterator i=rdns.iterator(); i.hasNext(); ) {
            AttributeDefinition attributeDefinition = (AttributeDefinition)i.next();
            Object attributeValue = rdn.get(attributeDefinition.getName());

            if (whereClause.length() > 0) whereClause.append(" and ");
            whereClause.append(attributeDefinition.getName());
            whereClause.append("=?");

            parameters.add(attributeValue);
        }

        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(tableName);
        sb.append(" where ");
        sb.append(whereClause);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getJDBCConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(" "+counter+" = "+v);
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

}
