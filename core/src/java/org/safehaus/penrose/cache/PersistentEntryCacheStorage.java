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
package org.safehaus.penrose.cache;

import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.EntryUtil;
import org.safehaus.penrose.source.FieldConfig;
import org.safehaus.penrose.source.SourceConfig;
import org.safehaus.penrose.filter.*;
import org.safehaus.penrose.session.PenroseSearchResults;
import org.safehaus.penrose.connection.ConnectionManager;
import org.safehaus.penrose.Penrose;
import org.ietf.ldap.LDAPException;

import javax.naming.NamingException;
import java.util.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author Endi S. Dewata
 */
public class PersistentEntryCacheStorage extends EntryCacheStorage {

    int mappingId;

    String connectionName;

    public PersistentEntryCacheStorage(Penrose penrose) throws Exception {
        super(penrose);
    }

    public void init() throws Exception {
        super.init();

        connectionName = getParameter(PersistentEntryCache.CONNECTION);

        mappingId = getMappingId();
    }

    public Connection getConnection() throws Exception {
        ConnectionManager connectionManager = penrose.getConnectionManager();
        return (Connection)connectionManager.openConnection(connectionName);
    }

    public void create() throws Exception {

        addMapping();
        mappingId = getMappingId();

        String dn = getEntryMapping().getDn();
        log.debug("Creating cache tables for mapping "+dn+" ("+mappingId+")");

        createEntriesTable();

        Collection attributeMappings = getEntryMapping().getAttributeMappings();
        for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            createAttributeTable(attributeMapping);
        }

        Collection sources = getPartition().getEffectiveSourceMappings(getEntryMapping());

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            SourceConfig sourceConfig = getPartition().getSourceConfig(sourceMapping.getSourceName());

            Collection fields = sourceConfig.getFieldConfigs();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                createFieldTable(sourceMapping, fieldConfig);
            }
        }
    }

    public int getMappingId() throws Exception {
        String dn = getEntryMapping().getDn();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        int id = 0;

        try {
            String sql = "select id from penrose_mappings where dn=?";
            con = getConnection();

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("Parameters: dn = "+dn, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            ps.setObject(1, dn);

            rs = ps.executeQuery();
            if (rs.next()) {
                id = rs.getInt(1);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Results: id = "+id, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            return id;

        } catch (Exception e) {
            log.error(e.getMessage());
            return -1;

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void addMapping() throws Exception {
        String dn = getEntryMapping().getDn();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            String sql = "insert into penrose_mappings values (null, ?)";
            con = getConnection();

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

    public int getEntryId(String dn) throws Exception {

        String tableName = "penrose_"+mappingId+"_entries";

        StringBuffer sb = new StringBuffer();
        Collection parameters = new ArrayList();

        sb.append("select id from ");
        sb.append(tableName);
        sb.append(" where rdn=? and parentDn=?");

        String parentDn = EntryUtil.getParentDn(dn);
        Row rdn = EntryUtil.getRdn(dn);

        parameters.add(rdn.toString());
        parameters.add(parentDn);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        int id = 0;

        try {
            con = getConnection();

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
                Object value = i.next();
                ps.setObject(counter, value);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters:", 80));
                counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Object value = i.next();
                    log.debug(Formatter.displayLine(" - "+counter+" = "+value, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            rs = ps.executeQuery();
            if (rs.next()) {
                id = rs.getInt(1);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Results: id = "+id, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            return id;

        } catch (Exception e) {
            log.error(e.getMessage());
            return -1;

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void addEntry(String dn) throws Exception {

        String tableName = "penrose_"+mappingId+"_entries";

        StringBuffer sb = new StringBuffer();
        Collection parameters = new ArrayList();

        sb.append("insert into ");
        sb.append(tableName);
        sb.append(" values (null, ?, ?)");

        String parentDn = EntryUtil.getParentDn(dn);
        Row rdn = EntryUtil.getRdn(dn);

        parameters.add(rdn.toString());
        parameters.add(parentDn);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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
                Object value = i.next();
                ps.setObject(counter, value);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters:", 80));
                counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Object value = i.next();
                    log.debug(Formatter.displayLine(" - "+counter+" = "+value, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void removeEntry(int entryId) throws Exception {

        String tableName = "penrose_"+mappingId+"_entries";

        StringBuffer sb = new StringBuffer();
        Collection parameters = new ArrayList();

        sb.append("delete from ");
        sb.append(tableName);

        if (entryId > 0) {
            sb.append(" where id=?");
            parameters.add(new Integer(entryId));
        }

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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
            }

            if (log.isDebugEnabled() && parameters.size() > 0) {
                log.debug(Formatter.displayLine("Parameter: id = "+entryId, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void createEntriesTable() throws Exception {

        String tableName = "penrose_"+mappingId+"_entries";

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(tableName);
        sb.append(" (id integer auto_increment, rdn varchar(255), parentDn varchar(255), primary key (id))");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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

    public void createAttributeTable(AttributeMapping attributeMapping) throws Exception {

        String tableName = "penrose_"+mappingId+"_attribute_"+attributeMapping.getName();

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(tableName);
        sb.append(" (id integer, value ");
        sb.append(getColumnTypeDeclaration(attributeMapping));
        sb.append(", primary key (id, value))");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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

    public String getColumnTypeDeclaration(AttributeMapping attributeMapping) {
        StringBuffer sb = new StringBuffer();

        sb.append(attributeMapping.getType());
        if ("VARCHAR".equals(attributeMapping.getType()) && attributeMapping.getLength() > 0) {
            sb.append("(");
            sb.append(attributeMapping.getLength());
            sb.append(")");
        }

        return sb.toString();
    }

    public String getColumnTypeDeclaration(FieldConfig fieldConfig) {
        StringBuffer sb = new StringBuffer();
        sb.append(fieldConfig.getType());

        if ("VARCHAR".equals(fieldConfig.getType()) && fieldConfig.getLength() > 0) {
            sb.append("(");
            sb.append(fieldConfig.getLength());
            sb.append(")");
        }

        return sb.toString();
    }

    public void createFieldTable(SourceMapping sourceMapping, FieldConfig fieldConfig) throws Exception {

        String tableName = "penrose_"+mappingId+"_field_"+sourceMapping.getName()+"_"+fieldConfig.getName();

        StringBuffer sb = new StringBuffer();
        sb.append("create table ");
        sb.append(tableName);
        sb.append(" (id integer, value ");
        sb.append(getColumnTypeDeclaration(fieldConfig));
        sb.append(", primary key (id, value))");

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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

    public void drop() throws Exception {
        if (!getPartition().isDynamic(getEntryMapping())) {
            String dn = getEntryMapping().getDn();
            remove(dn);
        }

        Collection sources = getPartition().getEffectiveSourceMappings(getEntryMapping());

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            dropEntrySourceTable(sourceMapping);
        }

        Collection attributeDefinitions = getEntryMapping().getAttributeMappings();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeMapping attributeMapping = (AttributeMapping)i.next();
            dropAttributeTable(attributeMapping);
        }

        dropEntriesTable();
    }

    public void dropEntrySourceTable(SourceMapping sourceMapping) throws Exception {

        SourceConfig sourceConfig = getPartition().getSourceConfig(sourceMapping.getSourceName());

        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();
            dropFieldTable(sourceMapping, sourceConfig, fieldConfig);
        }
    }

    public void dropFieldTable(SourceMapping sourceMapping, SourceConfig sourceConfig, FieldConfig fieldConfig) throws Exception {

        String tableName = "penrose_"+mappingId+"_field_"+sourceMapping.getName()+"_"+fieldConfig.getName();

        String sql = "drop table "+tableName;

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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

    public void dropEntriesTable() throws Exception {

        String tableName = "penrose_"+mappingId+"_entries";

        String sql = "drop table "+tableName;

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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

    public void dropAttributeTable(AttributeMapping attributeMapping) throws Exception {

        String tableName = "penrose_"+mappingId+"_attribute_"+attributeMapping.getName();

        String sql = "drop table "+tableName;

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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

    public Entry get(String dn) throws Exception {
        Entry entry = null;

        try {
            log.debug("Getting "+dn);

            int entryId = getEntryId(dn);
            if (entryId == 0) return null;

            AttributeValues attributeValues = new AttributeValues();

            Collection attributeMappings = entryMapping.getAttributeMappings();
            for (Iterator i=attributeMappings.iterator(); i.hasNext(); ) {
                AttributeMapping attributeMapping = (AttributeMapping)i.next();
                Collection values = getAttribute(attributeMapping, entryId);
                attributeValues.set(attributeMapping.getName(), values);
            }

            entry = new Entry(dn, entryMapping, attributeValues);

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine("dn: "+dn, 80));

                for (Iterator i=attributeValues.getNames().iterator(); i.hasNext(); ) {
                    String name = (String)i.next();
                    Collection values = attributeValues.get(name);

                    for (Iterator j=values.iterator(); j.hasNext(); ) {
                        Object value = j.next();
                        log.debug(Formatter.displayLine(name+": "+value, 80));
                    }
                }

                log.debug(Formatter.displaySeparator(80));
            }

            AttributeValues sourceValues = entry.getSourceValues();
            Collection sources = getPartition().getEffectiveSourceMappings(entryMapping);

            for (Iterator i=sources.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();

                SourceConfig sourceConfig = getPartition().getSourceConfig(sourceMapping.getSourceName());

                Collection fields = sourceConfig.getFieldConfigs();
                for (Iterator j=fields.iterator(); j.hasNext(); ) {
                    FieldConfig fieldConfig = (FieldConfig)j.next();

                    Collection values = getField(sourceMapping, fieldConfig, entryId);
                    //log.debug(" - "+sourceMapping.getName()+"."+fieldConfig.getName()+": "+values);

                    sourceValues.set(sourceMapping.getName()+"."+fieldConfig.getName(), values);
                }
            }

        } catch (NamingException e) {
            log.error(e.getMessage());
        }

        return entry;
    }

    public boolean convert(Filter filter, Map tables, Collection parameters, StringBuffer sb) throws Exception {
        if (filter instanceof NotFilter) {
            return convert((NotFilter) filter, tables, parameters, sb);

        } else if (filter instanceof AndFilter) {
            return convert((AndFilter) filter, tables, parameters, sb);

        } else if (filter instanceof OrFilter) {
            return convert((OrFilter) filter, tables, parameters, sb);

        } else if (filter instanceof SimpleFilter) {
            return convert((SimpleFilter) filter, tables, parameters, sb);
        }

        return true;
    }

    public boolean convert(
            SimpleFilter filter,
            Map tables,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        String name = filter.getAttribute();
        String operator = filter.getOperator();
        String value = filter.getValue();

        log.debug("Converting "+name+" "+operator+" "+value);

        if (name.equals("objectClass")) {
            if (value.equals("*")) return true;
        }

        String tableName = "penrose_"+mappingId+"_attribute_"+name;

        String alias = (String)tables.get(tableName);
        if (alias == null) {
            alias = "t"+(tables.size()+1);
            tables.put(tableName, alias);
        }

        sb.append("lower(");
        sb.append(alias);
        sb.append(".value) ");
        sb.append(operator);
        sb.append(" lower(?)");

        parameters.add(value);

        return true;
    }

    public boolean convert(
            NotFilter filter,
            Map tables,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();

        Filter f = filter.getFilter();
        boolean b = convert(f, tables, parameters, sb2);
        if (!b) return false;

        sb.append("not (");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    public boolean convert(
            AndFilter filter,
            Map tables,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            boolean b = convert(f, tables, parameters, sb3);
            if (!b) return false;

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" and ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0) return true;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    public boolean convert(
            OrFilter filter,
            Map tables,
            Collection parameters,
            StringBuffer sb)
            throws Exception {

        StringBuffer sb2 = new StringBuffer();
        for (Iterator i = filter.getFilters().iterator(); i.hasNext();) {
            Filter f = (Filter) i.next();

            StringBuffer sb3 = new StringBuffer();
            boolean b = convert(f, tables, parameters, sb3);
            if (!b) return false;

            if (sb2.length() > 0 && sb3.length() > 0) {
                sb2.append(" or ");
            }

            sb2.append(sb3);
        }

        if (sb2.length() == 0) return true;

        sb.append("(");
        sb.append(sb2);
        sb.append(")");

        return true;
    }

    public boolean contains(String baseDn, Filter filter) throws Exception {

        String tableName = "penrose_"+mappingId+"_entries";

        Map tables = new LinkedHashMap();

        Collection parameters = new ArrayList();
        StringBuffer whereClause = new StringBuffer();

        boolean b = convert(filter, tables, parameters, whereClause);
        if (!b) return false;

        StringBuffer fromClause = new StringBuffer();
        fromClause.append(tableName);
        fromClause.append(" t");

        for (Iterator i=tables.keySet().iterator(); i.hasNext(); ) {
            String tbName = (String)i.next();
            String alias = (String)tables.get(tbName);

            fromClause.append(", ");
            fromClause.append(tbName);
            fromClause.append(" ");
            fromClause.append(alias);

            if (whereClause.length() > 0) whereClause.append(" and ");

            whereClause.append("(");
            whereClause.append(alias);
            whereClause.append(".id = t.id)");
        }

        if (baseDn != null) {
            if (whereClause.length() > 0) whereClause.append(" and ");

            whereClause.append("(t.parentDn = ?)");
            parameters.add(baseDn);
        }

        String sql = "select count(*) from "+fromClause;
        if (whereClause.length() > 0) {
            sql = sql+" where "+whereClause;
        }

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = getConnection();

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

            log.debug(Formatter.displayLine("Parameters:", 80));

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object param = i.next();
                ps.setObject(counter, param);
                log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            rs = ps.executeQuery();

            if (!rs.next()) return false;
            
            long count = rs.getLong(1);

            return count > 0;

        } catch (Exception e) {
            log.error(e.getMessage(), e);

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        return false;
    }

    public boolean search(
            final String baseDn,
            final Filter filter,
            final PenroseSearchResults results
    ) throws Exception {

        log.debug(Formatter.displaySeparator(80));
        log.debug(Formatter.displayLine("Searching entry cache for "+entryMapping.getDn(), 80));
        log.debug(Formatter.displayLine("Filter: "+filter, 80));
        log.debug(Formatter.displayLine("Base DN: "+baseDn, 80));
        log.debug(Formatter.displaySeparator(80));

        String tableName = "penrose_"+mappingId+"_entries";

        Map tables = new LinkedHashMap();

        Collection parameters = new ArrayList();
        StringBuffer whereClause = new StringBuffer();

        boolean b = convert(filter, tables, parameters, whereClause);
        if (!b) return true;

        StringBuffer selectClause = new StringBuffer();
        selectClause.append("t.rdn, t.parentDn");

        StringBuffer fromClause = new StringBuffer();
        fromClause.append(tableName);
        fromClause.append(" t");

        for (Iterator i=tables.keySet().iterator(); i.hasNext(); ) {
            String tbName = (String)i.next();
            String alias = (String)tables.get(tbName);

            fromClause.append(", ");
            fromClause.append(tbName);
            fromClause.append(" ");
            fromClause.append(alias);

            if (whereClause.length() > 0) whereClause.append(" and ");

            whereClause.append("(");
            whereClause.append(alias);
            whereClause.append(".id = t.id)");
        }

        if (baseDn != null) {
            if (whereClause.length() > 0) whereClause.append(" and ");

            whereClause.append("(t.parentDn = ?)");
            parameters.add(baseDn);
        }

        String sql = "select "+selectClause+" from "+fromClause;
        if (whereClause.length() > 0) {
            sql = sql+" where "+whereClause;
        }

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = getConnection();

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

            log.debug(Formatter.displayLine("Parameters:", 80));

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object param = i.next();
                ps.setObject(counter, param);
                log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            rs = ps.executeQuery();

            log.debug(Formatter.displayLine("Results:", 80));

            boolean empty = true;

            while (rs.next()) {
                String rdn = (String)rs.getObject(1);
                String pdn = (String)rs.getObject(2);
                String dn = rdn+","+pdn;
                log.debug(Formatter.displayLine(" - "+dn, 80));
                Entry entry = get(dn);
                results.add(entry);
                empty = false;
            }

            log.debug(Formatter.displaySeparator(80));

            return !empty;

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);
            return false;

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
            results.close();
        }
    }

    public void search(SourceConfig sourceConfig, Row filter, PenroseSearchResults results) throws Exception {

        StringBuffer tableNames = new StringBuffer();
        tableNames.append("penrose_"+mappingId+"_entries t");

        StringBuffer whereClause = new StringBuffer();

        Collection parameters = new ArrayList();

        int c = 1;
        for (Iterator i=filter.getNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();
            Object value = filter.get(name);

            StringBuffer sb = new StringBuffer();

            Collection sourceMappings = entryMapping.getSourceMappings();
            for (Iterator j=sourceMappings.iterator(); j.hasNext(); c++) {
                SourceMapping sourceMapping = (SourceMapping)j.next();
                if (!sourceMapping.getSourceName().equals(sourceConfig.getName())) continue;

                String tableName = "penrose_"+mappingId+"_field_"+sourceMapping.getName()+"_"+name;

                tableNames.append(", ");
                tableNames.append(tableName);
                tableNames.append(" t");
                tableNames.append(c);

                if (sb.length() > 0) sb.append(" or ");

                sb.append("t.id=t");
                sb.append(c);
                sb.append(".id and t");
                sb.append(c);
                sb.append(".value=?");

                parameters.add(value);
            }

            if (whereClause.length() > 0) whereClause.append(" and ");

            whereClause.append("(");
            whereClause.append(sb);
            whereClause.append(")");
        }

        StringBuffer sb = new StringBuffer();
        sb.append("select t.rdn, t.parentDn from ");
        sb.append(tableNames);
        sb.append(" where ");
        sb.append(whereClause);

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = getConnection();

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

            log.debug(Formatter.displayLine("Parameters:", 80));

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object param = i.next();
                ps.setObject(counter, param);
                log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            rs = ps.executeQuery();

            while (rs.next()) {
                String rdn = (String)rs.getObject(1);
                String parentDn = (String)rs.getObject(2);
                String dn = rdn+","+parentDn;
                results.add(dn);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Results: value = "+results, 80));
                log.debug(Formatter.displaySeparator(80));
            }

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            results.setReturnCode(LDAPException.OPERATIONS_ERROR);

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
            results.close();
        }
    }

    public Map getExpired() throws Exception {
        Map results = new TreeMap();
        return results;
    }

    public void put(String dn, Entry entry) throws Exception {

        AttributeValues attributeValues = entry.getAttributeValues();

        log.debug("Storing "+dn);

        int entryId = getEntryId(dn);
        if (entryId == 0) {
            addEntry(dn);
            entryId = getEntryId(dn);
        }

        Collection attributeDefinitions = getEntryMapping().getAttributeMappings();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeMapping attributeDefinition = (AttributeMapping)i.next();

            deleteAttribute(attributeDefinition, entryId);

            Collection values = attributeValues.get(attributeDefinition.getName());
            if (values == null) continue;
            
            for (Iterator j=values.iterator(); j.hasNext(); ) {
                Object value = j.next();
                insertAttribute(attributeDefinition, entryId, value);
            }
        }

        AttributeValues sourceValues = entry.getSourceValues();
        Collection sources = getPartition().getEffectiveSourceMappings(getEntryMapping());

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();

            SourceConfig sourceConfig = getPartition().getSourceConfig(sourceMapping.getSourceName());

            Collection fields = sourceConfig.getFieldConfigs();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();

                deleteField(sourceMapping, fieldConfig, entryId);

                Collection values = sourceValues.get(sourceMapping.getName()+"."+fieldConfig.getName());
                if (values == null) continue;
                
                for (Iterator k=values.iterator(); k.hasNext(); ) {
                    Object value = k.next();
                    insertField(sourceMapping, fieldConfig, entryId, value);
                }
            }
        }
    }

    public void insertAttribute(
            AttributeMapping attributeMapping,
            int entryId,
            Object value
            ) throws Exception {

        String tableName = "penrose_"+mappingId+"_attribute_"+attributeMapping.getName();

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ");
        sb.append(tableName);
        sb.append(" values (?, ?)");

        String sql = sb.toString();

        Collection parameters = new ArrayList();
        parameters.add(new Integer(entryId));
        parameters.add(value);

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters:", 80));
                counter = 1;
                for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                    Object v = i.next();
                    log.debug(Formatter.displayLine(" - "+counter+" = "+v, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public Collection getAttribute(AttributeMapping attributeMapping, int entryId) throws Exception {

        String tableName = "penrose_"+mappingId+"_attribute_"+attributeMapping.getName();

        StringBuffer sb = new StringBuffer();
        sb.append("select value from ");
        sb.append(tableName);
        sb.append(" where id=?");

        String sql = sb.toString();

        Collection parameters = new ArrayList();
        parameters.add(new Integer(entryId));

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        Collection values = new ArrayList();

        try {
            con = getConnection();

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
                log.debug(Formatter.displayLine("Parameters: id = "+entryId, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                Object value = rs.getObject(1);
                values.add(value);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Results: value = "+values, 80));
                log.debug(Formatter.displaySeparator(80));
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

    public void insertField(
            SourceMapping sourceMapping,
            FieldConfig fieldConfig,
            int entryId,
            Object value
            ) throws Exception {

        String tableName = "penrose_"+mappingId+"_field_"+sourceMapping.getName()+"_"+fieldConfig.getName();

        StringBuffer sb = new StringBuffer();
        sb.append("insert into ");
        sb.append(tableName);
        sb.append(" values (?, ?)");

        String sql = sb.toString();

        Collection parameters = new ArrayList();
        parameters.add(new Integer(entryId));
        parameters.add(value);

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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

            log.debug(Formatter.displayLine("Parameters:", 80));

            int counter = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); counter++) {
                Object v = i.next();
                ps.setObject(counter, v);
                log.debug(Formatter.displayLine(" - "+counter+" = "+v, 80));
            }

            log.debug(Formatter.displaySeparator(80));

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public Collection getField(SourceMapping sourceMapping, FieldConfig fieldConfig, int entryId) throws Exception {

        String tableName = "penrose_"+mappingId+"_field_"+sourceMapping.getName()+"_"+fieldConfig.getName();

        StringBuffer sb = new StringBuffer();
        sb.append("select value from ");
        sb.append(tableName);
        sb.append(" where id=?");

        String sql = sb.toString();

        Collection parameters = new ArrayList();
        parameters.add(new Integer(entryId));

        Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        Collection values = new ArrayList();

        try {
            con = getConnection();

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
                log.debug(Formatter.displayLine("Parameters: id = "+entryId, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            rs = ps.executeQuery();

            while (rs.next()) {
                Object value = rs.getObject(1);
                values.add(value);
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Results: value = "+values, 80));
                log.debug(Formatter.displaySeparator(80));
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

    public void remove(String dn) throws Exception {

        Collection sources = getPartition().getEffectiveSourceMappings(getEntryMapping());

        int entryId = getEntryId(dn);

        Collection attributeDefinitions = getEntryMapping().getAttributeMappings();
        for (Iterator i=attributeDefinitions.iterator(); i.hasNext(); ) {
            AttributeMapping attributeDefinition = (AttributeMapping)i.next();

            deleteAttribute(attributeDefinition, entryId);
        }

        for (Iterator i=sources.iterator(); i.hasNext(); ) {
            SourceMapping sourceMapping = (SourceMapping)i.next();

            SourceConfig sourceConfig = getPartition().getSourceConfig(sourceMapping.getSourceName());

            Collection fields = sourceConfig.getFieldConfigs();
            for (Iterator j=fields.iterator(); j.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)j.next();
                deleteField(sourceMapping, fieldConfig, entryId);
            }
        }

        removeEntry(entryId);
    }

    public void deleteAttribute(
            AttributeMapping attributeMapping,
            int entryId) throws Exception {

        String tableName = "penrose_"+mappingId+"_attribute_"+attributeMapping.getName();

        Collection parameters = new ArrayList();

        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(tableName);

        if (entryId > 0) {
            sb.append(" where id=?");
            parameters.add(new Integer(entryId));
        }

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters: id = "+entryId, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps.execute();

        } catch (Exception e) {
            log.error(e.getMessage());

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void deleteField(
            SourceMapping sourceMapping,
            FieldConfig fieldConfig,
            int entryId) throws Exception {

        String tableName = "penrose_"+mappingId+"_field_"+sourceMapping.getName()+"_"+fieldConfig.getName();

        Collection parameters = new ArrayList();

        StringBuffer sb = new StringBuffer();
        sb.append("delete from ");
        sb.append(tableName);

        if (entryId > 0) {
            sb.append(" where id=?");
            parameters.add(new Integer(entryId));
        }

        String sql = sb.toString();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = getConnection();

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
            }

            if (log.isDebugEnabled()) {
                log.debug(Formatter.displayLine("Parameters: id = "+entryId, 80));
                log.debug(Formatter.displaySeparator(80));
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
