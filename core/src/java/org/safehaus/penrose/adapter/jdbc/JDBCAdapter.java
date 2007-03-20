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
package org.safehaus.penrose.adapter.jdbc;

import org.apache.commons.dbcp.*;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.ietf.ldap.LDAPException;
import org.safehaus.penrose.session.SearchResponse;
import org.safehaus.penrose.session.SearchRequest;
import org.safehaus.penrose.session.Modification;
import org.safehaus.penrose.engine.TransformEngine;
import org.safehaus.penrose.util.Formatter;
import org.safehaus.penrose.util.ExceptionUtil;
import org.safehaus.penrose.util.LDAPUtil;
import org.safehaus.penrose.filter.Filter;
import org.safehaus.penrose.filter.SubstringFilter;
import org.safehaus.penrose.filter.SimpleFilter;
import org.safehaus.penrose.mapping.*;
import org.safehaus.penrose.partition.FieldConfig;
import org.safehaus.penrose.partition.SourceConfig;
import org.safehaus.penrose.partition.Partition;
import org.safehaus.penrose.connector.*;
import org.safehaus.penrose.entry.RDNBuilder;
import org.safehaus.penrose.entry.RDN;
import org.safehaus.penrose.entry.AttributeValues;
import org.safehaus.penrose.entry.Attribute;
import org.safehaus.penrose.adapter.Adapter;
import org.safehaus.penrose.interpreter.Interpreter;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author Endi S. Dewata
 */
public class JDBCAdapter extends Adapter {

    public final static String DRIVER       = "driver";
    public final static String URL          = "url";
    public final static String USER         = "user";
    public final static String PASSWORD     = "password";

    public final static String CATALOG      = "catalog";
    public final static String SCHEMA       = "schema";
    public final static String TABLE        = "table";
    public final static String TABLE_NAME   = "tableName";
    public final static String FILTER       = "filter";

    public final static String INITIAL_SIZE                         = "initialSize";
    public final static String MAX_ACTIVE                           = "maxActive";
    public final static String MAX_IDLE                             = "maxIdle";
    public final static String MIN_IDLE                             = "minIdle";
    public final static String MAX_WAIT                             = "maxWait";

    public final static String VALIDATION_QUERY                     = "validationQuery";
    public final static String TEST_ON_BORROW                       = "testOnBorrow";
    public final static String TEST_ON_RETURN                       = "testOnReturn";
    public final static String TEST_WHILE_IDLE                      = "testWhileIdle";
    public final static String TIME_BETWEEN_EVICTION_RUNS_MILLIS    = "timeBetweenEvictionRunsMillis";
    public final static String NUM_TESTS_PER_EVICTION_RUN           = "numTestsPerEvictionRun";
    public final static String MIN_EVICTABLE_IDLE_TIME_MILLIS       = "minEvictableIdleTimeMillis";

    public final static String SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS  = "softMinEvictableIdleTimeMillis";
    public final static String WHEN_EXHAUSTED_ACTION                = "whenExhaustedAction";

    GenericObjectPool connectionPool;
    public DataSource ds;

    public void init() throws Exception {

        Properties properties = new Properties();
        for (Iterator i=getParameterNames().iterator(); i.hasNext(); ) {
            String param = (String)i.next();
            String value = getParameter(param);
            properties.setProperty(param, value);
        }

        String driver = (String)properties.remove(DRIVER);
        String url = (String)properties.remove(URL);

        Class.forName(driver);

        GenericObjectPool.Config config = new GenericObjectPool.Config();

        String s = (String)properties.remove(INITIAL_SIZE);
        int initialSize = s == null ? 0 : Integer.parseInt(s);

        s = (String)properties.remove(MAX_ACTIVE);
        if (s != null) config.maxActive = Integer.parseInt(s);

        s = (String)properties.remove(MAX_IDLE);
        if (s != null) config.maxIdle = Integer.parseInt(s);

        s = (String)properties.remove(MAX_WAIT);
        if (s != null) config.maxWait = Integer.parseInt(s);

        s = (String)properties.remove(MIN_EVICTABLE_IDLE_TIME_MILLIS);
        if (s != null) config.minEvictableIdleTimeMillis = Integer.parseInt(s);

        s = (String)properties.remove(MIN_IDLE);
        if (s != null) config.minIdle = Integer.parseInt(s);

        s = (String)properties.remove(NUM_TESTS_PER_EVICTION_RUN);
        if (s != null) config.numTestsPerEvictionRun = Integer.parseInt(s);

        s = (String)properties.remove(TEST_ON_BORROW);
        if (s != null) config.testOnBorrow = new Boolean(s).booleanValue();

        s = (String)properties.remove(TEST_ON_RETURN);
        if (s != null) config.testOnReturn = new Boolean(s).booleanValue();

        s = (String)properties.remove(TEST_WHILE_IDLE);
        if (s != null) config.testWhileIdle = new Boolean(s).booleanValue();

        s = (String)properties.remove(TIME_BETWEEN_EVICTION_RUNS_MILLIS);
        if (s != null) config.timeBetweenEvictionRunsMillis = Integer.parseInt(s);

        //s = (String)properties.remove(SOFT_MIN_EVICTABLE_IDLE_TIME_MILLIS);
        //if (s != null) config.softMinEvictableIdleTimeMillis = Integer.parseInt(s);

        //s = (String)properties.remove(WHEN_EXHAUSTED_ACTION);
        //if (s != null) config.whenExhaustedAction = Byte.parseByte(s);

        connectionPool = new GenericObjectPool(null, config);

        String validationQuery = (String)properties.remove(VALIDATION_QUERY);

        ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(url, properties);

        //PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(
                        connectionFactory,
                        connectionPool,
                        null, // statement pool factory
                        validationQuery, // test query
                        false, // read only
                        true // auto commit
                );

        log.debug("Initializing "+initialSize+" connections.");
        for (int i = 0; i < initialSize; i++) {
             connectionPool.addObject();
         }

        ds = new PoolingDataSource(connectionPool);
    }

    public void dispose() throws Exception {
        connectionPool.close();
    }

    public Object openConnection() throws Exception {
        return ds.getConnection();
    }

    public String getFieldNames(SourceConfig sourceConfig) throws Exception {
        StringBuilder sb = new StringBuilder();

        Collection fields = sourceConfig.getFieldConfigs();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            if (sb.length() > 0) sb.append(", ");
            sb.append(fieldConfig.getOriginalName());
        }

        return sb.toString();
    }

    public String getOringialPrimaryKeyFieldNamesAsString(SourceConfig sourceConfig) throws Exception {
        StringBuilder sb = new StringBuilder();

        Collection fields = sourceConfig.getOriginalPrimaryKeyNames();
        for (Iterator i=fields.iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            if (sb.length() > 0) sb.append(", ");
            sb.append(name);
        }

        return sb.toString();
    }

    public String getTableName(SourceConfig sourceConfig) {
        String catalog = sourceConfig.getParameter(CATALOG);
        String schema = sourceConfig.getParameter(SCHEMA);
        String table = sourceConfig.getParameter(TABLE);

        if (table == null) table = sourceConfig.getParameter(TABLE_NAME);
        if (catalog != null) table = catalog +"."+table;
        if (schema != null) table = schema +"."+table;

        return table;
    }

    public void bind(SourceConfig sourceConfig, RDN pk, String cred) throws Exception {
        throw ExceptionUtil.createLDAPException(LDAPException.INVALID_CREDENTIALS);
    }

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            SourceMapping sourceMapping,
            SourceConfig sourceConfig,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - Filter: "+request.getFilter(), 80));
            log.debug(Formatter.displayLine(" - Scope: "+request.getScope(), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        String table = getTableName(sourceConfig);
        String s = sourceConfig.getParameter(FILTER);

        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        sb.append(getFieldNames(sourceConfig));
        sb.append(" from ");
        sb.append(table);

        StringBuilder sqlFilter = new StringBuilder();
        if (s != null) sqlFilter.append(s);

        List parameterValues = new ArrayList();
        List parameterFieldConfigs = new ArrayList();

        Interpreter interpreter = penroseContext.getInterpreterManager().newInstance();

        JDBCFilterGenerator filterGenerator = new JDBCFilterGenerator(
                partition,
                entryMapping,
                sourceMapping,
                interpreter,
                parameterValues,
                parameterFieldConfigs,
                request.getFilter()
        );

        filterGenerator.run();

        String sourceFilter = filterGenerator.getJdbcFilter();
        if (sourceFilter != null) {
            if (sqlFilter.length() > 0) sqlFilter.append(" and ");
            sqlFilter.append(sourceFilter);
        }

        if (sqlFilter.length() > 0) {
            sb.append(" where ");
            sb.append(sqlFilter);
        }

        if (!sourceConfig.getOriginalPrimaryKeyNames().isEmpty()) {
            sb.append(" order by ");
            sb.append(getOringialPrimaryKeyFieldNamesAsString(sourceConfig));
        }

        String sql = sb.toString();

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = (java.sql.Connection)openConnection();

            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
    
            if (parameterValues.size() > 0) {
                int counter = 0;
                for (Iterator i=parameterValues.iterator(), j=parameterFieldConfigs.iterator(); i.hasNext() && j.hasNext(); ) {
                    Object param = i.next();
                    FieldConfig fieldConfig = (FieldConfig)j.next();
                    setParameter(ps, ++counter, param, fieldConfig);
                }

                if (debug) {
                    log.debug("Parameters:");

                    counter = 0;
                    for (Iterator i=parameterValues.iterator(), j=parameterFieldConfigs.iterator(); i.hasNext() && j.hasNext(); ) {
                        Object param = i.next();
                        FieldConfig fieldConfig = (FieldConfig)j.next();
                        String type = fieldConfig.getType();
                        log.debug(" - "+counter+" = "+param+" ("+type+")");
                    }
                }
            }

            rs = ps.executeQuery();

            int totalCount = response.getTotalCount();
            long sizeLimit = request.getSizeLimit();

            if (debug) {
                if (sizeLimit == 0) {
                    log.debug("Retrieving all entries.");
                } else {
                    log.debug("Retrieving "+(sizeLimit - totalCount)+" entries.");
                }
            }

            boolean hasMore = rs.next();

            while (hasMore && (sizeLimit == 0 || totalCount<sizeLimit)) {
                AttributeValues record = new AttributeValues();
                RDN pk = getRecord(sourceConfig, rs, record);

                if (debug) {
                    JDBCFormatter.printRecord(record, pk);
                }

                ConnectorSearchResult result = new ConnectorSearchResult(record);
                result.setEntryMapping(entryMapping);
                result.setSourceMapping(sourceMapping);
                result.setSourceConfig(sourceConfig);

                response.add(result);
                totalCount++;
                hasMore = rs.next();
            }

            if (sizeLimit != 0 && hasMore) {
                log.debug("Size limit exceeded.");
                throw ExceptionUtil.createLDAPException(LDAPException.SIZE_LIMIT_EXCEEDED);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}

            response.close();
        }
    }

    public void search(
            Partition partition,
            EntryMapping entryMapping,
            Collection sourceMappings,
            SearchRequest request,
            SearchResponse response
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        Filter filter = request.getFilter();

        if (debug) {
            Collection names = new ArrayList();
            for (Iterator i=sourceMappings.iterator(); i.hasNext(); ) {
                SourceMapping sourceMapping = (SourceMapping)i.next();
                names.add(sourceMapping.getName());
            }

            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("Search "+names, 80));
            log.debug(Formatter.displayLine(" - Filter: "+filter, 80));
            log.debug(Formatter.displayLine(" - Scope: "+ LDAPUtil.getScope(request.getScope()), 80));
            log.debug(Formatter.displaySeparator(80));
        }

        JDBCQueryGenerator queryGenerator = new JDBCQueryGenerator(
                this,
                partition,
                entryMapping,
                sourceMappings,
                penroseContext.getInterpreterManager().newInstance(),
                request,
                response
        );

        queryGenerator.run();

        String sql = queryGenerator.getSql();

        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            Collection lines = Formatter.split(sql, 80);
            for (Iterator i=lines.iterator(); i.hasNext(); ) {
                String line = (String)i.next();
                log.debug(Formatter.displayLine(line, 80));
            }
            log.debug(Formatter.displaySeparator(80));

            log.debug("Parameters:");

            int counter = 1;
            Collection parameterValues = queryGenerator.getParameterValues();
            Collection parameterFieldConfigs = queryGenerator.getParameterFieldCofigs();
            for (Iterator i=parameterValues.iterator(), j=parameterFieldConfigs.iterator(); i.hasNext() && j.hasNext(); counter++) {
                Object param = i.next();
                FieldConfig fieldConfig = (FieldConfig)j.next();
                String type = fieldConfig.getType();
                log.debug(" - "+counter+" = "+param+" ("+type+")");
            }
        }

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = (java.sql.Connection)openConnection();

            ps = con.prepareStatement(sql);

            int counter = 1;
            Collection parameterValues = queryGenerator.getParameterValues();
            Collection parameterFieldConfigs = queryGenerator.getParameterFieldCofigs();
            for (Iterator i=parameterValues.iterator(), j=parameterFieldConfigs.iterator(); i.hasNext() && j.hasNext(); counter++) {
                Object param = i.next();
                FieldConfig fieldConfig = (FieldConfig)j.next();
                setParameter(ps, counter, param, fieldConfig);
            }

            rs = ps.executeQuery();

            RDN lastPk = null;
            AttributeValues lastRecord = null;

            boolean hasMore = rs.next();
            while (hasMore) {
                AttributeValues record = new AttributeValues();
                RDN pk = getRecord(partition, sourceMappings, rs, record);

                if (lastPk == null) {
                    lastPk = pk;
                    lastRecord = record;

                } else if (pk.equals(lastPk)) {
                    lastRecord.add(record);

                } else {
                    ConnectorSearchResult result = new ConnectorSearchResult(lastRecord);
                    result.setEntryMapping(entryMapping);
                    //result.setSourceMapping(sourceMapping);
                    //result.setSourceConfig(sourceConfig);
                    response.add(result);

                    lastPk = pk;
                    lastRecord = record;
                }

                if (debug) {
                    JDBCFormatter.printRecord(record, pk);
                }

                hasMore = rs.next();
            }

            if (lastPk != null) {
                ConnectorSearchResult result = new ConnectorSearchResult(lastRecord);
                result.setEntryMapping(entryMapping);
                //result.setSourceMapping(sourceMapping);
                //result.setSourceConfig(sourceConfig);
                response.add(result);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}

            response.close();
        }
    }

    public RDN getPkValues(SourceConfig sourceConfig, ResultSet rs) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        int c = 1;

        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();

        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();

        for (Iterator i=fields.iterator(); i.hasNext() && c<=count; c++) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            Object value = rs.getObject(c);
            if (value == null) continue;

            rb.set(fieldConfig.getName(), value);
        }

        //log.debug("=> values: "+rb);

        return rb.toRdn();
    }

    public RDN getChanges(SourceConfig sourceConfig, ResultSet rs) throws Exception {

        RDNBuilder rb = new RDNBuilder();
        rb.set("changeNumber", rs.getObject("changeNumber"));
        rb.set("changeTime", rs.getObject("changeTime"));
        rb.set("changeAction", rs.getObject("changeAction"));
        rb.set("changeUser", rs.getObject("changeUser"));

        int counter = 5;
        for (Iterator i=sourceConfig.getPrimaryKeyNames().iterator(); i.hasNext(); ) {
            String name = (String)i.next();

            Object value = rs.getObject(counter++);
            if (value == null) continue;

            rb.set(name, value);
        }

        return rb.toRdn();
    }

    public RDN getRecord(
            Partition partition,
            Collection sourceMappings,
            ResultSet rs,
            AttributeValues record
    ) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        int source = 1;
        int column = 1;

        for (Iterator i=sourceMappings.iterator(); i.hasNext(); source++) {
            SourceMapping sourceMapping = (SourceMapping)i.next();
            String sourceName = sourceMapping.getName();

            SourceConfig sourceConfig = partition.getSourceConfig(sourceMapping);

            Collection fieldConfigs = sourceConfig.getFieldConfigs();
            for (Iterator j=fieldConfigs.iterator(); j.hasNext(); column++) {
                FieldConfig fieldConfig = (FieldConfig)j.next();

                Object value = rs.getObject(column);
                if (value == null) continue;

                String fieldName = fieldConfig.getName();
                String name = sourceName+"."+fieldName;

                record.add(name, value);

                if (source != 1 || !fieldConfig.isPrimaryKey()) continue;
                rb.set(name, value);
            }
        }

        //record.set("primaryKey", rb.toRdn());
        //log.debug("=> values: "+record);

        return rb.toRdn();
    }

    public RDN getRecord(
            SourceConfig sourceConfig,
            ResultSet rs,
            AttributeValues record
    ) throws Exception {

        RDNBuilder rb = new RDNBuilder();

        ResultSetMetaData rsmd = rs.getMetaData();
        int count = rsmd.getColumnCount();

        for (int c = 1; c<=count; c++) {
            String originalName = rsmd.getColumnName(c);
            FieldConfig fieldConfig = sourceConfig.getFieldConfigByOriginalName(originalName);
            if (fieldConfig == null) {
                // throw new Exception("Unknown field: "+originalName);
            	continue;
            }

            Object value = rs.getObject(c);
            if (value == null) continue;

            String name = fieldConfig.getName();
            value = formatAttributeValue(rsmd, c, value, fieldConfig);
            record.add(name, value);

            if (!fieldConfig.isPrimaryKey()) continue;
            rb.set(name, value);
        }

        //record.set("primaryKey", rb.toRdn());
        //log.debug("=> values: "+record);

        return rb.toRdn();
    }

    public void add(SourceConfig sourceConfig, RDN pk, AttributeValues sourceValues) throws Exception {

        boolean debug = log.isDebugEnabled();
        if (debug) {
            log.debug(Formatter.displaySeparator(80));
            log.debug(Formatter.displayLine("JDBC Add "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName(), 80));
            log.debug(Formatter.displayLine(" - DN: "+pk, 80));
            log.debug(Formatter.displaySeparator(80));
        }

        // convert sets into single values
        Collection rows = TransformEngine.convert(sourceValues);
    	RDN rdn = (RDN)rows.iterator().next();

        String table = getTableName(sourceConfig);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = (java.sql.Connection)openConnection();

            StringBuilder sb = new StringBuilder();
            StringBuilder sb2 = new StringBuilder();

            Collection fieldConfigs = sourceConfig.getFieldConfigs();
            Collection parameters = new ArrayList();
            for (Iterator i=fieldConfigs.iterator(); i.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)i.next();

                if (sb.length() > 0) {
                    sb.append(", ");
                    sb2.append(", ");
                }

                sb.append(fieldConfig.getOriginalName());
                sb2.append("?");

                Object obj = rdn.get(fieldConfig.getName());
                parameters.add(obj);
            }

            String sql = "insert into "+table+" ("+sb+") values ("+sb2+")";

            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            if (debug) {
            	log.debug(Formatter.displayLine("Parameters:", 80));
            }

            int c = 1;
            for (Iterator i=parameters.iterator(), j=fieldConfigs.iterator(); i.hasNext() && j.hasNext(); c++) {
                Object obj = i.next();
                FieldConfig fieldConfig = (FieldConfig)j.next();
                setParameter(ps, c, obj, fieldConfig);
                if (debug) {
                	log.debug(Formatter.displayLine(" - "+c+" = "+(obj == null ? null : obj.toString()), 80));
                }
            }

            if (debug) {
            	log.debug(Formatter.displaySeparator(80));
            }

            ps.executeUpdate();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void delete(SourceConfig sourceConfig, RDN pk) throws Exception {

        boolean debug = log.isDebugEnabled();
        //log.debug("Deleting entry "+pk);

        String table = getTableName(sourceConfig);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = (java.sql.Connection)openConnection();

            StringBuilder sb = new StringBuilder();
            for (Iterator i=pk.getNames().iterator(); i.hasNext(); ) {
                String name = (String)i.next();

                if (sb.length() > 0) sb.append(" and ");

                sb.append(name);
                sb.append("=?");
            }

            String sql = "delete from "+table+" where "+sb;

            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            if (debug) {
            	log.debug(Formatter.displayLine("Parameters:", 80));
            }

            int c = 1;
            for (Iterator i=pk.getNames().iterator(); i.hasNext(); c++) {
                String name = (String)i.next();
                Object value = pk.get(name);
                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
                setParameter(ps, c, value, fieldConfig);
                if (debug) {
                	log.debug(Formatter.displayLine(" - "+c+" = "+value, 80));
                }
            }

            if (debug) {
            	log.debug(Formatter.displaySeparator(80));
            }

            int count = ps.executeUpdate();
            if (count == 0) throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void modify(SourceConfig sourceConfig, RDN pk, Collection modifications) throws Exception {

        boolean debug = log.isDebugEnabled();

        String table = getTableName(sourceConfig);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = (java.sql.Connection)openConnection();

            StringBuilder columns = new StringBuilder();
            StringBuilder whereClause = new StringBuilder();
            Collection parameters = new ArrayList();
            Collection fieldConfigs = new ArrayList();
            
            for (Iterator i=modifications.iterator(); i.hasNext(); ) {
                Modification mi = (Modification)i.next();

                int type = mi.getType();
                Attribute attribute = mi.getAttribute();
                String name = attribute.getName();

                FieldConfig fieldConfig = sourceConfig.getFieldConfig(name);
                if (fieldConfig == null) {
                    throw new Exception("Unknown field: "+name);
                }
                fieldConfigs.add(fieldConfig);
                
                switch (type) {
                    case Modification.ADD:
                        if (columns.length() > 0) columns.append(", ");

                        columns.append(fieldConfig.getOriginalName());
                        columns.append("=?");
                        parameters.add(attribute.getValue());
                        break;

                    case Modification.REPLACE:
                        if (columns.length() > 0) columns.append(", ");

                        columns.append(fieldConfig.getOriginalName());
                        columns.append("=?");
                        parameters.add(attribute.getValue());
                        break;

                    case Modification.DELETE:
                        if (columns.length() > 0) columns.append(", ");

                        columns.append(fieldConfig.getOriginalName());
                        columns.append("=?");
                        parameters.add(null);
                        break;
                }
            }

            // if there's nothing to update, return
            if (columns.length() == 0) throw ExceptionUtil.createLDAPException(LDAPException.SUCCESS);
/*
            Collection fields = sourceConfig.getFieldConfigs();
            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)i.next();
                if (fieldConfig.isPrimaryKey()) continue;

                if (columns.length() > 0) columns.append(", ");

                columns.append(fieldConfig.getOriginalName());
                columns.append("=?");

                Object value = sourceValues.getOne(fieldConfig.getName());
                parameters.add(value);
            }
*/
            Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)i.next();
                fieldConfigs.add(fieldConfig);
                
                if (whereClause.length() > 0) whereClause.append(" and ");

                whereClause.append(fieldConfig.getOriginalName());
                whereClause.append("=?");

                Object value = pk.get(fieldConfig.getName());
                parameters.add(value);
            }

            String sql = "update "+table+" set "+columns+" where "+whereClause;

            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            if (debug) {
            	log.debug(Formatter.displayLine("Parameters:", 80));
            }

            int c = 1;
            for (Iterator i=parameters.iterator(), j=fieldConfigs.iterator(); i.hasNext() && j.hasNext(); c++) {
                Object value = i.next();
                FieldConfig fieldConfig = (FieldConfig)j.next();
                setParameter(ps, c, value, fieldConfig);
                if (debug) {
                	log.debug(Formatter.displayLine(" - "+c+" = "+value, 80));
                }
            }

            if (debug) {
            	log.debug(Formatter.displaySeparator(80));
            }

            int count = ps.executeUpdate();
            if (count == 0) throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public void modrdn(
            SourceConfig sourceConfig,
            RDN oldRdn,
            RDN newRdn,
            boolean deleteOldRdn
    ) throws Exception {

        boolean debug = log.isDebugEnabled();
        //log.debug("Renaming source "+source.getName()+": "+oldRdn+" with "+newRdn);

        String table = getTableName(sourceConfig);

        java.sql.Connection con = null;
        PreparedStatement ps = null;

        try {
            con = (java.sql.Connection)openConnection();

            StringBuilder columns = new StringBuilder();
            StringBuilder whereClause = new StringBuilder();
            Collection parameters = new ArrayList();

            Collection fields = sourceConfig.getFieldConfigs();
            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)i.next();
                if (!fieldConfig.isPrimaryKey()) continue;

                Object value = newRdn.get(fieldConfig.getName());
                if (value == null) continue;

                if (columns.length() > 0) columns.append(", ");

                columns.append(fieldConfig.getOriginalName());
                columns.append("=?");

                parameters.add(value);
            }

            for (Iterator i=fields.iterator(); i.hasNext(); ) {
                FieldConfig fieldConfig = (FieldConfig)i.next();
                if (!fieldConfig.isPrimaryKey()) continue;

                Object value = oldRdn.get(fieldConfig.getName());
                if (value == null) continue;

                if (whereClause.length() > 0) whereClause.append(" and ");

                whereClause.append(fieldConfig.getOriginalName());
                whereClause.append("=?");

                parameters.add(value);
            }

            String sql = "update "+table+" set "+columns+" where "+whereClause;

            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            if (debug) {
            	log.debug(Formatter.displayLine("Parameters:", 80));
            }

            int c = 1;
            for (Iterator i=parameters.iterator(); i.hasNext(); c++) {
                Object value = i.next();
                ps.setObject(c, value);
                if (debug) {
                	log.debug(Formatter.displayLine(" - "+c+" = "+value, 80));
                }
            }

            if (debug) {
            	log.debug(Formatter.displaySeparator(80));
            }

            int count = ps.executeUpdate();
            if (count == 0) throw ExceptionUtil.createLDAPException(LDAPException.NO_SUCH_OBJECT);

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public int getLastChangeNumber(SourceConfig sourceConfig) throws Exception {

        boolean debug = log.isDebugEnabled();

        String table = getTableName(sourceConfig);

        String sql = "select max(changeNumber) from "+table+"_changes";

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = (java.sql.Connection)openConnection();

            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                log.debug(Formatter.displayLine(sql, 80));
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);
            rs = ps.executeQuery();

            if (!rs.next()) return 0;

            Integer value = (Integer)rs.getObject(1);
            log.debug("Last change number: "+value);
            
            if (value == null) return 0;

            return value.intValue();

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }
    }

    public SearchResponse getChanges(SourceConfig sourceConfig, int lastChangeNumber) throws Exception {

        boolean debug = log.isDebugEnabled();
        //log.debug("Searching JDBC source "+sourceConfig.getConnectionName()+"/"+sourceConfig.getName());

        SearchResponse response = new SearchResponse();

        String table = getTableName(sourceConfig);

        int sizeLimit = 100;

        StringBuilder columns = new StringBuilder();
        columns.append("select changeNumber, changeTime, changeAction, changeUser");

        StringBuilder sb = new StringBuilder();
        sb.append("from ");
        sb.append(table);
        sb.append("_changes");

        for (Iterator i=sourceConfig.getPrimaryKeyFieldConfigs().iterator(); i.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)i.next();

            columns.append(", ");
            columns.append(fieldConfig.getOriginalName());
        }

        StringBuilder whereClause = new StringBuilder();
        whereClause.append("where changeNumber > ? order by changeNumber");

        List parameters = new ArrayList();
        parameters.add(new Integer(lastChangeNumber));

        String sql = columns+" "+sb+" "+whereClause;

        java.sql.Connection con = null;
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            con = (java.sql.Connection)openConnection();

            if (debug) {
                log.debug(Formatter.displaySeparator(80));
                Collection lines = Formatter.split(sql, 80);
                for (Iterator i=lines.iterator(); i.hasNext(); ) {
                    String line = (String)i.next();
                    log.debug(Formatter.displayLine(line, 80));
                }
                log.debug(Formatter.displaySeparator(80));
            }

            ps = con.prepareStatement(sql);

            if (debug) {
            	log.debug("Parameters: changeNumber = "+lastChangeNumber);
            }

            int counter = 0;
            for (Iterator i=parameters.iterator(); i.hasNext(); ) {
                Object param = i.next();
                ps.setObject(++counter, param);
                if (debug) {
                	log.debug(Formatter.displayLine(" - "+counter+" = "+param, 80));
                }
            }

            rs = ps.executeQuery();

            int width = 0;
            boolean first = true;

            for (int i=0; rs.next() && (sizeLimit == 0 || i<sizeLimit); i++) {
                RDN rdn = getChanges(sourceConfig, rs);
                response.add(rdn);

                if (first) {
                    width = printChangesHeader(sourceConfig);
                    first = false;
                }

                printChanges(sourceConfig, rdn);
            }

            JDBCFormatter.printFooter(sourceConfig);

            if (rs.next()) {
                log.debug("RC: size limit exceeded.");
                throw ExceptionUtil.createLDAPException(LDAPException.SIZE_LIMIT_EXCEEDED);
            }

        } finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception e) {}
        }

        response.close();

        return response;
    }

    public int printChangesHeader(SourceConfig sourceConfig) throws Exception {

        StringBuilder resultHeader = new StringBuilder();
        resultHeader.append("| ");
        resultHeader.append(Formatter.rightPad("#", 5));
        resultHeader.append(" | ");
        resultHeader.append(Formatter.rightPad("time", 19));
        resultHeader.append(" | ");
        resultHeader.append(Formatter.rightPad("action", 10));
        resultHeader.append(" | ");
        resultHeader.append(Formatter.rightPad("user", 10));
        resultHeader.append(" |");

        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)j.next();

            String name = fieldConfig.getName();
            int length = fieldConfig.getLength() > 15 ? 15 : fieldConfig.getLength();

            resultHeader.append(" ");
            resultHeader.append(Formatter.rightPad(name, length));
            resultHeader.append(" |");
        }

        int width = resultHeader.length();

        log.debug("Results:");
        log.debug(Formatter.displaySeparator(width));
        log.debug(resultHeader.toString());
        log.debug(Formatter.displaySeparator(width));

        return width;
    }

    public void printChanges(SourceConfig sourceConfig, RDN rdn) throws Exception {
        StringBuilder resultFields = new StringBuilder();
        resultFields.append("| ");
        resultFields.append(Formatter.rightPad(rdn.get("changeNumber").toString(), 5));
        resultFields.append(" | ");
        resultFields.append(Formatter.rightPad(rdn.get("changeTime").toString(), 19));
        resultFields.append(" | ");
        resultFields.append(Formatter.rightPad(rdn.get("changeAction").toString(), 10));
        resultFields.append(" | ");
        resultFields.append(Formatter.rightPad(rdn.get("changeUser").toString(), 10));
        resultFields.append(" |");

        Collection fields = sourceConfig.getPrimaryKeyFieldConfigs();
        for (Iterator j=fields.iterator(); j.hasNext(); ) {
            FieldConfig fieldConfig = (FieldConfig)j.next();

            Object value = rdn.get(fieldConfig.getName());
            int length = fieldConfig.getLength() > 15 ? 15 : fieldConfig.getLength();

            resultFields.append(" ");
            resultFields.append(Formatter.rightPad(value == null ? "null" : value.toString(), length));
            resultFields.append(" |");
        }

        log.debug(resultFields.toString());
    }

    public Filter convert(EntryMapping entryMapping, SubstringFilter filter) throws Exception {

        String attributeName = filter.getAttribute();
        Collection substrings = filter.getSubstrings();

        AttributeMapping attributeMapping = entryMapping.getAttributeMapping(attributeName);
        String variable = attributeMapping.getVariable();

        if (variable == null) return null;

        int index = variable.indexOf(".");
        String sourceName = variable.substring(0, index);
        String fieldName = variable.substring(index+1);

        StringBuilder sb = new StringBuilder();
        for (Iterator i=substrings.iterator(); i.hasNext(); ) {
            Object o = i.next();
            if (o.equals(SubstringFilter.STAR)) {
                sb.append("%");
            } else {
                String substring = (String)o;
                sb.append(substring);
            }
        }

        return new SimpleFilter(fieldName, "like", sb.toString());
    }

    protected void setParameter(PreparedStatement ps, int paramIndex, Object value, FieldConfig fieldConfig) throws Exception {
    	ps.setObject(paramIndex, value);
    }
    
    protected Object formatAttributeValue(ResultSetMetaData rsmd, int column, Object value, FieldConfig fieldConfig) {
    	return value;
    }
}
