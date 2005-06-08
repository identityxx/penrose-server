/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import org.apache.log4j.Logger;
import org.safehaus.penrose.Penrose;
import org.safehaus.penrose.mapping.Source;

import javax.sql.DataSource;

/**
 * @author Endi S. Dewata
 */
public class PenroseSourceHome {

	protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

	public final static String PENROSE_SOURCES_TABLE = "penrose_sources";

	public int debug = 0;
    private DataSource ds;

	public PenroseSourceHome(DataSource ds) throws Exception {

        this.ds = ds;

        try {
            drop();
        } catch (Exception e) {
            // ignore
        }

        create();
    }

    public void create() throws Exception {

        Connection con = null;
		PreparedStatement ps = null;

		try {
            con = ds.getConnection();

            String sql = "create table " + PENROSE_SOURCES_TABLE + " ("
                    + "source varchar(200), "
                    + "expiration datetime, "
                    + "modifyTime datetime, "
                    + "primary key (source))";

            log.debug("Executing " + sql);
            ps = con.prepareStatement(sql);
            ps.execute();

		} finally {
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

    public void drop() throws Exception {

        Connection con = null;
		PreparedStatement ps = null;

		try {
            con = ds.getConnection();

            String sql = "drop table " + PENROSE_SOURCES_TABLE;
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.execute();

		} finally {
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

	public Collection showAll() throws Exception {
        Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Collection result = new ArrayList();

		try {
            con = ds.getConnection();

			String sql = "select source, expiration, modifyTime from "
					+ PENROSE_SOURCES_TABLE;
			log.debug("Executing " + sql);

			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();

			log.debug("SOURCES --------------------------------------------------");
			while (rs.next()) {
				String source = rs.getString(1);
				Date expiration = rs.getTimestamp(2) == null ? null : new Date(
						rs.getTimestamp(2).getTime());
				Date modifyTime = rs.getTimestamp(3) == null ? null : new Date(
						rs.getTimestamp(3).getTime());
				log.debug("| "
                        + source + " | "
						+ expiration + " | "
                        + modifyTime + " |");
				result.add(source);
			}
			log.debug("SOURCES --------------------------------------------------");

		} finally {
			if (rs != null) try { rs.close(); } catch (Exception e) {}
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}

		return result;
	}

	public Date getExpiration(Source source) throws Exception {
        Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
            con = ds.getConnection();

			String sql = "select expiration from " + PENROSE_SOURCES_TABLE
					+ " where source = ?";
			log.debug("Executing " + sql);

			ps = con.prepareStatement(sql);
			ps.setString(1, source.getSourceName());
			rs = ps.executeQuery();

			if (!rs.next()) {
				log.debug("Cache for " + source.getSourceName() + " never created");
				return null;
			}

			Date expiration = rs.getTimestamp(1) == null ? null : new Date(rs.getTimestamp(1).getTime());
			log.debug("Cache expiration for "+source.getSourceName()+": "+expiration);
			return expiration;

		} finally {
			if (rs != null) try { rs.close(); } catch (Exception e) {}
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

	public Date getModifyTime(Source source) throws Exception {
        Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
            con = ds.getConnection();

			String sql = "select modifyTime from " + PENROSE_SOURCES_TABLE
					+ " where source = ?";
			log.debug("Executing " + sql);

			ps = con.prepareStatement(sql);
			ps.setString(1, source.getSourceName());
			rs = ps.executeQuery();

			if (!rs.next()) {
				log.debug("Cache for " + source.getSourceName() + " never created");
				return null;
			}

			Date modifyTime = rs.getTimestamp(1) == null ? null : new Date(rs.getTimestamp(1).getTime());
			log.debug("Cache for " + source.getSourceName() + " was modified on " + modifyTime);
			return modifyTime;

		} finally {
            if (rs != null) try { rs.close(); } catch (Exception e) {}
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

	public void setModifyTime(
            Source source,
            Date modifyTime)
			throws Exception {

		log.debug("Set modifyTime for " + source.getSourceName());

        Connection con = null;
		PreparedStatement ps = null;

		try {
            con = ds.getConnection();

			String sql = "update "
					+ PENROSE_SOURCES_TABLE
					+ " set modifyTime = ? where source = ?";
			log.debug("Executing " + sql);

			ps = con.prepareStatement(sql);
			ps.setTimestamp(1, new Timestamp(modifyTime.getTime()));
			ps.setString(2, source.getSourceName());
			ps.execute();

		} finally {
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

    public void insert(Source source) throws Exception {

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            String sql = "insert into " + PENROSE_SOURCES_TABLE
                    + " values (?, null, null)";
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.setString(1, source.getSourceName());
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

	public void setExpiration(Source source, Date time)
			throws Exception {

		log.debug("Setting cache expiration for " + source.getSourceName() + " to " + time);
        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();
            String sql = "update "
                + PENROSE_SOURCES_TABLE
                + " set expiration = ? where source = ?";
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.setTimestamp(1, time == null ? null : new Timestamp(time.getTime()));
            ps.setString(2, source.getSourceName());
            ps.execute();

		} finally {
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

}