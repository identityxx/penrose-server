/**
 * Copyright (c) 1998-2005, Verge Lab., LLC.
 * All rights reserved.
 */
package org.safehaus.penrose.cache.impl;


import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Timestamp;

import org.apache.log4j.Logger;
import org.safehaus.penrose.mapping.EntryDefinition;
import org.safehaus.penrose.Penrose;

import javax.sql.DataSource;

/**
 * @author Endi S. Dewata
 */
public class PenroseResultHome {

	protected Logger log = Logger.getLogger(Penrose.CACHE_LOGGER);

	public final static String PENROSE_RESULTS_TABLE = "penrose_results";

	public int debug = 0;
    private DataSource ds;

	public PenroseResultHome(DataSource ds) throws Exception {

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

            String sql = "create table " + PENROSE_RESULTS_TABLE + " ("
                    + "dn varchar(200), "
                    + "modifyTime datetime, "
                    + "primary key (dn))";
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

            String sql = "drop table " + PENROSE_RESULTS_TABLE;
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

	public Date getModifyTime(EntryDefinition entry) throws Exception {

		String dn = entry.getDn();

        Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
            con = ds.getConnection();

			String sql = "select modifyTime from " + PENROSE_RESULTS_TABLE
					+ " where dn = ?";
			log.debug("Executing " + sql);

			ps = con.prepareStatement(sql);
			ps.setString(1, dn);
			rs = ps.executeQuery();

			if (!rs.next()) {
				log.warn("Cache for " + dn + " does not exists");
				return null;
			}

			Date modifyTime = rs.getTimestamp(1) == null ? null : new Date(rs.getTimestamp(1).getTime());
			log.debug("Cache for " + dn + " was modified on " + modifyTime);

			return modifyTime;

		} finally {
			if (rs != null) try { rs.close(); } catch (Exception e) {}
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

    public void insert(EntryDefinition entry) throws Exception {

        String dn = entry.getDn();

        Connection con = null;
        PreparedStatement ps = null;

        try {
            con = ds.getConnection();

            String sql = "insert into " + PENROSE_RESULTS_TABLE
                    + " values (?, null)";
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.setString(1, dn);
            ps.execute();

        } finally {
            if (ps != null) try { ps.close(); } catch (Exception ex) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
        }
    }

	public void setModifyTime(EntryDefinition entry, Date modifyTime)
			throws Exception {

		String dn = entry.getDn();
		log.debug("Invalidate cache for " + dn);

        Connection con = null;
		PreparedStatement ps = null;

		try {
            con = ds.getConnection();

            String sql = "update " + PENROSE_RESULTS_TABLE
            + " set modifyTime = ? where dn = ?";
            log.debug("Executing " + sql);

            ps = con.prepareStatement(sql);
            ps.setTimestamp(1, new Timestamp(modifyTime.getTime()));
            ps.setString(2, dn);
            ps.execute();

		} finally {
			if (ps != null) try { ps.close(); } catch (Exception ex) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}
	}

	/**
	 * Show all the dn in the result
	 * 
	 * @return Collection of dn (String)
	 * @throws Exception
	 */
	public Collection showAll() throws Exception {

        Connection con = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Collection results = new HashSet();

		try {
            con = ds.getConnection();

			String sql = "select dn, modifyTime from " + PENROSE_RESULTS_TABLE;
			log.debug("Executing " + sql);

			ps = con.prepareStatement(sql);
			rs = ps.executeQuery();

			log.debug("RESULTS --------------------------------------------------");
			while (rs.next()) {
				String dn = rs.getString(1);
				Date modifyTime = rs.getTimestamp(2) == null ? null : new Date(
						rs.getTimestamp(2).getTime());
				log.debug("| " + dn + " | " + modifyTime + " |");
				results.add(dn);
			}
			log.debug("RESULTS --------------------------------------------------");

		} finally {
			if (rs != null) try { rs.close(); } catch (Exception e) {}
			if (ps != null) try { ps.close(); } catch (Exception e) {}
            if (con != null) try { con.close(); } catch (Exception ex) {}
		}

		return results;
	}

}