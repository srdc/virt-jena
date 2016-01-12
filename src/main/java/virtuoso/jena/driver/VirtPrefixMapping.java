/*
 *  $Id$
 *
 *  This file is part of the OpenLink Software Virtuoso Open-Source (VOS)
 *  project.
 *
 *  Copyright (C) 1998-2012 OpenLink Software
 *
 *  This project is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the
 *  Free Software Foundation; only version 2 of the License, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *  General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with this program; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

package virtuoso.jena.driver;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.jena.shared.JenaException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;

public class VirtPrefixMapping extends PrefixMappingImpl {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtPrefixMapping.class);

    protected VirtGraph m_graph = null;

    /**
     * Constructor for a persistent prefix mapping.
     *
     * @param graph the {@link VirtGraph}
     */
    public VirtPrefixMapping(VirtGraph graph) {
        super();
        m_graph = graph;

        // Populate the prefix map using data from the
        // persistent graph properties
        String query = "DB.DBA.XML_SELECT_ALL_NS_DECLS (3)";
        try {
            try (Statement stmt = m_graph.createStatement(); ResultSet rs = stmt.executeQuery(query)) {

                while (rs.next()) {
                    String prefix = rs.getString(1);
                    String uri = rs.getString(2);
                    if (uri != null)
                        super.setNsPrefix(prefix, uri);
                }
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    @Override
    public PrefixMapping removeNsPrefix(String prefix) {
        String query = "DB.DBA.XML_REMOVE_NS_BY_PREFIX(?, 1)";
        super.removeNsPrefix(prefix);

        try {
            try (PreparedStatement ps = m_graph.createPreparedStatement(query)) {
                ps.setString(1, prefix);
                ps.execute();
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }

        return this;
    }

    /*
     * (non-Javadoc) Override the default implementation so we can catch the
     * write operation and update the persistent store.
     *
     * @see com.hp.hpl.jena.shared.PrefixMapping#setNsPrefix(java.lang.String,
     * java.lang.String)
     */
    @Override
    public PrefixMapping setNsPrefix(String prefix, String uri) {
        super.setNsPrefix(prefix, uri);

        String query = "DB.DBA.XML_SET_NS_DECL(?, ?, 1)";

        // All went well, so persist the prefix by adding it to the graph
        // properties
        // (the addPrefix call will overwrite any existing mapping with the same
        // prefix
        // so it matches the behaviour of the prefixMappingImpl).
        try {
            try (PreparedStatement ps = m_graph.createPreparedStatement(query)) {
                ps.setString(1, prefix);
                ps.setString(2, uri);
                ps.execute();
            }
        } catch (Exception e) {
            throw new JenaException(e.toString());
        }
        return this;
    }

    @Override
    public PrefixMapping setNsPrefixes(PrefixMapping other) {
        return setNsPrefixes(other.getNsPrefixMap());
    }

    @Override
    public PrefixMapping setNsPrefixes(Map other) {
        checkUnlocked();
        for (Object o : other.entrySet()) {
            Map.Entry e = (Map.Entry) o;
            setNsPrefix((String) e.getKey(), (String) e.getValue());
        }
        return this;
    }
}
