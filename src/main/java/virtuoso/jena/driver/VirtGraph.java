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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.*;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.shared.AddDeniedException;
import org.apache.jena.shared.DeleteDeniedException;
import org.apache.jena.shared.JenaException;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import virtuoso.jdbc4.VirtuosoConnectionPoolDataSource;
import virtuoso.jdbc4.VirtuosoDataSource;

public class VirtGraph extends GraphBase {

    static public final String DEFAULT = "virt:DEFAULT";
    static final String sinsert = "sparql insert into graph iri(??) { `iri(??)` `iri(??)` `bif:__rdf_long_from_batch_params(??,??,??)` }";
    static final String sdelete = "sparql delete from graph iri(??) {`iri(??)` `iri(??)` `bif:__rdf_long_from_batch_params(??,??,??)`}";
    static final int BATCH_SIZE = 5000;
    static final String utf8 = "charset=utf-8";
    static final String charset = "UTF-8";
    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtGraph.class);

    static {
        VirtuosoQueryEngine.register();
    }

    protected String graphName;
    protected boolean readFromAllGraphs = false;
    protected String url_hostlist;
    protected String user;
    protected String password;
    protected boolean roundrobin = false;
    protected int prefetchSize = 200;
    protected Connection connection = null;
    protected String ruleSet = null;
    protected boolean useSameAs = false;
    protected int queryTimeout = 0;
    protected VirtPrefixMapping m_prefixMapping = null;
    private VirtuosoConnectionPoolDataSource pds = new VirtuosoConnectionPoolDataSource();
    private VirtuosoDataSource ds;
    private boolean isDSconnection = false;

    public VirtGraph() {
        this(null, "jdbc:virtuoso://localhost:1111/charset=UTF-8", null, null,
                false);
    }

    public VirtGraph(String graphName) {
        this(graphName, "jdbc:virtuoso://localhost:1111/charset=UTF-8", null,
                null, false);
    }

    public VirtGraph(String graphName, String _url_hostlist, String user,
                     String password) {
        this(graphName, _url_hostlist, user, password, false);
    }

    public VirtGraph(String url_hostlist, String user, String password) {
        this(null, url_hostlist, user, password, false);
    }

    public VirtGraph(String _graphName, VirtuosoDataSource _ds) {
        super();

        this.url_hostlist = _ds.getServerName();
        this.graphName = _graphName;
        this.user = _ds.getUser();
        this.password = _ds.getPassword();

        if (this.graphName == null)
            this.graphName = DEFAULT;

        try {
            connection = _ds.getConnection();
            isDSconnection = true;
            ds = _ds;
            // don't drop is it needed for initialize internal Jena classes
            ModelCom m = new ModelCom(this);

            TypeMapper tm = TypeMapper.getInstance();
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    public VirtGraph(VirtuosoDataSource _ds) {
        this(null, _ds);
    }

    public VirtGraph(String graphName, String _url_hostlist, String user,
                     String password, boolean _roundrobin) {
        super();

        this.url_hostlist = _url_hostlist.trim();
        this.roundrobin = _roundrobin;
        this.graphName = graphName;
        this.user = user;
        this.password = password;

        if (this.graphName == null)
            this.graphName = DEFAULT;

        try {
            if (url_hostlist.startsWith("jdbc:virtuoso://")) {

                String url = url_hostlist;
                if (!url.toLowerCase().contains(utf8)) {
                    if (url.charAt(url.length() - 1) != '/')
                        url = url + "/charset=UTF-8";
                    else
                        url = url + "charset=UTF-8";
                }
                if (roundrobin
                        && !url.toLowerCase().contains("roundrobin=")) {
                    if (url.charAt(url.length() - 1) != '/')
                        url = url + "/roundrobin=1";
                    else
                        url = url + "roundrobin=1";
                }
                Class.forName("virtuoso.jdbc4.Driver");
                connection = DriverManager.getConnection(url, user, password);
            } else {
                pds.setServerName(url_hostlist);
                pds.setUser(user);
                pds.setPassword(password);
                pds.setCharset(charset);
                pds.setRoundrobin(roundrobin);
                javax.sql.PooledConnection pconn = pds.getPooledConnection();
                connection = pconn.getConnection();
                isDSconnection = true;
            }

            ModelCom m = new ModelCom(this); // don't drop is it needed for
            // initialize internal Jena
            // classes
            TypeMapper tm = TypeMapper.getInstance();
        } catch (ClassNotFoundException | SQLException e) {
            throw new JenaException(e);
        }

    }

    private static String escapeString(String s) {
        StringBuilder buf = new StringBuilder(s.length());
        int i = 0;
        char ch;
        while (i < s.length()) {
            ch = s.charAt(i++);
            if (ch == '\'')
                buf.append('\\');
            buf.append(ch);
        }
        return buf.toString();
    }

    // GraphBase overrides
    public static String Node2Str(Node n) {
        if (n.isURI()) {
            return "<" + n + ">";
        } else if (n.isBlank()) {
            return "<_:" + n + ">";
        } else if (n.isLiteral()) {
            String s;
            StringBuilder sb = new StringBuilder();
            sb.append("'");
            sb.append(escapeString(n.getLiteralValue().toString()));
            sb.append("'");

            s = n.getLiteralLanguage();
            if (s != null && s.length() > 0) {
                sb.append("@");
                sb.append(s);
            }
            s = n.getLiteralDatatypeURI();
            if (s != null && s.length() > 0) {
                sb.append("^^<");
                sb.append(s);
                sb.append(">");
            }
            return sb.toString();
        } else {
            return "<" + n + ">";
        }
    }

    // getters
    public VirtuosoDataSource getDataSource() {
        if (isDSconnection)
            return (ds != null ? ds : pds);
        else
            return null;
    }

    public String getGraphName() {
        return this.graphName;
    }

    public String getGraphUrl() {
        return this.url_hostlist;
    }

    public String getGraphUser() {
        return this.user;
    }

    public String getGraphPassword() {
        return this.password;
    }

    public Connection getConnection() {
        return this.connection;
    }

    public int getFetchSize() {
        return this.prefetchSize;
    }

    public void setFetchSize(int sz) {
        this.prefetchSize = sz;
    }

    public int getQueryTimeout() {
        return this.queryTimeout;
    }

    public void setQueryTimeout(int seconds) {
        this.queryTimeout = seconds;
    }

    public int getCount() {
        return size();
    }

    public void remove(List<Triple> triples) {
        delete(triples.iterator(), null);
    }

    public void remove(Triple t) {
        delete(t);
    }

    public boolean getReadFromAllGraphs() {
        return readFromAllGraphs;
    }

    public void setReadFromAllGraphs(boolean val) {
        readFromAllGraphs = val;
    }

    public String getRuleSet() {
        return ruleSet;
    }

    public void setRuleSet(String _ruleSet) {
        ruleSet = _ruleSet;
    }

    public boolean getSameAs() {
        return useSameAs;
    }

    public void setSameAs(boolean _sameAs) {
        useSameAs = _sameAs;
    }

    public void createRuleSet(String ruleSetName, String uriGraphRuleSet) {
        checkOpen();

        try {
            try (java.sql.Statement st = createStatement()) {
                st.execute("rdfs_rule_set('" + ruleSetName + "', '"
                        + uriGraphRuleSet + "')");
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    public void removeRuleSet(String ruleSetName, String uriGraphRuleSet) {
        checkOpen();

        try {
            try (java.sql.Statement st = createStatement()) {
                st.execute("rdfs_rule_set('" + ruleSetName + "', '"
                        + uriGraphRuleSet + "', 1)");
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    protected java.sql.Statement createStatement() throws java.sql.SQLException {
        checkOpen();
        java.sql.Statement st = connection.createStatement();
        if (queryTimeout > 0)
            st.setQueryTimeout(queryTimeout);
        st.setFetchSize(prefetchSize);
        return st;
    }

    protected java.sql.PreparedStatement createPreparedStatement(String sql)
            throws java.sql.SQLException {
        checkOpen();
        java.sql.PreparedStatement st = connection.prepareStatement(sql);
        if (queryTimeout > 0)
            st.setQueryTimeout(queryTimeout);
        st.setFetchSize(prefetchSize);
        return st;
    }

    protected void executePrepareStatementOnGraph(
            java.sql.PreparedStatement ps, String graphName, Triple t) throws SQLException {
        ps.setString(1, graphName);
        bindSubject(ps, 2, t.getSubject());
        bindPredicate(ps, 3, t.getPredicate());
        bindObject(ps, 4, t.getObject());
        ps.execute();
        ps.close();
    }

    void bindSubject(PreparedStatement ps, int col, Node n) throws SQLException {
        if (n == null) return;
        if (n.isURI()) ps.setString(col, n.toString());
        else if (n.isBlank()) ps.setString(col, "_:" + n.toString());
        else
            throw new SQLException(
                    "Only URI or Blank nodes can be used as subject");
    }

    void bindPredicate(PreparedStatement ps, int col, Node n)
            throws SQLException {
        if (n == null) return;
        if (n.isURI()) ps.setString(col, n.toString());
        else
            throw new SQLException("Only URI nodes can be used as predicate");
    }

    void bindObject(PreparedStatement ps, int col, Node n) throws SQLException {
        if (n == null)
            return;
        if (n.isURI()) {
            ps.setInt(col, 1);
            ps.setString(col + 1, n.toString());
            ps.setNull(col + 2, java.sql.Types.VARCHAR);
        } else if (n.isBlank()) {
            ps.setInt(col, 1);
            ps.setString(col + 1, "_:" + n.toString());
            ps.setNull(col + 2, java.sql.Types.VARCHAR);
        } else if (n.isLiteral()) {
            String llang = n.getLiteralLanguage();
            String ltype = n.getLiteralDatatypeURI();
            if (llang != null && llang.length() > 0) {
                ps.setInt(col, 5);
                ps.setString(col + 1, n.getLiteralValue().toString());
                ps.setString(col + 2, n.getLiteralLanguage());
            } else if (ltype != null && ltype.length() > 0) {
                ps.setInt(col, 4);
                ps.setString(col + 1, n.getLiteralValue().toString());
                ps.setString(col + 2, n.getLiteralDatatypeURI());
            } else {
                ps.setInt(col, 3);
                ps.setString(col + 1, n.getLiteralValue().toString());
                ps.setNull(col + 2, java.sql.Types.VARCHAR);
            }
        } else {
            ps.setInt(col, 3);
            ps.setString(col + 1, n.toString());
            ps.setNull(col + 2, java.sql.Types.VARCHAR);
        }
    }

    // --java5 or newer @Override
    @Override
    public void performAdd(Triple t) {
        java.sql.PreparedStatement ps;
        try {
            ps = createPreparedStatement(sinsert);
            executePrepareStatementOnGraph(ps, this.graphName, t);

        } catch (Exception e) {
            throw new AddDeniedException(e.toString());
        }
    }

    @Override
    public void performDelete(Triple t) {
        java.sql.PreparedStatement ps;

        try {
            ps = createPreparedStatement(sdelete);
            executePrepareStatementOnGraph(ps, this.graphName, t);
        } catch (Exception e) {
            throw new DeleteDeniedException(e.toString());
        }
    }

    /**
     * more efficient
     *
     * @return {@link Integer}
     */
    // --java5 or newer @Override
    @Override
    protected int graphBaseSize() {
        StringBuilder sb = new StringBuilder(
                "select count(*) from (sparql define input:storage \"\" ");

        if (ruleSet != null)
            sb.append(" define input:inference '").append(ruleSet).append("'\n ");

        if (useSameAs)
            sb.append(" define input:same-as \"yes\"\n ");

        if (readFromAllGraphs)
            sb.append(" select * where {?s ?p ?o })f");
        else
            sb.append(" select * where { graph `iri(??)` { ?s ?p ?o }})f");

        ResultSet rs;
        int ret = 0;

        checkOpen();

        try {
            try (java.sql.PreparedStatement ps = createPreparedStatement(sb.toString())) {
                if (!readFromAllGraphs)
                    ps.setString(1, graphName);

                rs = ps.executeQuery();
                if (rs.next())
                    ret = rs.getInt(1);
                rs.close();
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
        return ret;
    }

    /**
     * maybe more efficient than default impl
     *
     * @param t {@link Triple}
     * @return {@link Boolean}
     */
    // --java5 or newer @Override
    @Override
    protected boolean graphBaseContains(Triple t) {
        ResultSet rs;
        String S, P, O;
        StringBuilder sb = new StringBuilder("sparql define input:storage \"\" ");

        checkOpen();

        S = " ?s ";
        P = " ?p ";
        O = " ?o ";

        if (!Node.ANY.equals(t.getSubject()))
            S = Node2Str(t.getSubject());

        if (!Node.ANY.equals(t.getPredicate()))
            P = Node2Str(t.getPredicate());

        if (!Node.ANY.equals(t.getObject()))
            O = Node2Str(t.getObject());

        if (ruleSet != null)
            sb.append(" define input:inference '").append(ruleSet).append("'\n ");

        if (useSameAs)
            sb.append(" define input:same-as \"yes\"\n ");

        if (readFromAllGraphs)
            sb.append(" select * where { ")
                    .append(S).append(" ")
                    .append(P).append(" ")
                    .append(O)
                    .append(" } limit 1");
        else
            sb.append(" select * where { graph <").append(graphName).append("> { ")
                    .append(S).append(" ")
                    .append(P).append(" ")
                    .append(O)
                    .append(" }} limit 1");

        try {
            boolean ret;
            try (java.sql.Statement stmt = createStatement()) {
                rs = stmt.executeQuery(sb.toString());
                ret = rs.next();
                rs.close();
            }
            return ret;
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    // --java5 or newer @Override
    @Override
    public ExtendedIterator<Triple> graphBaseFind(Triple tm) {
        String S, P, O;
        StringBuilder sb = new StringBuilder("sparql ");

        checkOpen();

        S = " ?s ";
        P = " ?p ";
        O = " ?o ";

        if (tm.getMatchSubject() != null)
            S = Node2Str(tm.getMatchSubject());

        if (tm.getMatchPredicate() != null)
            P = Node2Str(tm.getMatchPredicate());

        if (tm.getMatchObject() != null)
            O = Node2Str(tm.getMatchObject());

        if (ruleSet != null)
            sb.append(" define input:inference '").append(ruleSet).append("'\n ");

        if (useSameAs)
            sb.append(" define input:same-as \"yes\"\n ");

        if (readFromAllGraphs)
            sb.append(" select * where { ")
                    .append(S).append(" ")
                    .append(P).append(" ")
                    .append(O)
                    .append(" }");
        else
            sb.append(" select * from <").append(graphName).append("> where { ")
                    .append(S).append(" ")
                    .append(P).append(" ")
                    .append(O)
                    .append(" }");

        try {
            java.sql.PreparedStatement stmt;
            stmt = createPreparedStatement(sb.toString());
            return new VirtResSetIter(this, stmt.executeQuery(), tm, stmt);
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    // Extra functions

    // --java5 or newer @Override
    @Override
    public void close() {
        try {
            super.close(); // will set closed = true
            connection.close();
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    @Override
    public void clear() {
        clearGraph(this.graphName);
        getEventManager().notifyEvent(this, GraphEvents.removeAll);
    }

    public void read(String url, String type) {
        String exec_text;

        exec_text = "sparql load \"" + url + "\" into graph <" + graphName + ">";

        checkOpen();
        try {
            try (java.sql.Statement stmt = createStatement()) {
                stmt.execute(exec_text);
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    // --java5 or newer @SuppressWarnings("unchecked")
    void add(Iterator<Triple> it, List<Triple> list) {
        try {
            try (PreparedStatement ps = createPreparedStatement(sinsert)) {
                int count = 0;

                while (it.hasNext()) {
                    Triple t = it.next();

                    if (list != null)
                        list.add(t);

                    ps.setString(1, this.graphName);
                    bindSubject(ps, 2, t.getSubject());
                    bindPredicate(ps, 3, t.getPredicate());
                    bindObject(ps, 4, t.getObject());
                    ps.addBatch();
                    count++;

                    if (count > BATCH_SIZE) {
                        ps.executeBatch();
                        ps.clearBatch();
                        count = 0;
                    }
                }

                if (count > 0) {
                    ps.executeBatch();
                    ps.clearBatch();
                }
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    void delete(Iterator<Triple> it, List<Triple> list) {
        try {
            while (it.hasNext()) {
                Triple triple = it.next();

                if (list != null)
                    list.add(triple);

                performDelete(triple);
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    void delete_match(Triple tm) {
        String S, P, O;
        Node nS, nP, nO;

        checkOpen();

        S = "?s";
        P = "?p";
        O = "?o";

        nS = tm.getMatchSubject();
        nP = tm.getMatchPredicate();
        nO = tm.getMatchObject();

        try {
            if (nS == null && nP == null && nO == null) {

                clearGraph(this.graphName);

            } else if (nS != null && nP != null && nO != null) {
                java.sql.PreparedStatement ps;

                ps = createPreparedStatement(sdelete);
                ps.setString(1, this.graphName);
                bindSubject(ps, 2, nS);
                bindPredicate(ps, 3, nP);
                bindObject(ps, 4, nO);

                ps.execute();
                ps.close();

            } else {

                if (nS != null)
                    S = Node2Str(nS);

                if (nP != null)
                    P = Node2Str(nP);

                if (nO != null)
                    O = Node2Str(nO);

                String query = "sparql delete from graph <" + this.graphName
                        + "> { " + S + " " + P + " " + O + " } from <"
                        + this.graphName + "> where { " + S + " " + P + " " + O
                        + " }";

                try (java.sql.Statement stmt = createStatement()) {
                    stmt.execute(query);
                }
            }
        } catch (Exception e) {
            throw new DeleteDeniedException(e.toString());
        }
    }

    void clearGraph(String name) {
        String query = "sparql clear graph iri(??)";
        checkOpen();
        try {
            try (java.sql.PreparedStatement ps = createPreparedStatement(query)) {
                ps.setString(1, name);
                ps.execute();
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    public ExtendedIterator reifierTriples(Triple m) {
        return NiceIterator.emptyIterator();
    }

    public int reifierSize() {
        return 0;
    }

    // --java5 or newer @Override
    /*public BulkUpdateHandler getBulkUpdateHandler() {
		if (bulkHandler == null)
			bulkHandler = new VirtBulkUpdateHandler(this);
		return bulkHandler;
	}*/

    // --java5 or newer @Override
    @Override
    public TransactionHandler getTransactionHandler() {
        return new VirtTransactionHandler(this);
    }

    @Override
    public PrefixMapping getPrefixMapping() {
        if (m_prefixMapping == null)
            m_prefixMapping = new VirtPrefixMapping(this);
        return m_prefixMapping;
    }

    // --java5 or newer @Override
    public void add(Triple[] triples) {
        addIterator(Arrays.asList(triples).iterator(), false);
        gem.notifyAddArray(this, triples);
    }

    // --java5 or newer @Override
    protected void add(List<Triple> triples, boolean notify) {
        addIterator(triples.iterator(), false);
        if (notify)
            gem.notifyAddList(this, triples);
    }

    // --java5 or newer @Override
    public void addIterator(Iterator<Triple> it, boolean notify) {
        VirtGraph _graph = this;
        List<Triple> list;
        if (notify) list = new ArrayList<>();
        else list = null;

        _graph = prepareGraphConnection(_graph, it, list);

        if (notify)
            gem.notifyAddIterator(_graph, list);
    }

    public VirtGraph prepareGraphConnection(VirtGraph _graph, Iterator<Triple> it, List<Triple> list) {
        try {
            boolean autoCommit = _graph.getConnection().getAutoCommit();
            if (autoCommit)
                _graph.getConnection().setAutoCommit(false);
            _graph.add(it, list);
            if (autoCommit) {
                _graph.getConnection().commit();
                _graph.getConnection().setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new JenaException("Couldn't create transaction:" + e);
        }
        return _graph;
    }

    public void delete(Triple[] triples) {
        deleteIterator(Arrays.asList(triples).iterator(), false);
        gem.notifyDeleteArray(this, triples);
    }

    protected void delete(List<Triple> triples, boolean notify) {
        deleteIterator(triples.iterator(), false);
        if (notify)
            gem.notifyDeleteList(this, triples);
    }

    public void deleteIterator(Iterator<Triple> it, boolean notify) {
        VirtGraph _graph = this;
        List<Triple> list;
        if (notify) list = new ArrayList<>();
        else list = null;

        prepareGraphConnection(_graph, it, list);

        if (notify)
            gem.notifyDeleteIterator(_graph, list);
    }

    public void removeAll() {
        VirtGraph _graph = this;
        _graph.clearGraph(_graph.getGraphName());
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        VirtGraph _graph = this;
        _graph.delete_match(Triple.createMatch(s, p, o));
        gem.notifyEvent(this, GraphEvents.remove(s, p, o));
    }

}
