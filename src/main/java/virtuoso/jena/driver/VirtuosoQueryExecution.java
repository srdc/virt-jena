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

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.shared.JenaException;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.ResultBinding;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.engine.binding.BindingMap;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.sparql.util.ModelUtils;
import org.apache.jena.util.FileManager;

import java.sql.ResultSetMetaData;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;

public class VirtuosoQueryExecution implements QueryExecution {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtuosoQueryExecution.class);

    private String virt_graph = null;
    private final VirtGraph graph;
    private final String virt_query;
    private QuerySolution m_arg = null;

    private java.sql.Statement stmt = null;

    public VirtuosoQueryExecution(String query, VirtGraph _graph) {
        graph = _graph;
        virt_graph = graph.getGraphName();
        virt_query = query;
    }

    @Override
    public ResultSet execSelect() {
        try {
            stmt = graph.createStatement();
            java.sql.ResultSet rs = stmt.executeQuery(getQueryString());
            return new VResultSet(graph, rs);
        } catch (Exception e) {
            throw new JenaException("Can not create ResultSet.:" + e);
        }
    }

    public void setFileManager(FileManager arg) {
        throw new JenaException("UnsupportedMethodException");
    }

    @Override
    public void setInitialBinding(QuerySolution arg) {
        m_arg = arg;
    }

    @Override
    public Dataset getDataset() {
        return new VirtDataSource(graph);
    }

    @Override
    public Context getContext() {
        return null;
    }

    @Override
    public Model execConstruct() {
        return execConstruct(ModelFactory.createDefaultModel());
    }

    @Override
    public Model execConstruct(Model model) {
        return execQuerySparql(model);
    }

    @Override
    public Model execDescribe() {
        return execDescribe(ModelFactory.createDefaultModel());
    }

    @Override
    public Model execDescribe(Model model) {
        return execQuerySparql(model);
    }

    @Override
    public boolean execAsk() {
        boolean ret = false;

        try {
            stmt = graph.createStatement();
            try (java.sql.ResultSet rs = stmt.executeQuery(getQueryString())) {
                ResultSetMetaData rsmd = rs.getMetaData();

                while (rs.next()) {
                    if (rs.getInt(1) == 1)
                        ret = true;
                }
            }
            stmt.close();
            stmt = null;

        } catch (Exception e) {
            throw new JenaException("Convert results are FAILED.:" + e);
        }
        return ret;
    }

    private Model execQuerySparql(Model model) {
        try {
            stmt = graph.createStatement();
            try (java.sql.ResultSet rs = stmt.executeQuery(getQueryString())) {
                ResultSetMetaData rsmd = rs.getMetaData();
                while (rs.next()) {
                    Node s = VirtUtilities.toNode(rs.getObject(1));
                    Node p = VirtUtilities.toNode(rs.getObject(2));
                    Node o = VirtUtilities.toNode(rs.getObject(3));

                    Statement st = ModelUtils
                            .tripleToStatement(model, new Triple(s, p, o));
                    if (st != null)
                        model.add(st);
                }
            }
            stmt.close();
            stmt = null;
        } catch (Exception e) {
            throw new JenaException("Convert results are FAILED.:" + e);
        }
        return model;
    }

    @Override
    public void abort() {
        if (stmt != null)
            try {
                stmt.cancel();
            } catch (Exception ignored) {
            }
    }

    @Override
    public void close() {
        if (stmt != null)
            try {
                stmt.cancel();
                stmt.close();
            } catch (Exception ignored) {
            }
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    private String substBindings(String query) {
        if (m_arg == null)
            return query;

        StringBuilder buf = new StringBuilder();
        String delim = " ,)(;.";
        int i = 0;
        char ch;
        int qlen = query.length();
        while (i < qlen) {
            ch = query.charAt(i++);
            if (ch == '\\') {
                buf.append(ch);
                if (i < qlen)
                    buf.append(query.charAt(i++));

            } else if (ch == '"' || ch == '\'') {
                char end = ch;
                buf.append(ch);
                while (i < qlen) {
                    ch = query.charAt(i++);
                    buf.append(ch);
                    if (ch == end)
                        break;
                }
            } else if (ch == '?') { // Parameter
                String varData = null;
                int j = i;
                while (j < qlen && delim.indexOf(query.charAt(j)) < 0)
                    j++;
                if (j != i) {
                    String varName = query.substring(i, j);
                    RDFNode val = m_arg.get(varName);
                    if (val != null) {
                        varData = VirtUtilities.toString(val.asNode());
                        i = j;
                    }
                }
                if (varData != null)
                    buf.append(varData);
                else
                    buf.append(ch);
            } else {
                buf.append(ch);
            }
        }
        return buf.toString();
    }

    private String getQueryString() {
        StringBuilder sb = new StringBuilder("sparql\n ");

        if (graph.getRuleSet() != null)
            sb.append(" define input:inference '").append(graph.getRuleSet()).append("'\n");

        if (graph.getSameAs())
            sb.append(" define input:same-as \"yes\"\n");

        if (!graph.getReadFromAllGraphs())
            sb.append(" define input:default-graph-uri <").append(graph.getGraphName()).append("> \n");

        sb.append(substBindings(virt_query));

        return sb.toString();
    }

    // /=== Inner class ===========================================
    public class VResultSet implements ResultSet {
        ResultSetMetaData rsmd;
        java.sql.ResultSet rs;
        boolean v_finished = false;
        boolean v_prefetched = false;
        VirtModel m;
        BindingMap v_row;
        List<String> resVars = new LinkedList<>();
        int row_id = 0;

        protected VResultSet(VirtGraph _g, java.sql.ResultSet _rs) {
            rs = _rs;
            m = new VirtModel(_g);

            try {
                rsmd = rs.getMetaData();
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    resVars.add(rsmd.getColumnLabel(i));

                if (virt_graph != null && !virt_graph.equals("virt:DEFAULT"))
                    resVars.add("graph");
            } catch (Exception e) {
                throw new JenaException(
                        "ViruosoResultBindingsToJenaResults is FAILED.:" + e);
            }
        }

        @Override
        public boolean hasNext() {
            if (!v_finished && !v_prefetched)
                moveForward();
            return !v_finished;
        }

        @Override
        public QuerySolution next() {
            Binding binding = nextBinding();

            if (v_finished)
                throw new NoSuchElementException();

            return new ResultBinding(m, binding);
        }

        @Override
        public QuerySolution nextSolution() {
            return next();
        }

        @Override
        public Binding nextBinding() {
            if (!v_finished && !v_prefetched)
                moveForward();

            v_prefetched = false;

            if (v_finished)
                throw new NoSuchElementException();

            return v_row;
        }

        @Override
        public int getRowNumber() {
            return row_id;
        }

        @Override
        public List<String> getResultVars() {
            return resVars;
        }

        @Override
        public Model getResourceModel() {
            return m;
        }

        @Override
        protected void finalize() throws Throwable {
            if (!v_finished)
                try {
                    super.finalize();
                    close();
                } catch (Exception ignored) {
                    logger.error(ignored.getMessage(), ignored);
                }
        }

        protected void moveForward() throws JenaException {
            try {
                if (!v_finished && rs.next()) {
                    extractRow();
                    v_prefetched = true;
                } else
                    close();
            } catch (Exception e) {
                throw new JenaException("Convert results are FAILED.:" + e);
            }
        }

        protected void extractRow() throws Exception {
            v_row = new BindingHashMap();
            row_id++;

            try {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    Node n = VirtUtilities.toNode(rs.getObject(i));
                    if (n != null)
                        v_row.add(Var.alloc(rsmd.getColumnLabel(i)), n);
                }

                if (virt_graph != null && !virt_graph.equals("virt:DEFAULT"))
                    v_row.add(Var.alloc("graph"), NodeFactory.createURI(virt_graph));
            } catch (Exception e) {
                throw new JenaException(
                        "ViruosoResultBindingsToJenaResults is FAILED.:" + e);
            }
        }

        @Override
        public void remove() throws java.lang.UnsupportedOperationException {
            throw new UnsupportedOperationException(this.getClass().getName()
                    + ".remove");
        }

        private void close() {
            if (!v_finished) {
                if (rs != null) {
                    try {
                        rs.close();
                        rs = null;
                    } catch (Exception ignored) {
                    }
                }
            }
            v_finished = true;
        }

    }

    @Override
    public Iterator<Triple> execConstructTriples() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Iterator<Quad> execConstructQuads() {
        return null;
    }

    @Override
    public Dataset execConstructDataset() {
        return null;
    }

    @Override
    public Dataset execConstructDataset(Dataset dataset) {
        return null;
    }

    @Override
    public Iterator<Triple> execDescribeTriples() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Query getQuery() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setTimeout(long arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimeout(long arg0, TimeUnit arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimeout(long arg0, long arg1) {
        // TODO Auto-generated method stub

    }

    @Override
    public void setTimeout(long arg0, TimeUnit arg1, long arg2, TimeUnit arg3) {
        // TODO Auto-generated method stub

    }

    @Override
    public long getTimeout1() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public long getTimeout2() {
        // TODO Auto-generated method stub
        return 0;
    }

}
