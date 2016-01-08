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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.LabelExistsException;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.shared.JenaException;
import org.apache.jena.shared.Lock;
import org.apache.jena.shared.LockNone;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.util.Context;
import virtuoso.jdbc4.VirtuosoDataSource;

public class VirtDataSource extends VirtGraph implements Dataset {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtDataSource.class);

    /**
     * Default model - may be null - according to Javadoc
     */
    Model defaultModel = null;
    Lock lock = null;

    public VirtDataSource() {
        super();
    }

    public VirtDataSource(String _graphName, VirtuosoDataSource _ds) {
        super(_graphName, _ds);
    }

    protected VirtDataSource(VirtGraph g) {
        this.graphName = g.getGraphName();
        setReadFromAllGraphs(g.getReadFromAllGraphs());
        this.url_hostlist = g.getGraphUrl();
        this.user = g.getGraphUser();
        this.password = g.getGraphPassword();
        this.roundrobin = g.roundrobin;
        setFetchSize(g.getFetchSize());
        this.connection = g.getConnection();
    }

    public VirtDataSource(String url_hostlist, String user, String password) {
        super(url_hostlist, user, password);
    }

    /**
     * Set a named graph.
     *
     * @param name  the {@link String}
     * @param model the {@link Model}
     */
    @Override
    public void addNamedModel(String name, Model model)
            throws LabelExistsException {
        String query = "select count(*) from (sparql select * where { graph `iri(??)` { ?s ?p ?o }})f";
        ResultSet rs;
        int ret = 0;

        checkOpen();
        try {
            try (java.sql.PreparedStatement ps = createPreparedStatement(query)) {
                ps.setString(1, name);
                rs = ps.executeQuery();
                if (rs.next())
                    ret = rs.getInt(1);
                rs.close();
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }

        try {
            if (ret != 0)
                throw new LabelExistsException("A model with ID '" + name
                        + "' already exists.");
            Graph g = model.getGraph();
            int count = 0;
            try (java.sql.PreparedStatement ps = createPreparedStatement(sinsert)) {
                for (Iterator i = g.find(Node.ANY, Node.ANY, Node.ANY); i.hasNext(); ) {
                    Triple t = (Triple) i.next();

                    ps.setString(1, name);
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
        } catch (LabelExistsException | SQLException e) {
            throw new JenaException(e);
        }
    }

    /**
     * Remove a named graph.
     *
     * @param name the {@link String}
     */
    @Override
    public void removeNamedModel(String name) {
        String exec_text = "sparql clear graph <" + name + ">";

        checkOpen();
        try {
            try (java.sql.Statement stmt = createStatement()) {
                stmt.executeQuery(exec_text);
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    /**
     * Change a named graph for another uisng the same name
     *
     * @param name  the {@link String}
     * @param model the {@link Model}
     */
    @Override
    public void replaceNamedModel(String name, Model model) {
        try {
            getConnection().setAutoCommit(false);
            removeNamedModel(name);
            addNamedModel(name, model);
            getConnection().commit();
            getConnection().setAutoCommit(true);
        } catch (SQLException | LabelExistsException e) {
            try {
                getConnection().rollback();
            } catch (Exception e2) {
                throw new JenaException(
                        "Could not replace model, and could not rollback!", e2);
            }
            throw new JenaException("Could not replace model:", e);
        }
    }

    /**
     * Get the default graph as a Jena Model
     *
     * @return the {òlink Model}
     */
    @Override
    public Model getDefaultModel() {
        return defaultModel;
    }

    /**
     * Set the background graph. Can be set to null for none.
     *
     * @param model the {@link Model}
     */
    @Override
    public void setDefaultModel(Model model) {
        if (!(model instanceof VirtDataSource))
            throw new IllegalArgumentException(
                    "VirtDataSource supports only VirtModel as default model");
        defaultModel = model;
    }

    /**
     * Get a graph by name as a Jena Model
     *
     * @param name the {@link String}
     * @return the {@link Model}
     */
    @Override
    public Model getNamedModel(String name) {
        try {
            VirtuosoDataSource _ds = getDataSource();
            if (_ds != null)
                return new VirtModel(new VirtGraph(name, _ds));
            else
                return new VirtModel(new VirtGraph(name, this.getGraphUrl(),
                        this.getGraphUser(), this.getGraphPassword()));
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    /**
     * Does the dataset contain a model with the name supplied?
     *
     * @param name the {@link String}
     * @return the {@link Model}
     */
    @Override
    public boolean containsNamedModel(String name) {
        String query = "select count(*) from (sparql select * where { graph `iri(??)` { ?s ?p ?o }})f";
        ResultSet rs;
        int ret = 0;

        checkOpen();
        try {
            try (java.sql.PreparedStatement ps = createPreparedStatement(query)) {
                ps.setString(1, name);
                rs = ps.executeQuery();
                if (rs.next())
                    ret = rs.getInt(1);
                rs.close();
            }
        } catch (Exception e) {
            throw new JenaException(e);
        }
        return (ret != 0);
    }

    /**
     * List the names
     *
     * @return {@link Iterator}
     */
    @Override
    public Iterator<String> listNames() {
        String exec_text = "DB.DBA.SPARQL_SELECT_KNOWN_GRAPHS()";
        ResultSet rs;
        int ret = 0;

        checkOpen();
        try {
            List<String> names = new LinkedList<>();

            try (java.sql.Statement stmt = createStatement()) {
                rs = stmt.executeQuery(exec_text);
                while (rs.next())
                    names.add(rs.getString(1));
                rs.close();
            }
            return names.iterator();
        } catch (Exception e) {
            throw new JenaException(e);
        }
    }

    /**
     * Get the lock for this dataset
     *
     * @return {@link Lock}
     */
    @Override
    public Lock getLock() {
        if (lock == null)
            lock = new LockNone();
        return lock;
    }

    /**
     * Get the dataset in graph form
     *
     * @return {@link DatasetGraph}
     */
    @Override
    public DatasetGraph asDatasetGraph() {
        return new VirtDataSetGraph(this);
    }

    @Override
    public void abort() {
        // TODO Auto-generated method stub

    }

    @Override
    public void begin(ReadWrite arg0) {
        // TODO Auto-generated method stub

    }

    @Override
    public void commit() {
        // TODO Auto-generated method stub

    }

    @Override
    public void end() {
        // TODO Auto-generated method stub

    }

    @Override
    public Context getContext() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean isInTransaction() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean supportsTransactions() {
        // TODO Auto-generated method stub
        return false;
    }

    public class VirtDataSetGraph implements DatasetGraph {

        VirtDataSource vd = null;

        public VirtDataSetGraph(VirtDataSource vds) {
            vd = vds;
        }

        @Override
        public Graph getDefaultGraph() {
            return vd;
        }

        @Override
        public void setDefaultGraph(Graph arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public Graph getGraph(Node graphNode) {
            try {
                return new VirtGraph(graphNode.toString(), vd.getGraphUrl(),
                        vd.getGraphUser(), vd.getGraphPassword());
            } catch (Exception e) {
                throw new JenaException(e);
            }
        }

        @Override
        public boolean containsGraph(Node graphNode) {
            return containsNamedModel(graphNode.toString());
        }

        @Override
        public Iterator<Node> listGraphNodes() {
            String exec_text = "DB.DBA.SPARQL_SELECT_KNOWN_GRAPHS()";
            ResultSet rs;
            int ret = 0;

            vd.checkOpen();
            try {
                List<Node> names = new LinkedList<>();

                try (java.sql.Statement stmt = vd.createStatement()) {
                    rs = stmt.executeQuery(exec_text);
                    while (rs.next())
                        names.add(NodeFactory.createURI(rs.getString(1)));
                    rs.close();
                }
                return names.iterator();
            } catch (Exception e) {
                throw new JenaException(e);
            }
        }

        @Override
        public Lock getLock() {
            return vd.getLock();
        }

        @Override
        public long size() {
            return vd.size();
        }

        @Override
        public void close() {
            vd.close();
        }

        @Override
        public void add(Quad arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void add(Node arg0, Node arg1, Node arg2, Node arg3) {
            // TODO Auto-generated method stub

        }

        @Override
        public void addGraph(Node arg0, Graph arg1) {
            // TODO Auto-generated method stub

        }

        @Override
        public boolean contains(Quad arg0) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void clear() {

        }

        @Override
        public boolean contains(Node arg0, Node arg1, Node arg2, Node arg3) {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void delete(Quad arg0) {
            // TODO Auto-generated method stub

        }

        @Override
        public void delete(Node arg0, Node arg1, Node arg2, Node arg3) {
            // TODO Auto-generated method stub

        }

        @Override
        public void deleteAny(Node arg0, Node arg1, Node arg2, Node arg3) {
            // TODO Auto-generated method stub

        }

        @Override
        public Iterator<Quad> find() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Iterator<Quad> find(Quad arg0) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Iterator<Quad> find(Node arg0, Node arg1, Node arg2, Node arg3) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Iterator<Quad> findNG(Node arg0, Node arg1, Node arg2, Node arg3) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Context getContext() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public boolean isEmpty() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void removeGraph(Node arg0) {
            // TODO Auto-generated method stub

        }

    }

}
