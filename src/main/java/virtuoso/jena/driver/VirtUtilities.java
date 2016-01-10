package virtuoso.jena.driver;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.iri.IRI;
import org.apache.jena.iri.IRIFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.rdf.model.Literal;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.util.NodeUtils;

import java.util.Calendar;
import java.util.GregorianCalendar;

/**
 * Created by 4535992 on 08/01/2016.
 *
 * @see if you like these method you can found more on :
 */
public class VirtUtilities {

    private static final org.slf4j.Logger logger =
            org.slf4j.LoggerFactory.getLogger(VirtUtilities.class);

    /**
     * Method to convert a String uri to a jena Graph Node.
     * old name : createNode.
     * href: http://willware.blogspot.it/2010/02/jena-node-versus-rdfnode.html
     *
     * @param resource any element on API Jena can be converted to a Node.
     * @return the Jena Graph Node.
     */
    private static Node createNodeBase(Object resource, String lang, RDFDatatype rdfDatatype, Boolean xml){
        try {
            if (resource == null) {
                logger.warn("Try to create a Node from a 'NULL' value");
                return null;
            } else if (resource instanceof Node) {
                return (Node) resource;
            } else if (lang != null && rdfDatatype != null) {
                return NodeFactory.createLiteral(String.valueOf(resource), lang, rdfDatatype);
            } else if (rdfDatatype != null) {
                return NodeFactory.createLiteral(String.valueOf(resource), rdfDatatype);
            } else if (lang != null && xml != null) {
                return NodeFactory.createLiteral(String.valueOf(resource), lang, xml);
            } else if (lang != null) {
                return NodeFactory.createLiteral(String.valueOf(resource), lang);
            } else {
                if (resource instanceof Literal) {
                    return ((Literal) resource).asNode();
                } else if (resource instanceof Resource) {
                    return ((Resource) resource).asNode();
                } else if (resource instanceof RDFNode) {
                    return ((RDFNode) resource).asNode();
                } else if (resource instanceof LiteralLabel) {
                    return NodeFactory.createLiteral((LiteralLabel) resource);
                } else if (resource instanceof virtuoso.sql.ExtendedString) {
                    virtuoso.sql.ExtendedString vs = (virtuoso.sql.ExtendedString) resource;
                    if (vs.getIriType() == virtuoso.sql.ExtendedString.IRI
                            && (vs.getStrType() & 0x01) == 0x01) {
                        if (vs.toString().indexOf("_:") == 0)
                            return NodeFactory.createBlankNode(BlankNodeId.create(String.valueOf(vs)
                                    .substring(2))); // _:
                        else
                            return NodeFactory.createURI(String.valueOf(vs));

                    } else if (vs.getIriType() == virtuoso.sql.ExtendedString.BNODE) {
                        return NodeFactory.createBlankNode(BlankNodeId.create(String.valueOf(vs).substring(9))); // nodeID://

                    } else {
                        return NodeFactory.createLiteral(String.valueOf(vs));
                    }
                } else if (resource instanceof virtuoso.sql.RdfBox) {
                    virtuoso.sql.RdfBox rb = (virtuoso.sql.RdfBox) resource;
                    String rb_type = rb.getType();
                    if (rb_type != null) {
                        return NodeFactory.createLiteral(String.valueOf(rb), rb.getLang(), toRDFDatatype(rb_type));
                    } else {
                        return NodeFactory.createLiteral(String.valueOf(rb), rb.getLang());
                    }
                } else if (resource instanceof java.lang.Integer) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDinteger));
                } else if (resource instanceof java.lang.Short) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDinteger));
                } else if (resource instanceof java.lang.Float) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDfloat));
                } else if (resource instanceof java.lang.Double) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDdouble));
                } else if (resource instanceof java.math.BigDecimal) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDdecimal));
                } else if (resource instanceof java.sql.Blob) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDhexBinary));
                } else if (resource instanceof java.sql.Date) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDdate));
                } else if (resource instanceof java.sql.Timestamp) {
               /* return NodeFactory.createLiteral(
                        Timestamp2String((java.sql.Timestamp) resource), toRDFDatatype(XSDDatatype.XSDdateTime));*/
                    return NodeFactory.createLiteral(
                            Timestamp2String((java.sql.Timestamp) resource), toRDFDatatype(XSDDatatype.XSDdateTimeStamp));
                } else if (resource instanceof java.sql.Time) {
                    return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDdateTime));
                } else if (resource instanceof String) {
                    if (isIRI(resource)) {
                        return NodeUtils.asNode(String.valueOf(resource));
                  /*  } else if (isURI(resource) || isURL(resource)) {
                        return NodeFactory.createURI(String.valueOf(resource));
                    } else if (isDouble(resource)) {
                        return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDdouble));
                    } else if (isFloat(resource)) {
                        return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDfloat));
                    } else if (isInt(resource)) {
                        return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDinteger));
                    } else if (isNumeric(resource)) {
                        return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDinteger));*/
                    } else {
                        return NodeFactory.createLiteral(String.valueOf(resource), toRDFDatatype(XSDDatatype.XSDstring));
                    }
                }else{
                    logger.error("The Node Datatype '" + resource.getClass().getName() + "' is not recognised");
                    return null;
                }
            }
        }catch(Exception e){
            logger.error(e.getMessage(),e);
            return null;
        }
    }

    /**
     * Method to check if a String uri is a IRI normalized.
     * http://stackoverflow.com/questions/9419658/normalising-possibly-encoded-uri-strings-in-java
     *
     * @param uri the String to verify.
     * @return if true the String is a valid IRI.
     */
    private static Boolean isIRI(Object uri) {
        try {
            IRIFactory factory = IRIFactory.uriImplementation();
            IRI iri = factory.construct(String.valueOf(uri));
           /* ArrayList<String> a = new ArrayList<>();
            a.add(iri.getScheme());
            a.add(iri.getRawUserinfo());
            a.add(iri.getRawHost());
            a.add(iri.getRawPath());
            a.add(iri.getRawQuery());
            a.add(iri.getRawFragment());*/
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Method to convert a {@link java.sql.Timestamp} to a {@link String}
     *
     * @param v the {@link java.sql.Timestamp}.
     * @return the {@link String}
     */
    private static String Timestamp2String(java.sql.Timestamp v) {
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(v);

        int year = cal.get(Calendar.YEAR);
        int month = cal.get(Calendar.MONTH) + 1;
        int day = cal.get(Calendar.DAY_OF_MONTH);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);
        int nanos = v.getNanos();

        String yearS, monthS, dayS, hourS, minuteS, secondS, nanosS;
        String zeros = "000000000";
        String yearZeros = "0000";
        StringBuffer timestampBuf;

        if (year < 1000) {
            yearS = "" + year;
            yearS = yearZeros.substring(0, (4 - yearS.length())) + yearS;
        } else {
            yearS = "" + year;
        }

        if (month < 10) monthS = "0" + month;
        else monthS = Integer.toString(month);

        if (day < 10) dayS = "0" + day;
        else dayS = Integer.toString(day);

        if (hour < 10) hourS = "0" + hour;
        else hourS = Integer.toString(hour);

        if (minute < 10) minuteS = "0" + minute;
        else minuteS = Integer.toString(minute);

        if (second < 10) secondS = "0" + second;
        else secondS = Integer.toString(second);

        if (nanos == 0) {
            nanosS = "0";
        } else {
            nanosS = Integer.toString(nanos);

            // Add leading 0
            nanosS = zeros.substring(0, (9 - nanosS.length())) + nanosS;

            // Truncate trailing 0
            char[] nanosChar = new char[nanosS.length()];
            nanosS.getChars(0, nanosS.length(), nanosChar, 0);
            int truncIndex = 8;
            while (nanosChar[truncIndex] == '0') {
                truncIndex--;
            }
            nanosS = new String(nanosChar, 0, truncIndex + 1);
        }

        timestampBuf = new StringBuffer();
        timestampBuf.append(yearS);
        timestampBuf.append("-");
        timestampBuf.append(monthS);
        timestampBuf.append("-");
        timestampBuf.append(dayS);
        timestampBuf.append("T");
        timestampBuf.append(hourS);
        timestampBuf.append(":");
        timestampBuf.append(minuteS);
        timestampBuf.append(":");
        timestampBuf.append(secondS);
        timestampBuf.append(".");
        timestampBuf.append(nanosS);
        return (timestampBuf.toString());
    }

    /**
     * Method to convert a URI {@link String} to a correct {@link RDFDatatype}  jena.
     *
     * @param uri the {@link String} of the uri resource.
     * @return the {@link RDFDatatype} of the uri resource.
     */
    public static RDFDatatype toRDFDatatype(String uri) {
        return TypeMapper.getInstance().getSafeTypeByName(toXSDDatatype(uri).getURI());
    }

    /**
     * Method to convert a {@link XSDDatatype} to a correct {@link RDFDatatype}  jena.
     *
     * @param xsdDatatype the {@link XSDDatatype} of the uri resource.
     * @return the {@link RDFDatatype} of the uri resource.
     */
    public static RDFDatatype toRDFDatatype(XSDDatatype xsdDatatype) {
        return TypeMapper.getInstance().getSafeTypeByName(xsdDatatype.getURI());
    }

    /**
     * A list of com.hp.hpl.jena.datatypes.xsd.XSDDatatype.
     * return all the XSDDatatype supported from jena.
     */
    public static final XSDDatatype allFormatsOfXSDDataTypes[] = new XSDDatatype[]{
            XSDDatatype.XSDstring, XSDDatatype.XSDENTITY, XSDDatatype.XSDID, XSDDatatype.XSDIDREF,
            XSDDatatype.XSDanyURI, XSDDatatype.XSDbase64Binary, XSDDatatype.XSDboolean, XSDDatatype.XSDbyte,
            XSDDatatype.XSDdate, XSDDatatype.XSDdateTime, XSDDatatype.XSDdecimal, XSDDatatype.XSDdouble,
            XSDDatatype.XSDduration, XSDDatatype.XSDfloat, XSDDatatype.XSDgDay, XSDDatatype.XSDgMonth,
            XSDDatatype.XSDgMonthDay, XSDDatatype.XSDgYear, XSDDatatype.XSDgYearMonth, XSDDatatype.XSDhexBinary,
            XSDDatatype.XSDint, XSDDatatype.XSDinteger, XSDDatatype.XSDlanguage, XSDDatatype.XSDlong,
            XSDDatatype.XSDName, XSDDatatype.XSDNCName, XSDDatatype.XSDnegativeInteger, XSDDatatype.XSDNMTOKEN,
            XSDDatatype.XSDnonNegativeInteger, XSDDatatype.XSDnonPositiveInteger, XSDDatatype.XSDnormalizedString,
            XSDDatatype.XSDNOTATION, XSDDatatype.XSDpositiveInteger, XSDDatatype.XSDQName, XSDDatatype.XSDshort,
            XSDDatatype.XSDtime, XSDDatatype.XSDtoken, XSDDatatype.XSDunsignedByte, XSDDatatype.XSDunsignedInt,
            XSDDatatype.XSDunsignedLong, XSDDatatype.XSDunsignedShort
    };

    /**
     * Method convert a {@link String} to {@link XSDDatatype}.
     *
     * @param uri the {@link String} uri of the XSDDatatype.
     * @return the {@link XSDDatatype} of the string uri if exists.
     */
    public static XSDDatatype toXSDDatatype(String uri) {
        for (XSDDatatype xsdDatatype : allFormatsOfXSDDataTypes) {
            if (xsdDatatype.getURI().equalsIgnoreCase(XSDDatatype.XSD + "#" + uri)) return xsdDatatype;
            if (xsdDatatype.getURI().replace(XSDDatatype.XSD, "")
                    .toLowerCase().contains(uri.toLowerCase())) return xsdDatatype;
        }
        logger.error("The XSD Datatype '" + uri + "' is not recognised");
        throw new IllegalArgumentException("The XSD Datatype '" + uri + "' is not recognised");
    }

    public static Node toNode(Object resource, String lang, RDFDatatype rdfDatatype) {
        return createNodeBase(resource, null, rdfDatatype, null);
    }

    public static Node toNode(Object resource, String lang, boolean isXml) {
        return createNodeBase(resource, null, null, isXml);
    }

    public static Node toNode(Object resource) {
        return createNodeBase(resource, null, null, null);
    }

    public static Node toNode(Object resource, RDFDatatype rdfDatatype) {
        return createNodeBase(resource, null, rdfDatatype, null);
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
    public static String toString(Node n) {
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

    /**
     * Method to substitute all Bindings on the query String.
     * @param query the String of the Query.
     * @param querySolution the QuerySolution with the variable to check.
     * @return the content String of the Query update.
     */
    public static String substituteBindings(String query, QuerySolution querySolution) {
        if (querySolution == null)return query;

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
                    RDFNode val = querySolution.get(varName);
                    if (val != null) {
                        varData = toString(val.asNode());
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

    // Make query

    public static Query toQuery(String queryStr) {
        return QueryFactory.create(queryStr);
    }

    public static Query toQuery(Query query) {
        return QueryFactory.create(query);
    }






}
