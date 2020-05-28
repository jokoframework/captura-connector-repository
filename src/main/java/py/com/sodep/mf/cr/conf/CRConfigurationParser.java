package py.com.sodep.mf.cr.conf;

import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.io.xml.PrettyPrintWriter;
import com.thoughtworks.xstream.io.xml.StaxDriver;

public class CRConfigurationParser {

	private XStream xstream;

	public CRConfigurationParser() {
		xstream = new XStream(new StaxDriver());
		xstream.alias("column", CRColumn.class);
		xstream.alias("extUnit", CRExtractionUnit.class);
		xstream.alias("connection", CRConnection.class);
		xstream.alias("crdef", ConnectorDefinition.class);

		xstream.useAttributeFor(CRColumn.class, "sourceColumn");
		xstream.useAttributeFor(CRColumn.class, "targetColumn");
		xstream.useAttributeFor(CRColumn.class, "targetType");
		xstream.useAttributeFor(CRColumn.class, "triggerUpdate");
		xstream.useAttributeFor(CRColumn.class, "isPKMember");
		xstream.omitField(CRColumn.class, "length");
		xstream.omitField(CRColumn.class, "javaClass");

		xstream.useAttributeFor(CRExtractionUnit.class, "id");
		xstream.useAttributeFor(CRExtractionUnit.class, "connectionId");
		xstream.useAttributeFor(CRExtractionUnit.class, "stopOnError");
		xstream.useAttributeFor(CRExtractionUnit.class, "active");
		xstream.useAttributeFor(CRExtractionUnit.class, "frequencyInSeconds");
		xstream.useAttributeFor(CRExtractionUnit.class, "insertBatchSize");
		
		
		xstream.omitField(CRExtractionUnit.class, "lookupId");
		xstream.omitField(ConnectorDefinition.class, "sourceFile");
		

		xstream.useAttributeFor(CRConnection.class, "id");

	}

	public ConnectorDefinition parse(Reader r) {
		try {
			return (ConnectorDefinition) xstream.fromXML(r);
		}catch(Exception e){
			return null;
		}
		

	}

	/**
	 * Write a {@link ConnectorDefinition} to an outputstream as an XML. The
	 * method won't close the outputstream
	 * 
	 * @param def
	 * @param out
	 * @throws IOException
	 */
	public void write(ConnectorDefinition def, Writer out) throws IOException {
		PrettyPrintWriter w = new PrettyPrintWriter(out);
		// ObjectOutputStream out = xstream.createObjectOutputStream();
		// ObjectOutputStream outS = xstream.createObjectOutputStream(w);
		xstream.marshal(def, w);
	}

	private String toXML(Object o) {
		StringWriter w = new StringWriter();
		PrettyPrintWriter pw = new PrettyPrintWriter(w);
		xstream.marshal(o, pw);
		String xml = w.getBuffer().toString();
		try {
			w.close();
		} catch (IOException e) {
			// Since we are using a memory stream this should never happened. If
			// it happens it is a unexpected exception
			throw new RuntimeException(e);
		}
		return xml;

	}

	public static CRExtractionUnit parseExtractionUnit(Reader r) {
		CRConfigurationParser parser = new CRConfigurationParser();
		return (CRExtractionUnit) parser.xstream.fromXML(r);

	}

	public static String serialize(CRExtractionUnit lookup) {
		CRConfigurationParser parser = new CRConfigurationParser();
		return parser.toXML(lookup);
	}

	public static String computeHash(String xml) {
		MessageDigest messageDigest;
		try {
			messageDigest = MessageDigest.getInstance("MD5");
			messageDigest.update(xml.getBytes());
			String encryptedString = new String(messageDigest.digest());
			return encryptedString;
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(e);
		}

	}

}
