package py.com.sodep.mf.cr;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import py.com.sodep.mf.cr.conf.CRColumn;
import py.com.sodep.mf.exchange.MFField;
import py.com.sodep.mf.exchange.MFField.FIELD_TYPE;

public class JDBCToMFHelper {

	/**
	 * 
	 * @param the
	 *            class that is used by the JDBC driver when the method
	 *            rs.getObject() is called
	 * @return
	 */
	public static MFField.FIELD_TYPE jdbcTypeToMF(Class javaChoosenClass) {
		if (Number.class.isAssignableFrom(javaChoosenClass)) {
			return FIELD_TYPE.NUMBER;
		} else if (Boolean.class.isAssignableFrom(javaChoosenClass)) {
			return FIELD_TYPE.BOOLEAN;
		} else if (Date.class.isAssignableFrom(javaChoosenClass)
				|| java.util.Date.class.isAssignableFrom(javaChoosenClass)) {
			return FIELD_TYPE.DATE;
		} else if (String.class.isAssignableFrom(javaChoosenClass)) {
			return FIELD_TYPE.STRING;
		}
		return null;

	}

	public static String jdbcToH2(String classType, int precision) {
		if (classType.equals(Long.class.getCanonicalName()) || classType.equals(Integer.class.getCanonicalName())
				|| classType.equals(Short.class.getCanonicalName())
				|| classType.equals(Double.class.getCanonicalName())
				|| classType.equals(Float.class.getCanonicalName())
				|| classType.equals(BigDecimal.class.getCanonicalName())) {
			return "BIGINT";
		} else if (classType.equals(Double.class.getCanonicalName())
				|| classType.equals(Float.class.getCanonicalName())
				|| classType.equals(BigDecimal.class.getCanonicalName())) {
			return "DOUBLE";
		} else if (classType.equals(String.class.getCanonicalName())) {
			return "VARCHAR(" + precision + ")";
		} else if (classType.equals(Timestamp.class.getCanonicalName())
				|| classType.equals(Date.class.getCanonicalName())
				|| classType.equals(java.sql.Date.class.getCanonicalName())) {
			return "TIMESTAMP";
		} else if (classType.equals(Boolean.class.getCanonicalName())) {
			return "BOOLEAN";
		}
		return null;
	}

	/**
	 * Translate a list of CRColumn to the corresponding definition on a lookup
	 * table. The method expect that all CRColumn are completely defined
	 * 
	 * @param columns
	 * @return
	 */
	public static List<MFField> createLookupDefinition(List<CRColumn> columns) {
		ArrayList<MFField> fields = new ArrayList<MFField>();
		for (CRColumn col : columns) {

			FIELD_TYPE fieldType = FIELD_TYPE.valueOf(col.getTargetType());
			MFField field = new MFField(fieldType, col.getTargetColumn());
			field.setPk(col.isPKMember());
			fields.add(field);
		}
		return fields;
	}

	public static final String unsupportedPlain(List<CRColumn> columns) {
		StringBuffer buff = new StringBuffer();
		for (CRColumn crColumn : columns) {
			buff.append(crColumn.getSourceColumn() + "(" + crColumn.getJavaClass() + ")");
		}
		return buff.toString();
	}
}
