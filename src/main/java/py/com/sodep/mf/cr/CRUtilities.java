package py.com.sodep.mf.cr;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import py.com.sodep.mf.cr.conf.CRColumn;
import py.com.sodep.mf.cr.conf.TemporalConfigurationError;
import py.com.sodep.mf.exchange.objects.data.ConditionalCriteria;
import py.com.sodep.mf.exchange.objects.data.ConditionalCriteria.CONDITION_TYPE;
import py.com.sodep.mf.exchange.objects.data.Criteria;
import py.com.sodep.mf.exchange.objects.data.MFRestriction;

public class CRUtilities {

	/**
	 * Return the names of those columns that are members of the primary keys
	 * 
	 * @param columns
	 * @return
	 */
	public static Set<String> determineTargetPKColumns(List<CRColumn> columns) {
		TreeSet<String> pkCols = new TreeSet<String>();
		for (CRColumn crColumn : columns) {
			if (crColumn.isPKMember()) {
				pkCols.add(crColumn.getTargetColumn());
			}
		}
		return pkCols;
	}

	public static Set<String> determineSourcePKColumns(List<CRColumn> columns) {
		TreeSet<String> pkCols = new TreeSet<String>();
		for (CRColumn crColumn : columns) {
			if (crColumn.isPKMember()) {
				pkCols.add(crColumn.getSourceColumn());
			}
		}
		return pkCols;
	}

	/**
	 * A map that contains all target columns
	 * 
	 * @param columns
	 * @return
	 */
	public static Set<String> targetColumns(List<CRColumn> columns) {
		TreeSet<String> pkCols = new TreeSet<String>();
		for (CRColumn crColumn : columns) {
			pkCols.add(crColumn.getTargetColumn());
		}
		return pkCols;
	}

	public static Map<String, CRColumn> mapFromSourceColumnName(List<CRColumn> columns)
			throws TemporalConfigurationError {

		HashMap<String, CRColumn> map = new HashMap<String, CRColumn>();
		for (CRColumn crColumn : columns) {
			if (map.put(crColumn.getSourceColumn(), crColumn) != null) {
				throw new TemporalConfigurationError("Duplicate entries for source column '"
						+ crColumn.getSourceColumn() + "'. Column names are case insensitive");
			}
		}
		return map;
	}

	/**
	 * Chesck that there aren't CRColumns with the same target (case insensitive).
	 * 
	 * @param columns
	 * @throws TemporalConfigurationError
	 */
	public static Map<String, CRColumn> mapFromTargetColumn(List<CRColumn> columns) throws TemporalConfigurationError {

		HashMap<String, CRColumn> map = new HashMap<String, CRColumn>();
		for (CRColumn crColumn : columns) {
			if (map.put(crColumn.getTargetColumn().toUpperCase(), crColumn) != null) {
				throw new TemporalConfigurationError("Duplicate entries for target column '"
						+ crColumn.getSourceColumn() + "'. Column names are case insensitive");
			}

		}
		return map;
	}

	/**
	 * Receives a map that contains keys from the target column and map them to
	 * the source column.
	 * 
	 * @param targetToColumns
	 * @param row
	 * @return
	 */
	public static Map<String, Object> translateToSourceValues(Map<String, CRColumn> targetToColumns,
			Map<String, Object> row) {
		Set<String> targetCols = row.keySet();
		Iterator<String> it = targetCols.iterator();
		HashMap<String, Object> sourceColsMap = new HashMap<String, Object>();
		while (it.hasNext()) {
			String targetCol = it.next();
			Object value = row.get(targetCol);
			String sourceCol = targetToColumns.get(targetCol).getSourceColumn();
			sourceColsMap.put(sourceCol, value);

		}
		return sourceColsMap;
	}

	public static ConditionalCriteria toCriteriaUsingEquals(Map<String, Object> row) {
		ConditionalCriteria criteria = new ConditionalCriteria(CONDITION_TYPE.AND);
		Set<String> cols = row.keySet();
		for (String col : cols) {
			Object value = row.get(col);
			criteria.add(new Criteria(col, MFRestriction.OPERATOR.EQUALS, value));
		}
		return criteria;

	}

	public static String toString(Map<String, Object> row) {
		StringBuffer buff = new StringBuffer();
		Collection<Object> values = row.values();
		boolean isFirst = true;
		for (Object o : values) {
			if (!isFirst) {
				buff.append(",");
			}
			buff.append(o.toString());
		}
		return buff.toString();
	}
}
