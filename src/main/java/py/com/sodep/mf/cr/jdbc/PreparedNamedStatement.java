package py.com.sodep.mf.cr.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import py.com.sodep.mf.cr.exception.CRUnexpectedException;


/**
 * Since vanilla JDBC doesn't support named parameters this is a small wrapper
 * over {@link PreparedStatement} that support named parameters.
 * 
 * 
 * @author danicricco
 * 
 */
public class PreparedNamedStatement {

	private final Map<String, Integer> map;

	private final PreparedStatement stmt;

	public PreparedNamedStatement(Map<String, Integer> map, PreparedStatement stmt) {
		super();
		this.map = map;
		this.stmt = stmt;
	}

	public Map<String, Integer> getMap() {
		return map;
	}

	public PreparedStatement getStmt() {
		return stmt;
	}

	public void setObject(String named, Object parameterObj) throws SQLException {
		setObject(named, parameterObj, true);
	}

	public void setObject(String named, Object parameterObj, boolean failIfNotExists) throws SQLException {
		Integer index = map.get(named);
		if (index == null && failIfNotExists) {
			throw new CRUnexpectedException("Unknown parameter " + named);
		}
		if (index != null) {
			stmt.setObject(index, parameterObj);
		}

	}

	public void fill(Map<String, Object> data, boolean failIfNotExists) throws SQLException {
		fill("", data, failIfNotExists);
	}

	private void fill(String paramPrefix, Map<String, Object> data, boolean failIfNotExists) throws SQLException {
		Set<String> keys = data.keySet();
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String param = it.next();
			Object value = data.get(param);
			setObject(paramPrefix + param, value, failIfNotExists);
		}
	}

	public void fillSetClause(Map<String, Object> data, boolean failIfNotExists) throws SQLException {
		fill("s_", data, failIfNotExists);
	}

	public void fillSetClause(Map<String, Object> data) throws SQLException {
		// The method RepositoryDAO#prepareUpdateStmt
		fill("s_", data, true);
	}

	public void fillForInsert(Map<String, Object> data) throws SQLException {
		fill("", data, true);
	}

	public void fillWhereClause(Map<String, Object> data, boolean failIfNotExists) throws SQLException {
		fill("w_", data, failIfNotExists);
	}

	public void fillWhereClause(Map<String, Object> data) throws SQLException {
		fill("w_", data, true);
	}

	public boolean execute() throws SQLException {
		return stmt.execute();
	}

	public int executeUpdate() throws SQLException {
		return stmt.executeUpdate();
	}

	public ResultSet executeQuery() throws SQLException {
		return stmt.executeQuery();
	}

	public String toString() {
		return stmt.toString();
	}

	public long executeQueryCount() throws SQLException {

		ResultSet rs = executeQuery();
		if (rs.next()) {
			long count = rs.getLong(1);
			return count;
		} else {
			throw new IllegalStateException("A Query count must always have result. Error executing query ");
		}
	}
}
