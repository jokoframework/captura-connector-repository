package py.com.sodep.mf.cr;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import py.com.sodep.mf.cr.conf.CRConnection;
import py.com.sodep.mf.cr.conf.TemporalConfigurationError;
import py.com.sodep.mf.cr.exception.CRUnexpectedException;
import py.com.sodep.mf.cr.exception.ConnectionException;
import py.com.sodep.mf.cr.jdbc.PreparedNamedStatement;
import py.com.sodep.mf.exchange.MFLoookupTableDefinition;

public class MultiDBManager {

	private static final Logger logger = LogManager.getLogger(MultiDBManager.class);
	private final ConcurrentHashMap<String, CRConnection> connectionMap;

	public MultiDBManager() {
		this.connectionMap = new ConcurrentHashMap<String, CRConnection>();
	}

	/**
	 * Store the connection description in the internal cache. If the test of
	 * communication failed the connection won't be added
	 * 
	 * @param connection
	 * @throws ConnectionException
	 * @throws TemporalConfigurationError
	 */
	public void register(CRConnection connectionDescription, boolean testCommunication)
			throws TemporalConfigurationError, ConnectionException {

		if (testCommunication) {
			logger.debug("Testing communication to " + connectionDescription.getUrl());
			Connection conn = open(connectionDescription);
			try {

				logger.info("Successfully connected to " + connectionDescription.getUrl());
				conn.close();

			} catch (SQLException e) {
				throw new ConnectionException(connectionDescription, e);
			}
		}
		this.connectionMap.put(connectionDescription.getId(), connectionDescription);
	}

	private Connection open(CRConnection connectionDescription) throws ConnectionException {
		try {
			Class.forName(connectionDescription.getDriver());
			Connection connection = DriverManager.getConnection(connectionDescription.getUrl(),
					connectionDescription.getUser(), connectionDescription.getPass());
			return connection;
		} catch (ClassNotFoundException e) {
			throw new CRUnexpectedException("The driver " + connectionDescription.getDriver() + " is not registered. "
					+ e.getMessage());
		} catch (SQLException e) {
			throw new ConnectionException(connectionDescription, e);
		}
	}

	public Connection open(String connectionId) throws ConnectionException {
		CRConnection crConnection = this.connectionMap.get(connectionId);
		if (crConnection == null) {
			throw new CRUnexpectedException("Can't open a connection to a non registered DB");
		}
		return open(crConnection);
	}

	/**
	 * This method will automatically create a lookup definition from an SQL
	 * query
	 * 
	 * @param connectionId
	 * @param select
	 * @return
	 * @throws ConnectionException
	 * @throws TemporalConfigurationError
	 */
	public MFLoookupTableDefinition getLookupDefinition(String connectionId, String sql)
			throws TemporalConfigurationError, ConnectionException {

		Connection conn = open(connectionId);
		Statement stmt;
		try {
			stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			ResultSetMetaData rsMetadata = rs.getMetaData();
			MFLoookupTableDefinition def = new MFLoookupTableDefinition();
			for (int i = 0; i < rsMetadata.getColumnCount(); i++) {
				String columnName = rsMetadata.getColumnName(i);
				String classType = rsMetadata.getColumnClassName(i);
			}
			return def;
		} catch (SQLException e) {
			throw new TemporalConfigurationError("Unable to execute query " + sql, e);
		}

	}

	/**
	 * Open a resultset on the
	 * 
	 * @param connectionId
	 * @param sql
	 * @return
	 */
	public ResultSet openResultSet(String connectionId, String sql) {
		return null;
	}

	/**
	 * Use the target sql as a subquery of a query that counts the number of
	 * data that matched the given where clause. The only supported operator on
	 * the where clause is the equals
	 * 
	 * @param conn
	 * @param columns
	 * @return
	 * @throws SQLException
	 */
	public PreparedNamedStatement prepareCountSelectOnColumns(Connection conn, String sql, Set<String> columns)
			throws SQLException {
		String sqlWithoutSemicolon = null;
		if (sql.endsWith(";")) {
			sqlWithoutSemicolon = sql.substring(0, sql.length() - 1);
		} else {
			sqlWithoutSemicolon = sql;
		}

		String sqlCount = "Select count(1) from ( " + sqlWithoutSemicolon + " ) as Q where ";

		HashMap<String, Integer> indexMap = new HashMap<String, Integer>();
		Iterator<String> it = columns.iterator();
		Integer index = 1;
		while (it.hasNext()) {
			String col = it.next();
			
			sqlCount += col + " = ?";
			if (it.hasNext()) {
				sqlCount += " and ";
			}
			indexMap.put(col, index);
			index++;
		}
		PreparedStatement stmt = conn.prepareStatement(sqlCount);
		return new PreparedNamedStatement(indexMap, stmt);
	}
}
