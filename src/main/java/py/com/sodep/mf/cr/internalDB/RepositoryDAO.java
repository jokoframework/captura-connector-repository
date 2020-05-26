package py.com.sodep.mf.cr.internalDB;

import java.io.StringReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.h2.jdbcx.JdbcConnectionPool;

import py.com.sodep.mf.cr.DynaResultSet;
import py.com.sodep.mf.cr.DynaRow;
import py.com.sodep.mf.cr.JDBCToMFHelper;
import py.com.sodep.mf.cr.conf.CRColumn;
import py.com.sodep.mf.cr.conf.CRConfigurationParser;
import py.com.sodep.mf.cr.conf.CRConnection;
import py.com.sodep.mf.cr.conf.CRExtractionUnit;
import py.com.sodep.mf.cr.conf.H2Column;
import py.com.sodep.mf.cr.exception.CRUnexpectedException;
import py.com.sodep.mf.cr.jdbc.PreparedNamedStatement;

public class RepositoryDAO {

	private static final Logger logger = LogManager.getLogger(RepositoryDAO.class);
	public static final String META_COLUMN_PREFIX = "META_";
	public static final String USER_COLUMN_PREFIX = "U_";

	private JdbcConnectionPool connPool;

	public RepositoryDAO(String filePath, String user, String pass) throws ClassNotFoundException {
		Class.forName("org.h2.Driver");
		// ;MVCC=TRUE
		connPool = JdbcConnectionPool.create("jdbc:h2:mem:" + filePath, user, pass);
		logger.debug("Created in-memory database");
	}

	public void defineDB() throws SQLException {
		Connection conn = connPool.getConnection();
		Statement stmt = conn.createStatement();
		stmt.execute(DBTrackerDefinition.CREATE_EXTRACTION_UNIT);
		stmt.execute(DBTrackerDefinition.CREATE_TABLE_CONNECTION);
		stmt.execute(DBTrackerDefinition.CREATE_SCHEMA_LOOKUPS);

		conn.close();

	}

	public CRConnection getConnection(String id) throws SQLException {
		Connection conn = connPool.getConnection();
		CRConnection crConn = getConnection(conn, id);
		conn.close();
		return crConn;
	}

	public CRExtractionUnit getExtractionUnit(String id) throws SQLException {
		Connection conn = connPool.getConnection();
		CRExtractionUnit unit = getExtractionUnit(conn, id);
		conn.close();
		return unit;

	}

	public CRExtractionUnit getExtractionUnit(Connection conn, String id) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("SELECT * from " + DBTrackerDefinition.TABLE_EXTRACTION_UNIT
				+ " WHERE ID=?");
		stmt.setString(1, id);
		ResultSet q = stmt.executeQuery();
		CRExtractionUnit unit = null;
		if (q.next()) {

			Long lookupId = (Long) q.getObject("LOOKUP_ID");
			String definition = q.getString("DEFINITION");

			boolean active = q.getBoolean("ACTIVE");
			StringReader reader = new StringReader(definition);
			unit = CRConfigurationParser.parseExtractionUnit(reader);
			unit.setLookupId(lookupId);
			unit.setActive(active);
			unit.setId(id);

		}
		return unit;
	}

	private CRConnection getConnection(Connection conn, String id) throws SQLException {

		PreparedStatement stmt = conn.prepareStatement("SELECT * from " + DBTrackerDefinition.TABLE_CONNECTION
				+ " WHERE ID=?");
		stmt.setString(1, id);
		ResultSet q = stmt.executeQuery();
		CRConnection crConn = null;
		if (q.next()) {
			String url = q.getString("URL");
			String user = q.getString("USER");
			String pass = q.getString("PASSWORD");
			String driver = q.getString("DRIVER");
			crConn = new CRConnection(id, url, user, pass, driver);
		}

		return crConn;

	}

	/**
	 * Insert a new DB source. If the connection was already part of the system.
	 * Then it will not insert anything and return false
	 * 
	 * @param connection
	 * @throws SQLException
	 */
	private boolean insertConnection(Connection dbConn, CRConnection crConn) throws SQLException {

		CRConnection existingConn = getConnection(dbConn, crConn.getId());
		if (existingConn == null) {
			logger.debug("Adding connection to " + crConn.getUrl());
			PreparedStatement stmt = dbConn.prepareStatement("INSERT INTO " + DBTrackerDefinition.TABLE_CONNECTION
					+ " VALUES (?,?,?,?,?);");
			stmt.setString(1, crConn.getId());
			stmt.setString(2, crConn.getUrl());
			stmt.setString(3, crConn.getUser());
			stmt.setString(4, crConn.getPass());
			stmt.setString(5, crConn.getDriver());
			stmt.execute();

		}

		return existingConn == null;
	}

	public Connection open() throws SQLException {
		Connection conn = connPool.getConnection();
		conn.setAutoCommit(false);
		// conn.createStatement().execute("SET DEFAULT_LOCK_TIMEOUT 60");
		return conn;
	}

	public boolean insertConnection(CRConnection crConn) throws SQLException {

		boolean inserted;
		Connection conn = connPool.getConnection();
		conn.setAutoCommit(false);
		inserted = insertConnection(conn, crConn);
		conn.commit();
		conn.close();
		return inserted;
	}

	/**
	 * Insert a new connection or update its properties if it was already on the
	 * DB
	 * 
	 * @param crConn
	 * @return
	 * @throws SQLException
	 */
	public boolean insertOrUpdateConnection(Connection dbConn, CRConnection crConn) throws SQLException {
		boolean inserted = insertConnection(dbConn, crConn);
		if (!inserted) {
			// we need to update it
			PreparedStatement stmt = dbConn.prepareStatement("UPDATE " + DBTrackerDefinition.TABLE_CONNECTION
					+ " SET url=?,user=?,password=?,driver=? where ID=? ");

			stmt.setString(1, crConn.getUrl());
			stmt.setString(2, crConn.getUser());
			stmt.setString(3, crConn.getPass());
			stmt.setString(4, crConn.getDriver());
			stmt.setString(5, crConn.getId());
			stmt.execute();
		}
		return inserted;
	}

	public void insertExtractionUnit(CRExtractionUnit lookup) throws SQLException {
		Connection conn = open();
		conn.setAutoCommit(true);
		insertExtractionUnit(conn, lookup);

		conn.close();
	}

	private void insertExtractionUnit(Connection conn, CRExtractionUnit extractUnit) throws SQLException {

		PreparedStatement stmt = conn.prepareStatement("INSERT INTO " + DBTrackerDefinition.TABLE_EXTRACTION_UNIT
				+ " VALUES (?,?,?,?);");
		stmt.setString(1, extractUnit.getId());
		stmt.setLong(2, extractUnit.getLookupId());
		String lookupXML = CRConfigurationParser.serialize(extractUnit);
		stmt.setString(3, lookupXML);
		stmt.setBoolean(4, extractUnit.isActive());
		stmt.execute();

	}

	public void updateExtractionUnit(CRExtractionUnit extractUnit) throws SQLException {
		Connection conn = open();
		conn.setAutoCommit(true);
		updateExtractionUnit(conn, extractUnit);

		conn.close();
	}

	private void updateExtractionUnit(Connection conn, CRExtractionUnit extractUnit) throws SQLException {
		PreparedStatement stmt = conn.prepareStatement("UPDATE " + DBTrackerDefinition.TABLE_EXTRACTION_UNIT
				+ " SET LOOKUP_ID=? , DEFINITION=? , ACTIVE=? where ID = ?");

		stmt.setLong(1, extractUnit.getLookupId());
		String lookupXML = CRConfigurationParser.serialize(extractUnit);
		stmt.setString(2, lookupXML);
		stmt.setBoolean(3, extractUnit.isActive());
		stmt.setString(4, extractUnit.getId());
		stmt.execute();
	}

	public void dispose() {
		connPool.dispose();
	}

	public static String buildCreateTableDefinition(String identifier, List<CRColumn> columns) {
		String createTable = "CREATE TABLE " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier + "("
				+ META_COLUMN_PREFIX + "ROWID IDENTITY,";
		for (int i = 0; i < columns.size(); i++) {
			CRColumn crColumn = columns.get(i);
			String colName = USER_COLUMN_PREFIX + crColumn.getTargetColumn();
			String classTypeStr = crColumn.getJavaClass();
			String h2Type = JDBCToMFHelper.jdbcToH2(classTypeStr, crColumn.getLength());

			createTable += colName + " " + h2Type;
			if (crColumn.isPKMember()) {
				// createTable += " PRIMARY KEY";
			}

			createTable += ",";

		}
		createTable += META_COLUMN_PREFIX + "STATUS VARCHAR(25) DEFAULT '"
				+ DBTrackerDefinition.SYNCHRONIZATION_STATUS.INSERTED.toString() + "'," + META_COLUMN_PREFIX
				+ "LAST_CHANGE TIMESTAMP AS CURRENT_TIMESTAMP," + META_COLUMN_PREFIX + "SHOULD_DELETE boolean);";

		return createTable;
	}

	public static String buildIndexDefinition(String identifier, List<CRColumn> columns, boolean useOnlyPK) {
		String createIndex = "CREATE INDEX IDX_" + identifier + " ON " + DBTrackerDefinition.LOOKUP_SCHEMA + "."
				+ identifier + "(";
		int indexedColumns = 0;
		for (int i = 0; i < columns.size(); i++) {
			CRColumn crColumn = columns.get(i);
			if (crColumn.isPKMember() || !useOnlyPK) {
				String colName = USER_COLUMN_PREFIX + crColumn.getTargetColumn();
				if (indexedColumns > 0) {
					createIndex += ",";
				}
				createIndex += colName;
				indexedColumns++;
			}
		}
		createIndex += ")";
		return createIndex;
	}

	private void executeDML(String dml) throws SQLException {
		Connection conn = null;
		try {
			conn = open();
			Statement stmt = conn.createStatement();
			stmt.execute(dml);
		} finally {
			if (conn != null) {
				conn.close();
			}
		}
	}

	public void createTableDefinitionForLookup(String identifier, List<CRColumn> columns) throws SQLException {
		String createTableCommand = buildCreateTableDefinition(identifier, columns);
		executeDML(createTableCommand);
	}

	/**
	 * Create an index for the lookup tables
	 * 
	 * @param identifier
	 * @param columns
	 * @param useOnlyPKColumns
	 *            if this is true only the columns of the PK will be included
	 * @throws SQLException
	 */
	public void createIndexForLookup(String identifier, List<CRColumn> columns, boolean useOnlyPKColumns)
			throws SQLException {
		String createTableCommand = buildIndexDefinition(identifier, columns, useOnlyPKColumns);
		executeDML(createTableCommand);
	}

	/**
	 * This method check if a table on the lookup table schema exists and return
	 * its definition. The definition is indexed on the target column
	 * 
	 * @param identifier
	 * @return
	 * @throws SQLException
	 */
	public Map<String, H2Column> getTableDefinition(String identifier) throws SQLException {
		// The columns of the temporal table have either a user prefix or a meta
		// prefix.
		// This way we can easily distinguish them.
		String tableName = identifier.toUpperCase();
		Connection conn = null;
		try {
			conn = open();
			DatabaseMetaData dbMetadata = conn.getMetaData();

			ResultSet rsTables = dbMetadata.getTables(null, DBTrackerDefinition.LOOKUP_SCHEMA, tableName, null);

			if (rsTables.next()) {
				logger.trace("Table for lookup " + identifier + " already exist. Checking its definition");
				Map<String, H2Column> columns = new HashMap<String, H2Column>();
				ResultSet rsColumns = dbMetadata.getColumns(null, DBTrackerDefinition.LOOKUP_SCHEMA, tableName, null);

				while (rsColumns.next()) {
					String colName = rsColumns.getString("COLUMN_NAME");
					if (colName.startsWith(USER_COLUMN_PREFIX)) {
						String h2TypeName = rsColumns.getString("TYPE_NAME");
						int length = rsColumns.getInt("COLUMN_SIZE");

						H2Column col = new H2Column(colName, h2TypeName);
						col.setLength(length);
						colName = colName.substring(USER_COLUMN_PREFIX.length());
						columns.put(colName, col);
					}

				}
				// ResultSet rsPks = dbMetadata.getPrimaryKeys(null,
				// DBTrackerDefinition.LOOKUP_SCHEMA, tableName);
				// while (rsPks.next()) {
				// String colName = rsPks.getString("COLUMN_NAME");
				// columns.get(colName).setPkMember(true);
				// }
				return columns;
			}
			return null;
		} finally {
			if (conn != null) {
				conn.close();
			}
		}

	}

	public int deleteRowsOnStatuses(Connection conn, String identifier,
			DBTrackerDefinition.SYNCHRONIZATION_STATUS statuses[]) throws SQLException {
		Map<String, H2Column> tableDefinition = getTableDefinition(identifier);
		if (tableDefinition == null) {
			throw new CRUnexpectedException("Can't insert data on a non existing table");
		}
		String deleteDML = "DELETE FROM " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier + " WHERE ";
		for (int i = 0; i < statuses.length; i++) {
			deleteDML += " " + META_COLUMN_PREFIX + "STATUS='" + statuses[i].toString() + "'";
			if (i < statuses.length - 1) {
				deleteDML += " OR ";
			}

		}

		PreparedStatement stmt = conn.prepareStatement(deleteDML);
		return stmt.executeUpdate();

	}

	/**
	 * This method creates a statement that can be used to change the status of
	 * a given row. The valid named parameters are "status" and "rowId"
	 * 
	 * @param conn
	 * @param identifier
	 * @return
	 * @throws SQLException
	 */
	public PreparedNamedStatement prepareUpdateStatusStmt(Connection conn, String identifier) throws SQLException {
		Map<String, H2Column> tableDefinition = getTableDefinition(identifier);
		if (tableDefinition == null) {
			throw new CRUnexpectedException("Can't insert data on a non existing table");
		}
		String updateDML = "UPDATE " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier;
		updateDML += " set " + META_COLUMN_PREFIX + "STATUS=? WHERE " + META_COLUMN_PREFIX + "ROWID = ?";
		HashMap<String, Integer> namedMap = new HashMap<String, Integer>();
		namedMap.put("status", 1);
		namedMap.put("rowId", 2);
		PreparedStatement stmt = conn.prepareStatement(updateDML);

		return new PreparedNamedStatement(namedMap, stmt);
	}

	public PreparedNamedStatement prepareShuouldDeleteUpdate(Connection conn, String identifier) throws SQLException {
		Map<String, H2Column> tableDefinition = getTableDefinition(identifier);
		if (tableDefinition == null) {
			throw new CRUnexpectedException("Can't insert data on a non existing table");
		}
		String updateDML = "UPDATE " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier;
		updateDML += " set " + META_COLUMN_PREFIX + "SHOULD_DELETE=? WHERE " + META_COLUMN_PREFIX + "ROWID = ?";
		HashMap<String, Integer> namedMap = new HashMap<String, Integer>();
		namedMap.put("shouldDelete", 1);
		namedMap.put("rowId", 2);
		PreparedStatement stmt = conn.prepareStatement(updateDML);

		return new PreparedNamedStatement(namedMap, stmt);
	}

	public void markEverythingAsDeleteCandidates(Connection conn, String identifier) throws SQLException {
		PreparedStatement updateStmt = conn.prepareStatement("UPDATE " + DBTrackerDefinition.LOOKUP_SCHEMA + "."
				+ identifier + " SET " + META_COLUMN_PREFIX + "SHOULD_DELETE=true");
		updateStmt.executeUpdate();
	}

	public int setDeleteStatusOnShouldDeletes(Connection conn, String identifier) throws SQLException {
		PreparedStatement updateStmt = conn.prepareStatement("UPDATE " + DBTrackerDefinition.LOOKUP_SCHEMA + "."
				+ identifier + " SET " + META_COLUMN_PREFIX + "STATUS='"
				+ DBTrackerDefinition.SYNCHRONIZATION_STATUS.DELETED.toString() + "' WHERE " + META_COLUMN_PREFIX
				+ "SHOULD_DELETE=true");
		return updateStmt.executeUpdate();
	}

	/**
	 * Create an insert statement that can be used to insert data into the
	 * temporal H2 lookup table. This method return a namedParameter and all
	 * target column of the given table should be used to fill the statement.
	 * 
	 * @param conn
	 * @param identifier
	 * @param tableDef
	 * @return
	 * @throws SQLException
	 */
	public PreparedNamedStatement prepareInsertStmt(Connection conn, String identifier) throws SQLException {
		Map<String, H2Column> tableDefinition = getTableDefinition(identifier);
		if (tableDefinition == null) {
			throw new CRUnexpectedException("Can't insert data on a non existing table");
		}
		Set<String> keys = tableDefinition.keySet();
		Map<String, Integer> namedMap = new HashMap<String, Integer>();
		String insertDML = "INSERT INTO " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier + " (";

		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String userCol = it.next();
			String columnName = USER_COLUMN_PREFIX + userCol;
			insertDML += columnName + ",";

		}
		insertDML += META_COLUMN_PREFIX + "STATUS) values(";
		int i = 1;
		it = keys.iterator();
		while (it.hasNext()) {
			String param = it.next();
			insertDML += "?,";
			namedMap.put(param, i);
			i++;
		}

		insertDML += "'" + DBTrackerDefinition.SYNCHRONIZATION_STATUS.INSERTED.toString() + "')";
		PreparedStatement stmt = conn.prepareStatement(insertDML);
		return new PreparedNamedStatement(namedMap, stmt);

	}

	public List<DynaRow> find(Connection conn, String identifier, Map<String, Object> whereMap) throws SQLException {
		Map<String, H2Column> tableDefinition = getTableDefinition(identifier);
		return find(conn, tableDefinition, identifier, whereMap);
	}

	/**
	 * Create a result set to iterate over a given temporal table. The where is
	 * an 'OR' using the statuses
	 * 
	 * @param conn
	 * @param identifier
	 * @param columns
	 *            the columns that are going to be included in the DynaResultSet
	 * @param statuses
	 * @return
	 * @throws SQLException
	 */
	public DynaResultSet openCursorBasedOnStatus(Connection conn, String identifier, Set<String> columns,
			DBTrackerDefinition.SYNCHRONIZATION_STATUS statuses[]) throws SQLException {

		String sql = "SELECT * from " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier + " where ";
		for (int i = 0; i < statuses.length; i++) {
			sql += " " + META_COLUMN_PREFIX + "STATUS='" + statuses[i].toString() + "'";
			if (i < statuses.length - 1) {
				sql += " OR ";
			}

		}
		sql += " order by " + META_COLUMN_PREFIX + "ROWID";

		PreparedStatement stmt = conn.prepareStatement(sql);
		ResultSet rs = stmt.executeQuery();
		return new DynaResultSet(rs, columns);
	}

	public DynaResultSet openCursorBasedOnStatus(Connection conn, String identifier,
			DBTrackerDefinition.SYNCHRONIZATION_STATUS statuses[]) throws SQLException {
		Map<String, H2Column> tableDefinition = getTableDefinition(identifier);
		return openCursorBasedOnStatus(conn, identifier, tableDefinition.keySet(), statuses);

	}

	public DynaResultSet openCursor(Connection conn, String identifier, Set<String> columns,
			Map<String, Object> userColumnWhere) throws SQLException {
		String sql = "SELECT * from " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier;
		Map<String, Integer> namedMap = new HashMap<String, Integer>();

		if (userColumnWhere != null) {
			sql += " where ";
			Set<String> params = userColumnWhere.keySet();
			Iterator<String> it = params.iterator();
			Integer index = 1;

			while (it.hasNext()) {
				String param = it.next();
				Object value = userColumnWhere.get(param);
				if (value != null) {
					sql += USER_COLUMN_PREFIX + param + " = ? ";
					namedMap.put(param, index);
					index++;
				} else {
					sql += USER_COLUMN_PREFIX + param + " IS NULL ";
				}
				if (it.hasNext()) {
					sql += " and ";
				}
			}
		}
		sql += " order by " + META_COLUMN_PREFIX + "ROWID";
		PreparedStatement stmt = conn.prepareStatement(sql);
		if (userColumnWhere != null) {
			Set<String> params = userColumnWhere.keySet();
			Iterator<String> it = params.iterator();
			while (it.hasNext()) {
				String param = it.next();
				Object value = userColumnWhere.get(param);
				if (value != null) {
					int index = namedMap.get(param);
					stmt.setObject(index, value);
				}

			}
		}
		ResultSet rs = stmt.executeQuery();
		return new DynaResultSet(rs, columns);
	}

	/**
	 * Execute a select on a given table. This method receives the table
	 * definition so it can be cached externally and execute by the user
	 * 
	 * @param conn
	 * @param identifier
	 * @param The
	 *            result of calling {@link #getTableDefinition(String)} with the
	 *            identifier supplied
	 * @param whereMap
	 *            all values will be used with the equals operator
	 * @return
	 * @throws SQLException
	 */
	public List<DynaRow> find(Connection conn, Map<String, H2Column> tableDefinition, String identifier,
			Map<String, Object> whereMap) throws SQLException {

		if (tableDefinition == null) {
			throw new CRUnexpectedException("Can't insert data on a non existing table");
		}

		Set<String> userColumns = tableDefinition.keySet();
		DynaResultSet rs = openCursor(conn, identifier, userColumns, whereMap);
		ArrayList<DynaRow> data = new ArrayList<DynaRow>();

		while (rs.next()) {
			DynaRow row = rs.getRow();
			data.add(row);
		}
		return data;

	}

	/**
	 * 
	 * @param conn
	 * @param identifier
	 * @param whereMap
	 * @param newValues
	 * @throws SQLException
	 */
	public PreparedNamedStatement prepareUpdateStmt(Connection conn, String identifier, Set<String> whereParameter,
			Set<String> setParams) throws SQLException {
		Map<String, H2Column> tableDefinition = getTableDefinition(identifier);
		if (tableDefinition == null) {
			throw new CRUnexpectedException("Can't insert data on a non existing table");
		}
		// TODO check that the "newValues" map has the same columns (or a
		// subset) of the tableDefinition

		Map<String, Integer> namedMap = new HashMap<String, Integer>();

		String sql = "UPDATE " + DBTrackerDefinition.LOOKUP_SCHEMA + "." + identifier + " SET  ";

		Iterator<String> it = setParams.iterator();

		Integer index = 1;
		while (it.hasNext()) {
			String param = it.next();
			sql += USER_COLUMN_PREFIX + param + " = ? , ";
			namedMap.put("s_" + param, index);
			index++;
		}
		sql += " " + META_COLUMN_PREFIX + "STATUS='" + DBTrackerDefinition.SYNCHRONIZATION_STATUS.UPDATED + "' where ";

		it = whereParameter.iterator();
		while (it.hasNext()) {
			String whereParam = it.next();
			sql += USER_COLUMN_PREFIX + whereParam + " = ? ";
			if (it.hasNext()) {
				sql += " and ";
			}
			namedMap.put("w_" + whereParam, index);
			index++;
		}

		PreparedStatement stmt = conn.prepareStatement(sql);
		return new PreparedNamedStatement(namedMap, stmt);
	}

	/*
	 * public static void main(String[] args) throws ClassNotFoundException,
	 * SQLException {
	 * 
	 * 
	 * 
	 * DOMConfigurator.configure("log4j.xml"); logger.debug("starting app");
	 * 
	 * RepositoryDAO serverTracker = new RepositoryDAO("work/lookups", "sa", "");
	 * serverTracker.defineDB();
	 * 
	 * serverTracker.insertConnection(new CRConnection("myURL", "ddd", "user",
	 * "XXXX", "org.postgres"));
	 * 
	 * serverTracker.getTableDefinition("MYLOOKUP");
	 * 
	 * serverTracker.dispose();
	 * 
	 * 
	 * 
	 * }
	 */
}
