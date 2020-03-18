package py.com.sodep.mf.cr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import py.com.sodep.mf.cr.conf.CRColumn;
import py.com.sodep.mf.cr.conf.CRExtractionUnit;
import py.com.sodep.mf.cr.conf.H2Column;
import py.com.sodep.mf.cr.conf.TemporalConfigurationError;
import py.com.sodep.mf.cr.conf.UnrecoverableConfigurationError;
import py.com.sodep.mf.cr.exception.CRUnexpectedException;
import py.com.sodep.mf.cr.exception.ConnectionException;
import py.com.sodep.mf.cr.internalDB.DBTrackerDefinition;
import py.com.sodep.mf.cr.internalDB.RepositoryDAO;
import py.com.sodep.mf.cr.jdbc.PreparedNamedStatement;
import py.com.sodep.mf.cr.webapi.MFWebApiLookupFacade;
import py.com.sodep.mf.cr.webapi.WebApiClient;
import py.com.sodep.mf.cr.webapi.exception.RestAuhtorizationException;
import py.com.sodep.mf.cr.webapi.exception.WebApiException;
import py.com.sodep.mf.exchange.MFDataHelper;
import py.com.sodep.mf.exchange.MFLoookupTableDefinition;
import py.com.sodep.mf.exchange.objects.data.ConditionalCriteria;
import py.com.sodep.mf.exchange.objects.data.MFOperationResult;

public class LookupPopulator implements Runnable {

	private static final int DEFAULT_INSERT_BATCH_SIZE = 500;
	private static final int LOG_FREQUENCY = 10 * 1000;// log something every
														// 120
														// seconds
	private static final int DEFAULT_FREQUENCY = 10;// 10 seconds
	private static final Logger logger = LogManager.getLogger(LookupPopulator.class);

	private final WebApiClient restClient;
	private final CRServer server;
	private final MultiDBManager dbManager;
	private final CRExtractionUnit extUnit;
	private final RepositoryDAO repositoryDAO;
	private volatile boolean alive;

	private Object myThreadLock = new Object();
	private Thread myThread;

	public LookupPopulator(CRServer server, MultiDBManager dbManager, CRExtractionUnit extUnit,
			RepositoryDAO repositoryDAO) {
		this.server = server;
		this.dbManager = dbManager;
		this.extUnit = extUnit;
		this.repositoryDAO = repositoryDAO;
		this.restClient = server.buildRestClient();
	}

	/**
	 * Start the threads required to check and push the data of the extraction
	 * unit. Currently only one thread is initiated, but this might change in
	 * the future.
	 * 
	 * @return
	 */
	public int start() {
		if (!extUnit.isActive()) {
			logger.warn("Thread for extraction unit #" + extUnit.getId()
					+ " was not started because it was declared as active=false");
			return 0;
		}
		alive = true;
		Thread me;
		synchronized (myThreadLock) {
			me = myThread = new Thread(this, "Lookup Populator #" + extUnit.getId());
		}
		me.start();
		return 1;
	}

	public void stop() {
		alive = false;
		Thread me;

		synchronized (myThreadLock) {
			me = myThread;
		}
		if (me != null) {
			me.interrupt();
		}
		this.restClient.close();
	}

	public void join() throws InterruptedException {
		Thread me;
		synchronized (myThreadLock) {
			me = myThread;
		}
		if (me != null) {
			me.join();
		}
	}

	/**
	 * Read data from the external DB and save it into the H2 database. When
	 * this method ends the rows that have changed are going to be marked as
	 * INSERTED, UPDATED or DELETED. They might remain in the previous state if
	 * there were no changes on the row.
	 * 
	 * @throws SQLException
	 * @throws ConnectionException
	 * @throws TemporalConfigurationError
	 * @throws UnrecoverableConfigurationError
	 * @throws InterruptedException
	 */
	private void localSynchronization() throws ConnectionException, TemporalConfigurationError,
			UnrecoverableConfigurationError, InterruptedException {

		Connection jdbcExternalconnection = null;// Connection to the external
													// DB
		Connection h2Conn = null;// The connection to the internal h2 database

		try {
			try {
				// The fist step is to check if data can be imported to the
				// table
				// Even-though we have checked this during startup the external
				// DB
				// might have changed.
				jdbcExternalconnection = dbManager.open(extUnit.getConnectionId());
				// The first step is to check if the
				logger.debug("Checking compatibility of the temporal h2 table for extraction unit #" + extUnit.getId());
				Map<String, CRColumn> sourceColumnToClasses = server.obtainCRColumns(jdbcExternalconnection, extUnit);
				ArrayList<CRColumn> supportedColumns = server.determineSupportedColumns(sourceColumnToClasses, false);
				Map<String, H2Column> tableDefinition = repositoryDAO.getTableDefinition(extUnit.getId());
				CRServer.checkDefinition(extUnit.getId(), supportedColumns, tableDefinition);
				logger.debug("SUCCESS. The extraction unit " + extUnit.getId()
						+ " can be inserted into the temporal data");
				Set<String> pkColumns = CRUtilities.determineTargetPKColumns(supportedColumns);
				Set<String> targetColumns = CRUtilities.targetColumns(supportedColumns);
				if (pkColumns.size() <= 0) {
					throw new UnrecoverableConfigurationError(
							"Can't process extraction unit that doesn't have PK columns. " + extUnit.getId());
				}

				h2Conn = repositoryDAO.open();
				extractData(jdbcExternalconnection, h2Conn, supportedColumns, tableDefinition, pkColumns, targetColumns);
				// markDeletedRows(jdbcExternalconnection, h2Conn,
				// supportedColumns, pkColumns);
				markAndSweepDeletedRows(jdbcExternalconnection, h2Conn, tableDefinition, supportedColumns, pkColumns);
			} finally {
				if (jdbcExternalconnection != null) {
					jdbcExternalconnection.close();
				}
				if (h2Conn != null) {
					h2Conn.close();
				}
			}
		} catch (SQLException e) {
			// We handle the SQLExeption as temporal errors since it might be
			// cause due to a problem on the remote database. The next time we
			// might succeed
			throw new TemporalConfigurationError("", e);
		}
	}

	/**
	 * <p>
	 * This method implement a mark-and-sweep-like algorithm to find the deleted
	 * rows. Assume that everything is going to be deleted, then iterate over
	 * the external table and mark all rows that shouldn't be deleted. Finally,
	 * changed the status to DELETED of all rows that were not marked (sweep!).
	 * </p>
	 * <p>
	 * This method has the advantage that the external cycle is over the remote
	 * table. Therefore, if its not correctly indexed it won't affect two much
	 * (only one query on the remote table). On the other hand, the method
	 * {@link #markDeletedRows(Connection, Connection, ArrayList, Set)} iterates
	 * over the h2 table and for every row will make a new query to the external
	 * database (n queries to the external DB, where n is the number of rows on the local DB)
	 * </p>
	 * 
	 * @param jdbcExternalconnection
	 * @param h2Conn
	 * @param tableDefinition
	 * @param supportedColumns
	 * @param pkColumns
	 * @throws SQLException
	 * @throws TemporalConfigurationError
	 * @throws InterruptedException
	 */
	private void markAndSweepDeletedRows(Connection jdbcExternalconnection, Connection h2Conn,
			Map<String, H2Column> tableDefinition, ArrayList<CRColumn> supportedColumns, Set<String> pkColumns)
			throws SQLException, TemporalConfigurationError, InterruptedException {
		logger.trace("-----------------------------");
		logger.debug("Analyzing deleted rows of #" + extUnit.getId() + ". Using mark-and-sweep algorithm");
		logger.trace("-----------------------------");
		TimerStats logTimer;// in order to avoid extensive logging the
		DeleteStatistics deleteStatistics = new DeleteStatistics();
		// mark that everything should be deleted
		repositoryDAO.markEverythingAsDeleteCandidates(h2Conn, extUnit.getId());
		Statement stmt = jdbcExternalconnection.createStatement();
		ResultSet rs = stmt.executeQuery(extUnit.getSql());
		PreparedNamedStatement stmtShouldDelete = repositoryDAO.prepareShuouldDeleteUpdate(h2Conn, extUnit.getId());
		stmtShouldDelete.setObject("shouldDelete", false);
		// iterate over the external table and check mark this rows as
		// SHOULD_DELETE=false
		deleteStatistics.startTime();
		logTimer = TimerStats.startTimer();
		while (rs.next()) {
			if (!alive) {
				throw new InterruptedException();
			}
			deleteStatistics.incNumberOfRowsAnalyzed();
			Map<String, Object>[] externalRowMaps = loadExternalRow(supportedColumns, rs, true);
			Map<String, Object> externalDataPKOnly = externalRowMaps[0];
			List<DynaRow> row = repositoryDAO.find(h2Conn, tableDefinition, extUnit.getId(), externalDataPKOnly);
			if (row.size() > 0) {
				// this row still exists on the remote table so we need to mark
				// it (SHOULD_DELETE=FALSE)
				DynaRow dynaRow = row.get(0);
				stmtShouldDelete.setObject("rowId", dynaRow.getRowID());
				stmtShouldDelete.executeUpdate();

			}
			if (logger.isDebugEnabled() && logTimer.elapsedTime() > LOG_FREQUENCY) {

				logger.debug("Extraction unit #" + extUnit.getId() + ".Deleting process. Elapsed time = "
						+ deleteStatistics.elapsedTime() + " sec. Analyzed "
						+ deleteStatistics.getNumberOfRowsAnalyzed());
				logTimer = TimerStats.startTimer();
			}

		}
		int deletedRows = repositoryDAO.setDeleteStatusOnShouldDeletes(h2Conn, extUnit.getId());
		logger.debug("Extraction unit #" + extUnit.getId() + ".Deleting process. Elapsed time = "
				+ deleteStatistics.elapsedTime() + " sec., deletedRows=" + deletedRows + "  Analyzed "
				+ deleteStatistics.getNumberOfRowsAnalyzed());
		// mark the DELETED status on all rows whose SHOULD_DELETE column
		// remained in SHOULD_DELETE=FALSE

		h2Conn.commit();
		// what has remained SHOULD_DELETE=TRUE should be marked as DELETED
	}

	/**
	 * This will execute a query on the local h2 database and check on the
	 * remote database if the row still exists. T The method will run faster if
	 * the remote SQL is indexed
	 * 
	 * @param jdbcExternalconnection
	 * @param h2Conn
	 * @param supportedColumns
	 * @param pkColumns
	 * @throws SQLException
	 * @throws TemporalConfigurationError
	 * @throws InterruptedException
	 */
	private void markDeletedRows(Connection jdbcExternalconnection, Connection h2Conn,
			ArrayList<CRColumn> supportedColumns, Set<String> pkColumns) throws SQLException,
			TemporalConfigurationError, InterruptedException {
		logger.trace("-----------------------------");
		logger.debug("Analyzing deleted rows of #" + extUnit.getId());
		logger.trace("-----------------------------");
		TimerStats logTimer;// in order to avoid extensive logging the
		// extraction unit will log every LOG_FREQUENCY
		// ----------------------------------------
		// CHECK ROWS THAT HAS BEEN DELETED
		// ----------------------------------------
		DynaResultSet h2Rs = repositoryDAO.openCursor(h2Conn, extUnit.getId(), pkColumns, null);
		Set<String> sourcePkCols = CRUtilities.determineSourcePKColumns(supportedColumns);
		PreparedNamedStatement testDeleteQuery = dbManager.prepareCountSelectOnColumns(jdbcExternalconnection,
				extUnit.getSql(), sourcePkCols);
		PreparedNamedStatement changeToDeletedStatus = repositoryDAO.prepareUpdateStatusStmt(h2Conn, extUnit.getId());
		changeToDeletedStatus.setObject("status", DBTrackerDefinition.SYNCHRONIZATION_STATUS.DELETED.toString());
		Map<String, CRColumn> targetMap = CRUtilities.mapFromTargetColumn(supportedColumns);
		DeleteStatistics deleteStats = new DeleteStatistics();
		deleteStats.startTime();
		logTimer = TimerStats.startTimer();
		while (h2Rs.next()) {
			deleteStats.incNumberOfRowsAnalyzed();
			if (!alive) {
				throw new InterruptedException();
			}
			DynaRow row = h2Rs.getRow();
			logger.trace("Analyzing " + row);
			// this is a row on the internal DB that contains all the
			// information of the PK
			// We need to check if it still exists on the remote DB
			Map<String, Object> targetValues = row.getUserValues();
			Map<String, Object> sourceValues = CRUtilities.translateToSourceValues(targetMap, targetValues);
			testDeleteQuery.fill(sourceValues, true);
			long count = testDeleteQuery.executeQueryCount();
			if (count <= 0) {
				logger.trace("Was Deleted");
				// mark as deleted
				changeToDeletedStatus.setObject("rowId", row.getRowID());
				changeToDeletedStatus.execute();
				deleteStats.incNumberOfRowsDeleted();
			} else {
				logger.trace("Still exists");
			}
			h2Conn.commit();

			if (logger.isDebugEnabled() && logTimer.elapsedTime() > LOG_FREQUENCY) {

				logger.debug("Extraction unit #" + extUnit.getId() + ".Deleting process. " + deleteStats.getStats());
				logTimer = TimerStats.startTimer();
			}
		}
		logger.debug("Extraction unit #" + extUnit.getId() + ". FINISH Deleting process. " + deleteStats.getStats());
	}

	/**
	 * This method takes the definition of the columns, and an open resultset to
	 * load the external row on memory.
	 * 
	 * @param supportedColumns
	 * @param rs
	 * @return An array that contains two maps of the same data. The first map
	 *         contains only the pk columns. The second row contains all rows
	 * @throws SQLException
	 * @throws TemporalConfigurationError
	 */
	private Map<String, Object>[] loadExternalRow(ArrayList<CRColumn> supportedColumns, ResultSet rs, boolean loadOnlyPK)
			throws SQLException, TemporalConfigurationError {
		HashMap<String, Object> rowOnExternalDB = new HashMap<String, Object>();
		HashMap<String, Object> pkWhere = new HashMap<String, Object>();

		for (CRColumn colDef : supportedColumns) {
			String sourceColumn = colDef.getSourceColumn();
			Object value = rs.getObject(sourceColumn);

			if (colDef.isPKMember()) {
				if (value == null) {
					throw new TemporalConfigurationError("Error processing " + extUnit.getId()
							+ ". The source column '" + sourceColumn
							+ "' contains NULL values. NULL values are not allowed on PK fields");
				}
				pkWhere.put(colDef.getTargetColumn(), value);

			}
			if (!loadOnlyPK) {
				rowOnExternalDB.put(colDef.getTargetColumn(), value);
			}

		}

		Map<String, Object>[] externalRow = new Map[2];
		externalRow[0] = pkWhere;
		externalRow[1] = rowOnExternalDB;
		return externalRow;
	}

	/**
	 * <p>
	 * The process of extraction data executes the SQL provided on the
	 * extraction unit and for every row will check if it needs to be updated or
	 * inserted on the local h2 database.
	 * </p>
	 * <p>
	 * The idea of the algorithm is to avoid making comparison on the Java
	 * level, and leave it to the H2 database. In addition, the CR enforce an
	 * index over the user declared PK, hence the queries should run pretty
	 * fast.
	 * </p>
	 * <p>
	 * For every row on the external Database the algorithm will make two
	 * queries:
	 * </p>
	 * <p>
	 * The first query will check if the row already exists by creating a WHERE
	 * clause with all possible columns. If the query returns a row it means
	 * that it was already imported and it didn't change on the external
	 * database.
	 * </p>
	 * <p>
	 * If the first query didn't return anything. The algorithm will attempt to
	 * execute a query with only the columns of the PK on the WHERE CLAUSE. If
	 * the row didn't exist then it means that we need to INSERT it. If the row
	 * already existed, then it means that it has changed and we need to UPDATE
	 * it,
	 * </p>
	 * 
	 * @param jdbcExternalconnection
	 * @param h2Conn
	 * @param supportedColumns
	 * @param tableDefinition
	 * @param pkColumns
	 * @param targetColumns
	 * @throws SQLException
	 * @throws InterruptedException
	 * @throws TemporalConfigurationError
	 */
	private ExtractionStatistics extractData(Connection jdbcExternalconnection, Connection h2Conn,
			ArrayList<CRColumn> supportedColumns, Map<String, H2Column> tableDefinition, Set<String> pkColumns,
			Set<String> targetColumns) throws SQLException, InterruptedException, TemporalConfigurationError {
		TimerStats tempTimer;// useful for keeping track of temporal executing
		TimerStats logTimer;// in order to avoid extensive logging the
							// extraction unit will log every LOG_FREQUENCY
		// ----------------------------------------
		// CHECK WHICH ROWS HAS BEEN INSERTED AND WHICH ONE ARE NEW
		// ----------------------------------------
		Statement stmt = jdbcExternalconnection.createStatement();
		ResultSet rs = stmt.executeQuery(extUnit.getSql());

		PreparedNamedStatement updateStmt = repositoryDAO.prepareUpdateStmt(h2Conn, extUnit.getId(), pkColumns,
				targetColumns);
		PreparedNamedStatement insertStmt = repositoryDAO.prepareInsertStmt(h2Conn, extUnit.getId());

		logger.trace("-----------------------------");
		logger.debug("Analyzing remote SQL query of extraction unit #" + extUnit.getId() + ". Extracting data...");
		logger.trace("-----------------------------");

		ExtractionStatistics stats = new ExtractionStatistics();
		stats.startTime();

		logTimer = TimerStats.startTimer();
		while (rs.next()) {
			stats.incNumberOfRowsAnalyzed();
			if (!alive) {
				throw new InterruptedException();
			}

			Map<String, Object>[] externalRowMaps = loadExternalRow(supportedColumns, rs, false);

			Map<String, Object> externalDataPKOnly = externalRowMaps[0];
			Map<String, Object> externalDataFull = externalRowMaps[1];
			String pkStr = "";
			String rowStr = "";
			if (logger.isTraceEnabled()) {
				pkStr = CRUtilities.toString(externalDataPKOnly);
				rowStr = CRUtilities.toString(externalDataFull);
			}
			// We will check if the same row already exists
			// If this row already exists on the temporal table then we
			// don't have to do anything
			tempTimer = TimerStats.startTimer();
			List<DynaRow> rows = repositoryDAO.find(h2Conn, tableDefinition, extUnit.getId(), externalDataFull);
			stats.addSt_FindingRowTime(tempTimer.elapsedTime());
			if (rows.size() <= 0) {
				// If the row doesn't exists it might be for two
				// possible reasons
				tempTimer = TimerStats.startTimer();
				rows = repositoryDAO.find(h2Conn, tableDefinition, extUnit.getId(), externalDataPKOnly);
				stats.addSt_FindingRowTimeOnPK(tempTimer.elapsedTime());
				if (rows.size() > 0) {
					// There was an old row and some of the data has
					// changed
					updateStmt.fillSetClause(externalDataFull);
					updateStmt.fillWhereClause(externalDataPKOnly);
					tempTimer = TimerStats.startTimer();
					updateStmt.execute();
					stats.addSt_UpdateTime(tempTimer.elapsedTime());
					stats.incNumberOfRowsUpdated();
					logger.trace("Updated.  " + pkStr);
				} else {
					// This is a new row
					// We need to insert the row
					insertStmt.fillForInsert(externalDataFull);
					tempTimer = TimerStats.startTimer();
					insertStmt.execute();
					stats.addSt_InsertTime(tempTimer.elapsedTime());
					stats.incNumberOfRowsInserted();
					logger.trace("Inserted. " + rowStr);

				}

			} else {
				stats.incNumberOfRowsDidntChange();
				logger.trace("Didn't change. " + pkStr);
			}

			tempTimer = TimerStats.startTimer();
			h2Conn.commit();
			stats.addSt_Commit(tempTimer.elapsedTime());

			if (logger.isDebugEnabled() && logTimer.elapsedTime() > LOG_FREQUENCY) {

				logger.debug("Extraction unit #" + extUnit.getId() + ". Extracting data " + stats.getGlobalStats());
				logger.debug("Extraction unit #" + extUnit.getId() + stats.getAvgExecutionTime());

				stats.resetStatsValues();
				logTimer = TimerStats.startTimer();

			}

		}
		stmt.close();
		logger.info("Extraction unit #" + extUnit.getId() + ". FINISH Extracting data " + stats.getGlobalStats());
		return stats;

	}

	private List<Map<String, String>> transformDataToRESTFormat(MFLoookupTableDefinition def, ArrayList<DynaRow> rows) {
		ArrayList<Map<String, String>> tranformedRows = new ArrayList<Map<String, String>>();
		for (DynaRow r : rows) {
			Map<String, Object> userValues = r.getUserValues();
			Map<String, String> tr = MFDataHelper.serializeValues(def, userValues);
			tranformedRows.add(tr);
		}

		return tranformedRows;
	}

	private void pushData() throws IOException, WebApiException, RestAuhtorizationException, TemporalConfigurationError,
			UnrecoverableConfigurationError, ConnectionException, InterruptedException {
		// iterate over the table
		// insert or update should be sent for every row
		Connection jdbcExternalconnection = null;
		Connection h2Conn = null;
		try {
			boolean isLoggedIn = false;
			boolean isDataToSent = false;
			try {
				jdbcExternalconnection = dbManager.open(extUnit.getConnectionId());
				Map<String, CRColumn> sourceColumnToClasses = server.obtainCRColumns(jdbcExternalconnection, extUnit);
				jdbcExternalconnection.close();
				jdbcExternalconnection = null;
				ArrayList<CRColumn> supportedColumns = server.determineSupportedColumns(sourceColumnToClasses, false);

				Set<String> pkColumns = CRUtilities.determineTargetPKColumns(supportedColumns);

				MFWebApiLookupFacade lookupFacade = new MFWebApiLookupFacade(restClient);
				MFLoookupTableDefinition def = null;
				h2Conn = repositoryDAO.open();

				PreparedNamedStatement changeRowStatusStmt = repositoryDAO.prepareUpdateStatusStmt(h2Conn,
						extUnit.getId());

				logger.trace("-----------------------------");
				logger.debug("Pushing new rows to " + restClient.getBaseURL() + " based on extraction unit #"
						+ extUnit.getId());
				logger.trace("-----------------------------");

				DynaResultSet rsOverInsertedRows = repositoryDAO
						.openCursorBasedOnStatus(
								h2Conn,
								extUnit.getId(),
								new DBTrackerDefinition.SYNCHRONIZATION_STATUS[] { DBTrackerDefinition.SYNCHRONIZATION_STATUS.INSERTED });

				long insertBatchSize = DEFAULT_INSERT_BATCH_SIZE;
				if (extUnit.getInsertBatchSize() != null) {
					insertBatchSize = extUnit.getInsertBatchSize();
				}
				ArrayList<DynaRow> rowsToSendList = new ArrayList<DynaRow>();
				long elapsedTime = 0;
				long numberOfInsertedRows = 0;
				long startTime, lastLogStamp;
				startTime = lastLogStamp = System.currentTimeMillis();
				while (rsOverInsertedRows.next()) {
					if (!alive) {
						throw new InterruptedException();
					}
					isDataToSent = true;
					if (!isLoggedIn) {
						// only login if there is data to sent
						restClient.login();
						isLoggedIn = true;
						// Obtain remotely the definition of the lookup table so
						// we are sure that we are posting to the expected
						// values
						def = lookupFacade.getLookupTableById(extUnit.getLookupId());
					}
					DynaRow row = rsOverInsertedRows.getRow();

					rowsToSendList.add(row);
					if (rowsToSendList.size() > insertBatchSize) {
						sendBatchToTheServer(h2Conn, lookupFacade, def, changeRowStatusStmt, rowsToSendList);
						numberOfInsertedRows += rowsToSendList.size();

						rowsToSendList.clear();
					}
					if (logger.isDebugEnabled() && (System.currentTimeMillis() - lastLogStamp) > LOG_FREQUENCY) {

						elapsedTime = ((System.currentTimeMillis() - startTime)) / 1000;
						logger.debug("Extraction unit #" + extUnit.getId() + ".Pushing NEW data. Elapsed time: "
								+ elapsedTime + " sec. , new rows pushed " + numberOfInsertedRows);
						lastLogStamp = System.currentTimeMillis();
					}
				}
				// need to send the remaining data
				if (rowsToSendList.size() > 0) {
					sendBatchToTheServer(h2Conn, lookupFacade, def, changeRowStatusStmt, rowsToSendList);
				}
				elapsedTime = ((System.currentTimeMillis() - startTime)) / 1000;
				logger.debug("Extraction unit #" + extUnit.getId() + ".FINISH Pushing NEW data. Elapsed time: "
						+ elapsedTime + " sec. , new rows pushed " + numberOfInsertedRows);

				logger.trace("-----------------------------");
				logger.debug("Pushing modifications to " + restClient.getBaseURL() + " based on extraction unit #"
						+ extUnit.getId());
				logger.trace("-----------------------------");

				DynaResultSet rsOverModifiedRows = repositoryDAO
						.openCursorBasedOnStatus(
								h2Conn,
								extUnit.getId(),
								new DBTrackerDefinition.SYNCHRONIZATION_STATUS[] { DBTrackerDefinition.SYNCHRONIZATION_STATUS.UPDATED });
				changeRowStatusStmt.setObject("status",
						DBTrackerDefinition.SYNCHRONIZATION_STATUS.SYNCHRONIZED.toString());
				long numberOfUpdatedRows = 0;

				startTime = lastLogStamp = System.currentTimeMillis();
				while (rsOverModifiedRows.next()) {
					if (!alive) {
						throw new InterruptedException();
					}
					isDataToSent = true;
					if (!isLoggedIn) {
						// only login if there is data to sent
						restClient.login();
						isLoggedIn = true;
						// Obtain remotely the definition of the lookup table so
						// we are sure that we are posting to the expected
						// values
						def = lookupFacade.getLookupTableById(extUnit.getLookupId());
					}
					DynaRow row = rsOverModifiedRows.getRow();
					Map<String, Object> userValues = row.getUserValues();
					Map<String, String> rowToSend = MFDataHelper.serializeValues(def, userValues);
					logger.trace("Pushing " + row);
					MFOperationResult result = lookupFacade.updateData(extUnit.getLookupId(), rowToSend);
					if (result.hasSucceeded()) {
						logger.trace("SUCCESS. pusing " + row);
						changeRowStatusStmt.setObject("rowId", row.getRowID());
						changeRowStatusStmt.execute();
						h2Conn.commit();
						numberOfUpdatedRows++;
					} else {
						// Since the updateData will update or insert we are
						// really not expecting to fail.
						// Note that this is a fail on the business layer. this
						// doesn't mean a fail on the connection (IOExeption)
						throw new CRUnexpectedException("received a fail pushing document " + row);
					}
					if (logger.isDebugEnabled() && (System.currentTimeMillis() - lastLogStamp) > LOG_FREQUENCY) {
						elapsedTime = ((System.currentTimeMillis() - startTime)) / 1000;
						logger.debug("Extraction unit #" + extUnit.getId() + ".Pushing modification. Elapsed time: "
								+ elapsedTime + " sec. , updates pushed " + numberOfUpdatedRows);
						lastLogStamp = System.currentTimeMillis();
					}
				}
				elapsedTime = ((System.currentTimeMillis() - startTime)) / 1000;
				logger.info("Extraction unit #" + extUnit.getId() + ". FINISH Pushing modification. Elapsed time: "
						+ elapsedTime + " sec. , updates pushed " + numberOfUpdatedRows);
				if (!isDataToSent) {
					logger.trace("There is no data to sent to the server ");
				}

				logger.trace("-----------------------------");
				logger.debug("Pushing deletes to " + restClient.getBaseURL() + " based on extraction unit #"
						+ extUnit.getId());
				logger.trace("-----------------------------");
				isDataToSent = false;
				long numberOfDeletedRows = 0;
				elapsedTime = 0;
				startTime = lastLogStamp = System.currentTimeMillis();
				PreparedNamedStatement changeToSYNCHDel = repositoryDAO
						.prepareUpdateStatusStmt(h2Conn, extUnit.getId());
				changeToSYNCHDel.setObject("status",
						DBTrackerDefinition.SYNCHRONIZATION_STATUS.SYNCH_DELETED.toString());
				DynaResultSet rsOverDeletes = repositoryDAO
						.openCursorBasedOnStatus(
								h2Conn,
								extUnit.getId(),
								pkColumns,
								new DBTrackerDefinition.SYNCHRONIZATION_STATUS[] { DBTrackerDefinition.SYNCHRONIZATION_STATUS.DELETED });
				while (rsOverDeletes.next()) {
					if (!alive) {
						throw new InterruptedException();
					}
					if (!isLoggedIn) {
						// only login if it haven't login yet
						restClient.login();
						isLoggedIn = true;
					}
					isDataToSent = true;
					DynaRow row = rsOverDeletes.getRow();
					// note that this map will only contains values of the PK
					// columns, since we have request only those column on the
					// last repositoryDAO
					// .openCursorBasedOnStatus
					Map<String, Object> userValuesOfPK = row.getUserValues();
					logger.trace("Deleting " + userValuesOfPK);
					ConditionalCriteria selector = CRUtilities.toCriteriaUsingEquals(userValuesOfPK);
					MFOperationResult result = lookupFacade.delete(extUnit.getLookupId(), selector);
					if (result.hasSucceeded()) {
						logger.trace("SUCESS deleting " + userValuesOfPK);
						changeToSYNCHDel.setObject("rowId", row.getRowID());
						changeToSYNCHDel.execute();
						h2Conn.commit();
						numberOfDeletedRows++;
					} else {
						throw new CRUnexpectedException("received a fail on deleting document " + row);
					}
					if (logger.isDebugEnabled() && (System.currentTimeMillis() - lastLogStamp) > LOG_FREQUENCY) {
						elapsedTime = ((System.currentTimeMillis() - startTime)) / 1000;
						logger.debug("Extraction unit #" + extUnit.getId() + ".Sending deleted data. Elapsed time: "
								+ elapsedTime + " sec. , rows deleted " + numberOfDeletedRows);
						lastLogStamp = System.currentTimeMillis();
					}

				}
				elapsedTime = ((System.currentTimeMillis() - startTime)) / 1000;
				logger.info("Extraction unit #" + extUnit.getId() + ".Sending deleted data. Elapsed time: "
						+ elapsedTime + " sec. , rows deleted " + numberOfDeletedRows);
				lastLogStamp = System.currentTimeMillis();
				if (!isDataToSent) {
					logger.trace("There is no delete to push");
				}
				// Deletes all rows that were successfully deleted on the remote
				// server
				int deletedRows = repositoryDAO
						.deleteRowsOnStatuses(
								h2Conn,
								extUnit.getId(),
								new DBTrackerDefinition.SYNCHRONIZATION_STATUS[] { DBTrackerDefinition.SYNCHRONIZATION_STATUS.SYNCH_DELETED });
				logger.trace("There were " + deletedRows + " deleted rows");
				h2Conn.commit();

			} finally {
				if (h2Conn != null) {
					h2Conn.close();
				}
				if (isLoggedIn) {
					restClient.logout();
				}

			}
		} catch (SQLException e) {
			// This is an unexpected exception since we should have everything
			// under control on our local database
			throw new CRUnexpectedException(e);
		}
	}

	private void sendBatchToTheServer(Connection h2Conn, MFWebApiLookupFacade lookupFacade, MFLoookupTableDefinition def,
			PreparedNamedStatement changeRowStatusStmt, ArrayList<DynaRow> rowsToSendList) throws IOException,
			RestAuhtorizationException, WebApiException, SQLException {
		// flush the data to the server
		List<Map<String, String>> rowsInRESTFormat = transformDataToRESTFormat(def, rowsToSendList);
		MFOperationResult result = lookupFacade.insertData(extUnit.getLookupId(), rowsInRESTFormat);
		if (result.getNumberOfAffectedRows() == rowsToSendList.size()) {
			// All rows were inserted so we need to mark them as
			// synchronized
			changeRowStatusStmt.setObject("status", DBTrackerDefinition.SYNCHRONIZATION_STATUS.SYNCHRONIZED.toString());
			logger.trace("SUCCESS inserting " + rowsToSendList.size() + " rows ");
		} else {
			// If there were some rows that couldn't be inserted
			// it might be due to a duplicate. Therefore, we
			// need to update them.
			// We assume that the update is what the user
			// desires. If he starts several connections to the
			// same lookup table, then the last incoming update
			// will prevail
			changeRowStatusStmt.setObject("status", DBTrackerDefinition.SYNCHRONIZATION_STATUS.UPDATED.toString());
			logger.trace("SUCCESS sending " + rowsToSendList.size()
					+ " rows, but some of them were already on the server. ");
		}
		for (DynaRow r : rowsToSendList) {
			changeRowStatusStmt.setObject("rowId", r.getRowID());
			changeRowStatusStmt.execute();
		}
		h2Conn.commit();
	}

	@Override
	public void run() {
		logger.info("Starting thread for extraction unit #" + extUnit.getId());
		long elapsedTime, timeStoSleep;
		long frequency = DEFAULT_FREQUENCY;
		if (extUnit.getFrequencyInSeconds() != null) {
			frequency = extUnit.getFrequencyInSeconds();
		}
		// convert the frequency to ms.
		frequency = frequency * 1000;
		Throwable unexpectedError = null;
		while (alive) {
			logger.debug("Synchronizing " + extUnit.getId());

			elapsedTime = System.currentTimeMillis();
			try {

				// Analyze the SQL and import in the local database the rows
				// The rows will be specially marked so the pushData method can
				// pick up them and send the modifications to the server
				localSynchronization();

				pushData();

			} catch (Throwable e) {
				// a configuration error might happen if something changed
				// on the source DB. We are only going to stop the server if
				// the user has marked the extraction unit this way. The same
				// applied to RestAuhtorizationException (the user might be
				// granted with the required authorization ).
				// Any other authorization will stop the server
				boolean shouldStop = true;
				if (e instanceof InterruptedException) {
					logger.debug("The thread of the extraction unit #" + extUnit.getId() + " was interrupted");

				} else if (e instanceof TemporalConfigurationError) {
					logger.debug("ConfigurationError on extraction unit " + extUnit.getId(), e);
					logger.error("ConfigurationError on extraction unit " + extUnit.getId() + ". Error:  "
							+ e.getMessage());
					shouldStop = extUnit.isStopOnError();
				} else if (e instanceof RestAuhtorizationException) {
					logger.debug("Check Remote authorizations.", e);
					logger.error("Check Remote authorizations. Details = " + e.getMessage());

					shouldStop = extUnit.isStopOnError();
				} else if (e instanceof ConnectionException) {
					logger.debug("Check Remote authorizations.", e);
					logger.error("Check Remote authorizations. Details = " + e.getMessage());
					shouldStop = extUnit.isStopOnError();
				} else {

					unexpectedError = e;
				}

				if (shouldStop) {
					alive = false;

				}

			}
			elapsedTime = System.currentTimeMillis() - elapsedTime;

			timeStoSleep = frequency - elapsedTime;
			if (timeStoSleep > 0) {
				// Wait for the remaining time until the user desired
				// frequency has been reached

				try {
					if (alive) {
						Thread.sleep(timeStoSleep);
					}

				} catch (InterruptedException e) {
					// do nothing, somebody has awake this thread. The thread
					// will check if status and act according to it (i.e.
					// active)
				}
			}

		}
		if (unexpectedError != null) {
			logger.fatal("Got an unepected exception on " + Thread.currentThread().getName(), unexpectedError);
			try {
				// stop the other threads on an unexpected situation
				server.shutdown();
			} catch (InterruptedException e) {

			}
		}
		logger.info("Stoping thread for extraction unit #" + extUnit.getId());
	}
}
