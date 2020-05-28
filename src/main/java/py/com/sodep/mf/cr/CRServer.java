package py.com.sodep.mf.cr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import py.com.sodep.mf.cr.conf.CRColumn;
import py.com.sodep.mf.cr.conf.CRConnection;
import py.com.sodep.mf.cr.conf.CRExtractionUnit;
import py.com.sodep.mf.cr.conf.ConnectorDefinition;
import py.com.sodep.mf.cr.conf.DefinitionDoesntMatchException;
import py.com.sodep.mf.cr.conf.H2Column;
import py.com.sodep.mf.cr.conf.TemporalConfigurationError;
import py.com.sodep.mf.cr.conf.UnrecoverableConfigurationError;
import py.com.sodep.mf.cr.exception.CRUnexpectedException;
import py.com.sodep.mf.cr.exception.ConnectionException;
import py.com.sodep.mf.cr.internalDB.RepositoryDAO;
import py.com.sodep.mf.cr.webapi.MFWebApiLookupFacade;
import py.com.sodep.mf.cr.webapi.WebApiClient;
import py.com.sodep.mf.cr.webapi.exception.RestAuhtorizationException;
import py.com.sodep.mf.cr.webapi.exception.WebApiException;
import py.com.sodep.mf.exchange.MFField;
import py.com.sodep.mf.exchange.MFField.FIELD_TYPE;
import py.com.sodep.mf.exchange.MFLoookupTableDefinition;
import py.com.sodep.mf.exchange.objects.lookup.LookupTableDTO;
import py.com.sodep.mf.exchange.objects.lookup.LookupTableDefinitionException;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class CRServer implements SignalHandler {
	private static final Logger logger = LogManager.getLogger(CRServer.class);

	private final WebApiClient restClient;
	private final MFWebApiLookupFacade lookupRest;

	private RepositoryDAO repositoryDAO;

	private final Long applicationId;
	private MultiDBManager dbManager = new MultiDBManager();

	private CopyOnWriteArrayList<LookupPopulator> lookupThreads = new CopyOnWriteArrayList<LookupPopulator>();

	private Object startLock = new Object();
	private boolean started = false;

	public CRServer(WebApiClient restClient, String user, String pass, String filePath, Long applicationId)
			throws ClassNotFoundException, SQLException {
		this.restClient = restClient;
		this.lookupRest = new MFWebApiLookupFacade(restClient);
		this.repositoryDAO = new RepositoryDAO(filePath, user, pass);
		this.applicationId = applicationId;

		this.repositoryDAO.defineDB();
	}

	public WebApiClient buildRestClient() {
		return restClient.clone();
	}

	/**
	 * The number of threads initiated. If it was already started the method
	 * will return 0
	 * 
	 * @return
	 */
	public int start() {
		synchronized (startLock) {
			if (started) {
				return 0;
			}
			int countOfThreads = 0;
			for (LookupPopulator populator : lookupThreads) {
				countOfThreads = populator.start();
			}

			started = true;
			return countOfThreads;
		}
	}

	public void stop() throws InterruptedException {
		synchronized (startLock) {
			for (LookupPopulator populator : lookupThreads) {
				populator.stop();
			}
		}
		for (LookupPopulator populator : lookupThreads) {
			populator.join();
		}
	}

	public void releaseResources() {
		repositoryDAO.dispose();
	}

	private void addOrUpdateConnection(Connection conn, CRConnection crConn, boolean testCommunication)
			throws TemporalConfigurationError, ConnectionException {
		try {

			repositoryDAO.insertOrUpdateConnection(conn, crConn);
			dbManager.register(crConn, testCommunication);

		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	public void configure(ConnectorDefinition desDefinition) throws TemporalConfigurationError, ConnectionException,
			IOException, RestAuhtorizationException, LookupTableDefinitionException, WebApiException,
			UnrecoverableConfigurationError {
		logger.debug("Configuring Connector repository");
		List<CRConnection> connections = desDefinition.getConnections();
		configureConnection(connections);
		List<CRExtractionUnit> lookups = desDefinition.getExtractionUnits();

		restClient.login();
		try {
			if(lookups!=null)
			{
				for (CRExtractionUnit crExtractionUnit : lookups) {
					CRExtractionUnit savedExtUnit = configureLookup(crExtractionUnit);
					this.lookupThreads.add(new LookupPopulator(this, dbManager, savedExtUnit, repositoryDAO));
				}
			}
		} finally {
			restClient.logout();
		}
	}

	private void checkTargetColumnNames(MFLoookupTableDefinition def, String unitId) throws TemporalConfigurationError {
		List<MFField> fields = def.getFields();
		if (fields == null) {
			throw new TemporalConfigurationError("The lookup for the extraction unit #" + unitId
					+ " doesn't have any column");
		}
		for (MFField f : fields) {
			if (!f.getColumnName().matches("^\\w+$")) {
				throw new TemporalConfigurationError("The column " + f.getColumnName() + " is not a valid column name");
			}
		}
	}

	public CRExtractionUnit configureLookup(CRExtractionUnit extUnit) throws TemporalConfigurationError,
			ConnectionException, IOException, RestAuhtorizationException, LookupTableDefinitionException,
			WebApiException, UnrecoverableConfigurationError {
		if (extUnit.getId() == null) {
			throw new UnrecoverableConfigurationError("Can't create an extraction unit without the atribute 'id'");
		}
		if (extUnit.getConnectionId() == null) {
			throw new UnrecoverableConfigurationError("Can't create an extraction without connection");
		}
		// ---------------------------------------------------------------------------
		// The configuration of an extraction unit consist of the following
		// steps
		// STEP 0: Base on the SQL query and the extraction unit determine all
		// possible CRColumns
		// Step 1: Determine supported columns
		// Base on the result of step0 determine the columns that will form part
		// of the lookup definition
		// Step 2: Determine lookup definition.
		// Base on the supported columns determine the lookup table
		// definition
		// Step 3: Check (or create) remote definition.
		// Check that there is no lookup with the declared identifier. If the
		// lookup contains the same identifier then the current and the remote
		// definition must match
		// Step 4: Check (or create) extraction unit
		// Check that the extraction unit exist
		// Step 5: Check (or create) temporal table
		// Check that the temporal table exist or create it. If the definition
		// of the expected table doesn't match the already existing table, then
		// fail

		// Step 3,4, and 5 were designed idempotent on purpose, so the system
		// can recover on crashes performed on any of these steps.

		// ---------------------------------------------------------------------------
		// //STEP1: Determine supported columns
		String connectionId = extUnit.getConnectionId();
		if (!checkCRUnitName(extUnit.getId())) {
			throw new TemporalConfigurationError("The lookup identifier '" + extUnit.getId() + "' is not valid");
		}

		// The setter removes the trailing semicolon, called here because
		// XStream does not use setters/getters
		extUnit.setSql(extUnit.getSql());

		extUnit.setId(extUnit.getId().toUpperCase());
		logger.debug("Configuring extraction unit " + extUnit.getId());
		Connection jdbcExternalconnection = null;
		Map<String, CRColumn> sourceColumnToClasses = null;
		try {
			jdbcExternalconnection = dbManager.open(extUnit.getConnectionId());

			// Obtain the Lookup Definition from the Extraction Unit

			// STEP 0: Determine possible CRColumns
			sourceColumnToClasses = obtainCRColumns(jdbcExternalconnection, extUnit);

		} finally {
			if (jdbcExternalconnection != null) {
				try {
					jdbcExternalconnection.close();
				} catch (SQLException e) {
					logger.debug("Couldn't close connection " + connectionId, e);
					logger.warn("Couldn't close connection " + connectionId);
				}
			}
		}
		// STEP 1: Determine supported columns
		ArrayList<CRColumn> supportedColumns = determineSupportedColumns(sourceColumnToClasses, true);

		// The method mapFromTargetColumn will throw an exception if there are
		// duplicate column base on their target (case insensitive)
		CRUtilities.mapFromTargetColumn(supportedColumns);
		boolean hasPK = false;
		for (int i = 0; i < supportedColumns.size() && !hasPK; i++) {
			CRColumn col = supportedColumns.get(i);
			if (col.isPKMember()) {
				hasPK = true;
			}
		}
		if (!hasPK) {
			throw new UnrecoverableConfigurationError("The extraction unit #" + extUnit.getId()
					+ " Doesn't have any Primary Key. It need at least one primary key column");
		}
		// STEP 2: Determine lookup definition.
		MFLoookupTableDefinition currentDefinition = createLookupDefinition(extUnit, supportedColumns);
		checkTargetColumnNames(currentDefinition, extUnit.getId());
		// STEP 3: Define or get the remote lookup Table
		MFLoookupTableDefinition remoteLookup = checkRemoteDefinition(extUnit, currentDefinition);
		if (remoteLookup == null) {
			// This lookup table doesn't exist on the remote server, so we
			// need to create it
			remoteLookup = lookupRest.createLookupTable(currentDefinition);
			logger.debug("Remote lookup table created with id #" + remoteLookup.getInfo().getPk());
		}
		// STEP 4:Check or create the extraction unit (insert a row on the table
		// for extraction unit)

		extUnit.setLookupId(remoteLookup.getInfo().getPk());
		CRExtractionUnit savedUnit = insertOrUpdateExtractionUnit(extUnit, remoteLookup);

		// STEP 5: Define the temporal table
		checkOrDefineTemporalLookupTable(extUnit.getId(), supportedColumns);
		logger.info("SUCCESS. The extraction unit " + extUnit.getId() + " is register");
		return savedUnit;
	}

	public void checkOrDefineTemporalLookupTable(String extUnitId, ArrayList<CRColumn> supportedColumns)
			throws TemporalConfigurationError {
		try {
			Map<String, H2Column> tableDefinition = repositoryDAO.getTableDefinition(extUnitId);
			if (tableDefinition == null) {
				logger.debug("Defining temporal data table for " + extUnitId);
				repositoryDAO.createTableDefinitionForLookup(extUnitId, supportedColumns);
				logger.debug("SUCCESS. temporal table for " + extUnitId + " was created");
				repositoryDAO.createIndexForLookup(extUnitId, supportedColumns, true);
				logger.debug("SUCCESS. Index for lookup table " + extUnitId + " created ");
			} else {
				// The temporal lookup table already exist so we need to check
				// if the definition matched the expected columns
				logger.debug("Checking definition of temporal table for " + extUnitId);
				checkDefinition(extUnitId, supportedColumns, tableDefinition);
				logger.debug("SUCCESS. definition of the temporal table for " + extUnitId
						+ " matched the current SQL data");

			}

		} catch (SQLException e) {
			// We can't recover from this, so we treat it as an unexpected
			// exception. This is an SQL exception during the creation of the
			// temporal lookup table
			throw new RuntimeException(e);
		}
	}

	public static void checkDefinition(String extUnitId, List<CRColumn> supportedColumns,
			Map<String, H2Column> tableDefinition) throws TemporalConfigurationError {
		for (CRColumn crColumn : supportedColumns) {
			H2Column h2Column = tableDefinition.get(crColumn.getTargetColumn());
			if (h2Column == null) {
				throw new CRUnexpectedException("The temporal table for '" + extUnitId + "' doesn't have the column "
						+ crColumn.getTargetColumn());
			}
			String expectedType = JDBCToMFHelper.jdbcToH2(crColumn.getJavaClass(), crColumn.getLength());
			if (expectedType.startsWith("VARCHAR")) {
				if (!h2Column.getH2TypeName().equals("VARCHAR") || h2Column.getLength() != crColumn.getLength()) {
					throw new TemporalConfigurationError("Problem on " + extUnitId + " column is of type "
							+ h2Column.getH2TypeName() + " length = " + h2Column.getLength() + ", but " + expectedType
							+ " was expected");
				}
			} else {
				if (!h2Column.getH2TypeName().equals(expectedType)) {
					throw new TemporalConfigurationError("Problem on " + extUnitId + " column is of type "
							+ h2Column.getH2TypeName() + " but " + expectedType + " was expected");
				}
			}

		}
	}

	public static boolean checkCRUnitName(String identifier) {
		return identifier.matches("^\\w+$");
	}

	private CRExtractionUnit insertOrUpdateExtractionUnit(CRExtractionUnit extUnit,
			MFLoookupTableDefinition remoteLookup) {
		try {
			// Insert or get the local extraction unit (an extraction unit match
			// a remote lookup table)
			CRExtractionUnit savedUnit = repositoryDAO.getExtractionUnit(extUnit.getId());
			if (savedUnit == null) {

				// This is a Loookup that doesn't exist locally
				repositoryDAO.insertExtractionUnit(extUnit);
				logger.debug("Extraction unit #" + extUnit.getId() + " for lookup table #" + extUnit.getLookupId()
						+ " was inserted");

			} else {
				logger.debug("Extraction unit was already registered ");
				logger.debug("Updating extaction unit #" + extUnit.getId());
				repositoryDAO.updateExtractionUnit(extUnit);
				logger.debug("SUCCESS. The extraction unit " + extUnit.getId()
						+ " was updated to the latest definition");
			}
			return extUnit;
			// check or create the temporal table for the lookup table
		} catch (SQLException e) {
			// We handle this as unexpected exception since there is nothing
			// produced by the user
			// This exception might happen from data inserted in the extraction
			// unit

			throw new RuntimeException(e);
		}
	}

	/**
	 * From all possible columns detect which of them have unsupported columns
	 * and return a list containing just the supported columns
	 * 
	 * @param sourceColumnToClasses
	 * @param failOnUnsupportedColumns
	 *            if this is true and there are unsupported columns then a
	 *            {@link TemporalConfigurationError} will be thrown
	 * @return
	 * @throws TemporalConfigurationError
	 */
	ArrayList<CRColumn> determineSupportedColumns(Map<String, CRColumn> sourceColumnToClasses,
			boolean failOnUnsupportedColumns) throws UnrecoverableConfigurationError {
		ArrayList<CRColumn> supportedColumns = new ArrayList<CRColumn>();
		ArrayList<CRColumn> unsupportedColumns = new ArrayList<CRColumn>();

		Set<String> columns = sourceColumnToClasses.keySet();
		for (String col : columns) {
			CRColumn column = sourceColumnToClasses.get(col);
			if (column.getTargetType() == null) {
				logger.debug("The column " + column.getSourceColumn() + " data type is unsupported. Data Type = "
						+ column.getJavaClass());
				unsupportedColumns.add(column);
			} else {
				supportedColumns.add(column);
			}
		}
		if (unsupportedColumns.size() > 0 && failOnUnsupportedColumns) {
			throw new UnrecoverableConfigurationError("Unsupported data types on SQL "
					+ JDBCToMFHelper.unsupportedPlain(unsupportedColumns));
		}
		return supportedColumns;
	}

	private MFLoookupTableDefinition checkRemoteDefinition(CRExtractionUnit extUnit,
			MFLoookupTableDefinition currentDefinition) throws IOException, WebApiException,
			RestAuhtorizationException, TemporalConfigurationError {
		MFLoookupTableDefinition previousRemoteLookup = obtainRemoteLookup(extUnit.getId());
		if (previousRemoteLookup != null) {
			logger.debug("There is already a remote lookup identified with #" + extUnit.getId());
			logger.debug("Checking if the current definition match the remote definition");
			List<MFField> currentFields = currentDefinition.getFields();
			List<MFField> remoteFields = previousRemoteLookup.getFields();
			if (!currentFields.equals(remoteFields)) {
				throw new DefinitionDoesntMatchException(previousRemoteLookup, currentDefinition, extUnit.getId());
			} else {
				logger.debug("SUCCESS. Remote definition successfully matched the current definition (based on the SQL query)");
			}
		}
		return previousRemoteLookup;
	}

	/**
	 * Get a remote lookup table that was declared with the given identifier. If
	 * no lookup table exist then null will be returned
	 * 
	 * @param extUnit
	 * @return
	 * @throws IOException
	 * @throws WebApiException
	 * @throws RestAuhtorizationException
	 */
	private MFLoookupTableDefinition obtainRemoteLookup(String identifier) throws IOException, WebApiException,
			RestAuhtorizationException {
		MFLoookupTableDefinition previousRemoteLookup = null;
		List<LookupTableDTO> remoteLookups = lookupRest.listAll(applicationId, identifier);
		if (remoteLookups.size() > 0) {
			// There is already a lookup with this identifier
			previousRemoteLookup = lookupRest.getLookupTableById(remoteLookups.get(0).getPk());
		}
		return previousRemoteLookup;
	}

	MFLoookupTableDefinition createLookupDefinition(CRExtractionUnit extUnit, ArrayList<CRColumn> supportedColumns) {
		MFLoookupTableDefinition def = new MFLoookupTableDefinition();
		List<MFField> mfFields = JDBCToMFHelper.createLookupDefinition(supportedColumns);

		def.setFields(mfFields);
		LookupTableDTO dto = new LookupTableDTO();
		dto.setApplicationId(this.applicationId);
		dto.setIdentifier(extUnit.getId());
		dto.setAcceptRESTDMLs(true);
		dto.setName(extUnit.getDescription());
		def.setInfo(dto);
		return def;
	}

	public static List<CRColumn> columnsForExtractionUnit(CRConnection connection, CRExtractionUnit extractionUnit)
			throws TemporalConfigurationError, ConnectionException {
		List<CRColumn> columns = new ArrayList<CRColumn>();
		MultiDBManager dbManager = new MultiDBManager();
		dbManager.register(connection, true);

		Connection dbConnection = null;
		dbConnection = dbManager.open(connection.getId());
		
		Map<String, CRColumn> columnMap = CRServer.obtainCRColumns(dbConnection, extractionUnit);
		columns.addAll(columnMap.values());

		return columns;
	}

	/**
	 * This method maps the columns of the SQL to CRColumn. The user might
	 * define its own CRColumn in the extraction unit, if there is a CRUnit
	 * defined then those value should be used, otherwise, default values will
	 * be assigned. Target columns are case insensitive (they will be converted
	 * to upper case for convenience)
	 * 
	 * @param extUnit
	 * @param metaData
	 * @return
	 * @throws SQLException
	 * @throws TemporalConfigurationError
	 */
	 static Map<String, CRColumn> obtainCRColumns(Connection conn, CRExtractionUnit extUnit)
			throws TemporalConfigurationError {
		try {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(extUnit.getSql());
			ResultSetMetaData metaData = rs.getMetaData();
			Map<String, CRColumn> userDefinedColumns = extUnit.mapFromSourceColumnName();
			int columnCount = metaData.getColumnCount();
			HashMap<String, CRColumn> columnNameToClass = new HashMap<String, CRColumn>();
			for (int column = 1; column <= columnCount; column++) {
				String sourceColumn = metaData.getColumnLabel(column);
				int precision = metaData.getPrecision(column);
				Class<?> javaChoosenClass = null;

				try {
					javaChoosenClass = Class.forName(metaData.getColumnClassName(column));
				} catch (ClassNotFoundException e) {
					// This shouldn't happen since this is returning data types
					// from java.sql
					throw new RuntimeException(e);
				}

				CRColumn userDefinedColumn = userDefinedColumns.get(sourceColumn);

				if (userDefinedColumn != null && userDefinedColumn.getTargetType() != null) {
					// If the user has specified a target type we need to check
					// if
					// it corresponds with the metadata returned from the JDBC
					// driver

					if (userDefinedColumn.getTargetType().equals(FIELD_TYPE.NUMBER.toString())) {
						if (!Number.class.isAssignableFrom(javaChoosenClass)) {
							throw new TemporalConfigurationError("Error on extraction unit #" + extUnit.getId()
									+ ". The column #" + sourceColumn
									+ " was declared of type NUMBER but it can't be cast to Number. The JDBC Type is "
									+ javaChoosenClass);
						}
						// The user specified that he wants the data as
						// number
						// We will use the same data type returned from the
						// source JDBC
					} else if (userDefinedColumn.getTargetType().equals(FIELD_TYPE.BOOLEAN.toString())) {
						if (!Boolean.class.isAssignableFrom(javaChoosenClass)) {
							throw new TemporalConfigurationError(
									"Error on extraction unit #"
											+ extUnit.getId()
											+ ". The column #"
											+ sourceColumn
											+ " was declared of type Boolean but it can't be cast to a Boolen. The JDBC Type is "
											+ javaChoosenClass);
						}
					} else if (userDefinedColumn.getTargetType().equals(FIELD_TYPE.DATE.toString())) {
						if (!Date.class.isAssignableFrom(javaChoosenClass)
								&& !Timestamp.class.isAssignableFrom(javaChoosenClass)) {
							throw new TemporalConfigurationError(
									"Error on extraction unit #"
											+ extUnit.getId()
											+ ". The column #"
											+ sourceColumn
											+ " was declared of type Date but it can't be cast neither to a Date nor to a Timestamp. The JDBC Type is "
											+ javaChoosenClass);
						}

					} else if (userDefinedColumn.getTargetType().equals(FIELD_TYPE.STRING.toString())) {
						if (!String.class.isAssignableFrom(javaChoosenClass)) {
							throw new TemporalConfigurationError(
									"Error on extraction unit #"
											+ extUnit.getId()
											+ ". The column #"
											+ sourceColumn
											+ " was declared of type String but it can't be cast to a String. The JDBC Type is "
											+ javaChoosenClass);
						}
					} else {
						// check that the string entered on target type is a
						// valid
						// one
						try {
							FIELD_TYPE targetType = FIELD_TYPE.valueOf(userDefinedColumn.getTargetType());
						} catch (IllegalArgumentException e) {
							throw new TemporalConfigurationError("Unsupported targetType "
									+ userDefinedColumn.getTargetType());
						}
					}

				} else {
					if (userDefinedColumn == null) {
						// The user didn't configure the column, so we need to
						// assign the default values
						userDefinedColumn = new CRColumn();
						userDefinedColumn.setSourceColumn(sourceColumn);
						userDefinedColumn.setPKMember(false);
						userDefinedColumn.setTriggerUpdate(true);

					}
				}

				if (userDefinedColumn.getTargetColumn() == null) {
					// by default the target column has the same name as the
					// source
					// column
					userDefinedColumn.setTargetColumn(sourceColumn);
				}
				if (userDefinedColumn.getTargetType() == null) {
					userDefinedColumn.setTargetType(JDBCToMFHelper.jdbcTypeToMF(javaChoosenClass).toString());
				}

				userDefinedColumn.setLength(precision);
				userDefinedColumn.setJavaClass(javaChoosenClass.getName());
				columnNameToClass.put(sourceColumn.toUpperCase(), userDefinedColumn);
			}
			return columnNameToClass;
		} catch (SQLException e) {
			// This is a configuration error since it probably means that the
			// user enter something wrong on the SQL query
			throw new TemporalConfigurationError("SQL Error configuring extraction unit  #" + extUnit.getId()
					+ ". SQL Error " + e.getMessage(), e);
		}
	}

	private void configureConnection(List<CRConnection> connections) throws ConnectionException,
			TemporalConfigurationError {

		Connection conn = null;

		try {
			conn = repositoryDAO.open();
			
			if(connections!=null)
			{
				for (CRConnection crConnection : connections) {
					addOrUpdateConnection(conn, crConnection, true);
				}
			}

			conn.commit();
		} catch (TemporalConfigurationError e) {
			if (conn != null) {
				try {
					conn.rollback();
				} catch (SQLException e1) {
					logger.error("Error during roolback");
					logger.debug("Error during roolback", e1);
					throw new TemporalConfigurationError(e1.getMessage());
				}
			}
			throw e;
		} catch (SQLException e) {

			try {
				logger.error("SQLException configurint CR Server");
				logger.debug("SQLException configurint CR Server", e);
				if (conn != null) {
					conn.rollback();
				}
				// There shouldn't be any SQL problems, so I consider this a
				// unexpected error
				throw new RuntimeException(e);
			} catch (SQLException e1) {
				logger.error("Error during roolback");
				logger.debug("Error during roolback", e1);
				throw new TemporalConfigurationError(e1.getMessage());
			}
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {

				}
			}
		}
	}

	public void shutdown() throws InterruptedException {
		logger.debug("Shutting down server");
		stop();
		releaseResources();
		logger.info("CR Server gracefully shut down");
	}

	@Override
	public void handle(Signal sig) {
		// TODO Auto-generated method stub

	}
}
