package py.com.sodep.mf.cr.internalDB;

public class DBTrackerDefinition {

	public static final String LOOKUP_SCHEMA = "LOOKUPS";
	public static final String TABLE_EXTRACTION_UNIT = "EXTRACTION_UNIT";
	public static final String TABLE_CONNECTION = "CONNECTIONS";

	public static final String CREATE_SCHEMA_LOOKUPS = "CREATE SCHEMA IF NOT EXISTS " + LOOKUP_SCHEMA;
	public static final String CREATE_EXTRACTION_UNIT = "CREATE TABLE IF NOT EXISTS " + TABLE_EXTRACTION_UNIT
			+ "(ID VARCHAR(255) PRIMARY KEY,LOOKUP_ID BIGINT,DEFINITION CLOB,ACTIVE BOOLEAN);";

	public static final String CREATE_TABLE_CONNECTION = "CREATE TABLE IF NOT EXISTS "
			+ TABLE_CONNECTION
			+ "(ID VARCHAR(255) PRIMARY KEY, URL VARCHAR(255),USER VARCHAR(255),PASSWORD VARCHAR(255),DRIVER VARCHAR(255));";

	public static enum SYNCHRONIZATION_STATUS {
		INSERTED, UPDATED, DELETED, SYNCHRONIZED, SYNCH_DELETED
	};
}
