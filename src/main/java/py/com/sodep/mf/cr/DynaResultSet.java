package py.com.sodep.mf.cr;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;

import py.com.sodep.mf.cr.internalDB.DBTrackerDefinition.SYNCHRONIZATION_STATUS;
import py.com.sodep.mf.cr.internalDB.RepositoryDAO;

/**
 * This is a wrapper over the {@link ResultSet} class that allows to return a
 * {@link DynaRow} for every row
 * 
 * @author danicricco
 * 
 */
public class DynaResultSet {

	private final Set<String> userColumns;

	private final ResultSet rs;

	public DynaResultSet(ResultSet rs, Set<String> userColumns) {
		this.rs = rs;
		this.userColumns = userColumns;
	}

	public boolean next() throws SQLException {
		return rs.next();
	}

	public DynaRow getRow() throws SQLException {
		HashMap<String, Object> userData = new HashMap<String, Object>();
		DynaRow row = new DynaRow(userData);

		Iterator<String> it = userColumns.iterator();
		// put on the MAP the user defined columns
		while (it.hasNext()) {
			String param = it.next();
			Object value = rs.getObject(RepositoryDAO.USER_COLUMN_PREFIX + param.toUpperCase());
			userData.put(param, value);
		}
		long rowID = rs.getLong(RepositoryDAO.META_COLUMN_PREFIX + "ROWID");
		String statusStr = rs.getString(RepositoryDAO.META_COLUMN_PREFIX + "STATUS");
		Timestamp lastChange = rs.getTimestamp(RepositoryDAO.META_COLUMN_PREFIX + "LAST_CHANGE");
		row.setLastModification(lastChange);
		row.setSynchStatus(SYNCHRONIZATION_STATUS.valueOf(statusStr));
		row.setRowID(rowID);
		return row;
	}
	
	
}
