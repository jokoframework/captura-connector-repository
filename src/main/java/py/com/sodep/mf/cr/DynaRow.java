package py.com.sodep.mf.cr;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import py.com.sodep.mf.cr.internalDB.DBTrackerDefinition;

/**
 * The representation of the data imported into the H2 table. Metadata can be
 * acceded directly by handy methods such as: {@link #getRowID()},
 * {@link #getLastModification()} . User data are available on a map
 * {@link #getUserValues()}
 * 
 * 
 * @author danicricco
 * 
 */
public class DynaRow {

	private Map<String, Object> userValues;
	private Long rowID;
	private DBTrackerDefinition.SYNCHRONIZATION_STATUS synchStatus;
	private Timestamp lastModification;

	public DynaRow() {

	}

	public Object getValue(String column) {
		return userValues.get(column);
	}

	public DynaRow(HashMap<String, Object> userData) {
		this.userValues = userData;
	}

	public Map<String, Object> getUserValues() {
		return userValues;
	}

	public void setUserValues(Map<String, Object> userValues) {
		this.userValues = userValues;
	}

	public Long getRowID() {
		return rowID;
	}

	public void setRowID(Long rowID) {
		this.rowID = rowID;
	}

	public DBTrackerDefinition.SYNCHRONIZATION_STATUS getSynchStatus() {
		return synchStatus;
	}

	public void setSynchStatus(DBTrackerDefinition.SYNCHRONIZATION_STATUS synchStatus) {
		this.synchStatus = synchStatus;
	}

	public Timestamp getLastModification() {
		return lastModification;
	}

	public void setLastModification(Timestamp lastModification) {
		this.lastModification = lastModification;
	}

	public String toString() {
		StringBuffer buff = new StringBuffer();
		buff.append(rowID + " [ ");
		Map<String, Object> map = getUserValues();
		Set<String> keys = map.keySet();
		Iterator<String> it = keys.iterator();
		while (it.hasNext()) {
			String col = it.next();
			Object value = map.get(col);
			buff.append(value);
			if (it.hasNext()) {
				buff.append(",");
			}
		}
		buff.append(" ] ");
		return buff.toString();
	}
}
