package py.com.sodep.mf.cr.conf;

import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import py.com.sodep.mf.cr.CRUtilities;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CRExtractionUnit {

	private String id;
	private String connectionId;
	private String sql;
	private String description;
	private List<CRColumn> columns;
	private boolean active;
	private boolean stopOnError;

	private Long lookupId;

	private Integer frequencyInSeconds;

	private Integer insertBatchSize;

	public CRExtractionUnit() {

	}

	public CRExtractionUnit(String id, String connectionId) {
		this.id = id;
		this.connectionId = connectionId;
	}
	
	@JsonProperty("label")
	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		// Having a semicolon at the end of the statement causes an error in
		// Oracle
		if (sql.endsWith(";")) {
			sql = sql.substring(0, sql.length() - 1);
		}

		this.sql = sql;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public List<CRColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<CRColumn> columns) {
		this.columns = columns;
	}

	public boolean isActive() {
		return active;
	}

	public void setActive(boolean active) {
		this.active = active;
	}

	public Long getLookupId() {
		return lookupId;
	}

	public void setLookupId(Long lookupId) {
		this.lookupId = lookupId;
	}

	public boolean isStopOnError() {
		return stopOnError;
	}

	public void setStopOnError(boolean failIfUnsupportedColumn) {
		this.stopOnError = failIfUnsupportedColumn;
	}

	public Map<String, CRColumn> mapFromSourceColumnName() throws TemporalConfigurationError {
		return CRUtilities.mapFromSourceColumnName(columns);
	}

	public Integer getInsertBatchSize() {
		return insertBatchSize;
	}

	public void setInsertBatchSize(Integer insertBatchSize) {
		this.insertBatchSize = insertBatchSize;
	}

	public Integer getFrequencyInSeconds() {
		return frequencyInSeconds;
	}

	public void setFrequencyInSeconds(Integer frequencyInSeconds) {
		this.frequencyInSeconds = frequencyInSeconds;
	}

	public boolean hasEmptyFields() {
		return StringUtils.isEmpty(id) || StringUtils.isEmpty(connectionId) || StringUtils.isEmpty(sql)
				|| StringUtils.isEmpty(description) || frequencyInSeconds == null || insertBatchSize == null;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (active ? 1231 : 1237);
		result = prime * result + ((columns == null) ? 0 : columns.hashCode());
		result = prime * result + ((connectionId == null) ? 0 : connectionId.hashCode());
		result = prime * result + ((description == null) ? 0 : description.hashCode());
		result = prime * result + ((frequencyInSeconds == null) ? 0 : frequencyInSeconds.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((insertBatchSize == null) ? 0 : insertBatchSize.hashCode());
		result = prime * result + ((lookupId == null) ? 0 : lookupId.hashCode());
		result = prime * result + ((sql == null) ? 0 : sql.hashCode());
		result = prime * result + (stopOnError ? 1231 : 1237);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (!(obj instanceof CRExtractionUnit)) {
			return false;
		}
		CRExtractionUnit other = (CRExtractionUnit) obj;
		if (active != other.active) {
			return false;
		}
		if (columns == null) {
			if (other.columns != null) {
				return false;
			}
		} else if (!columns.equals(other.columns)) {
			return false;
		}
		if (connectionId == null) {
			if (other.connectionId != null) {
				return false;
			}
		} else if (!connectionId.equals(other.connectionId)) {
			return false;
		}
		if (description == null) {
			if (other.description != null) {
				return false;
			}
		} else if (!description.equals(other.description)) {
			return false;
		}
		if (frequencyInSeconds == null) {
			if (other.frequencyInSeconds != null) {
				return false;
			}
		} else if (!frequencyInSeconds.equals(other.frequencyInSeconds)) {
			return false;
		}
		if (id == null) {
			if (other.id != null) {
				return false;
			}
		} else if (!id.equals(other.id)) {
			return false;
		}
		if (insertBatchSize == null) {
			if (other.insertBatchSize != null) {
				return false;
			}
		} else if (!insertBatchSize.equals(other.insertBatchSize)) {
			return false;
		}
		if (lookupId == null) {
			if (other.lookupId != null) {
				return false;
			}
		} else if (!lookupId.equals(other.lookupId)) {
			return false;
		}
		if (sql == null) {
			if (other.sql != null) {
				return false;
			}
		} else if (!sql.equals(other.sql)) {
			return false;
		}
		if (stopOnError != other.stopOnError) {
			return false;
		}
		return true;
	}

}
