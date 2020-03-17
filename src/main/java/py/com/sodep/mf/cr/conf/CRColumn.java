package py.com.sodep.mf.cr.conf;

public class CRColumn {

	/**
	 * The name of the column on the SQL
	 */
	private String sourceColumn;
	/**
	 * The name of the column for the target lookup table
	 */
	private String targetColumn;
	/**
	 * The type of the column on the lookup table. A String representation of
	 * #MFField#FIELD_TYPE
	 */
	private String targetType;
	/**
	 * If this is set to true the system will check if the property has changed
	 * in order to send an update
	 */
	private boolean triggerUpdate;
	/**
	 * If set to true the column will be declared as PK and will be used to
	 * identify a row
	 */
	private boolean isPKMember;

	/**
	 * This field should not be assigned by the user. It is the value returned
	 * by the metaData.getColumnClassName(column) of the corresponding sql
	 * column
	 */
	private String javaClass;
	/**
	 * This field should not be assigned by the user. It is the value returned
	 * by the metaData.getPrecision(column) of the corresponding sql column
	 */
	private int length;

	public CRColumn() {

	}

	public CRColumn(String sourceColumn, String targetType) {
		this.sourceColumn = sourceColumn;
		this.targetType = targetType;
	}

	public CRColumn(String sourceColumn) {
		this(sourceColumn, null);
	}

	public String getSourceColumn() {
		return sourceColumn;
	}

	public void setSourceColumn(String sourceColumn) {
		this.sourceColumn = sourceColumn;
	}

	public String getTargetColumn() {
		if (targetColumn != null) {
			return targetColumn.toUpperCase();
		} else {
			return null;
		}

	}

	public void setTargetColumn(String targetColumn) {
		this.targetColumn = targetColumn;
	}

	public String getTargetType() {
		return targetType;
	}

	public void setTargetType(String targetType) {
		this.targetType = targetType;
	}

	public boolean isTriggerUpdate() {
		return triggerUpdate;
	}

	public void setTriggerUpdate(boolean triggerUpdate) {
		this.triggerUpdate = triggerUpdate;
	}

	public boolean isPKMember() {
		return isPKMember;
	}

	public void setPKMember(boolean isPKMember) {
		this.isPKMember = isPKMember;
	}

	public String getJavaClass() {
		return javaClass;
	}

	public void setJavaClass(String javaClass) {
		this.javaClass = javaClass;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (isPKMember ? 1231 : 1237);
		result = prime * result + ((javaClass == null) ? 0 : javaClass.hashCode());
		result = prime * result + length;
		result = prime * result + ((sourceColumn == null) ? 0 : sourceColumn.hashCode());
		result = prime * result + ((targetColumn == null) ? 0 : targetColumn.hashCode());
		result = prime * result + ((targetType == null) ? 0 : targetType.hashCode());
		result = prime * result + (triggerUpdate ? 1231 : 1237);
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
		if (!(obj instanceof CRColumn)) {
			return false;
		}
		CRColumn other = (CRColumn) obj;
		if (isPKMember != other.isPKMember) {
			return false;
		}
		if (javaClass == null) {
			if (other.javaClass != null) {
				return false;
			}
		} else if (!javaClass.equals(other.javaClass)) {
			return false;
		}
		if (length != other.length) {
			return false;
		}
		if (sourceColumn == null) {
			if (other.sourceColumn != null) {
				return false;
			}
		} else if (!sourceColumn.equals(other.sourceColumn)) {
			return false;
		}
		if (targetColumn == null) {
			if (other.targetColumn != null) {
				return false;
			}
		} else if (!targetColumn.equals(other.targetColumn)) {
			return false;
		}
		if (targetType == null) {
			if (other.targetType != null) {
				return false;
			}
		} else if (!targetType.equals(other.targetType)) {
			return false;
		}
		if (triggerUpdate != other.triggerUpdate) {
			return false;
		}
		return true;
	}

}
