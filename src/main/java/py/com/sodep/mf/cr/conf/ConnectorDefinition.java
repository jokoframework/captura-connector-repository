package py.com.sodep.mf.cr.conf;

import java.util.List;
import java.util.ListIterator;

import org.apache.commons.lang3.StringUtils;

public class ConnectorDefinition {
	private List<CRConnection> connections;
	private List<CRExtractionUnit> extractionUnits;
	private String sourceFile;

	public List<CRConnection> getConnections() {
		return connections;
	}

	public void setConnections(List<CRConnection> connections) {
		this.connections = connections;
	}

	public List<CRExtractionUnit> getExtractionUnits() {
		return extractionUnits;
	}

	public void setExtractionUnits(List<CRExtractionUnit> lookups) {
		this.extractionUnits = lookups;
	}
	
	public void addConnection(CRConnection connection) {
		this.connections.add(connection);
	}
	
	public void addExtractionUnit(CRExtractionUnit eu) {
		this.extractionUnits.add(eu);
	}

	public void updateConnection(CRConnection oldConnection, CRConnection newConnection) {
		ListIterator<CRConnection> iter = this.connections.listIterator();
		String id = oldConnection.getId();

		while (iter.hasNext()) {
			CRConnection currentConnection = iter.next();

			if (id.equals(currentConnection.getId())) {
				// Leave the password unchanged
				if (StringUtils.isEmpty(newConnection.getPass())) {
					newConnection.setPass(currentConnection.getPass());
				}

				iter.remove();
				iter.add(newConnection);
			}
		}

		// If the connection id changed we need to update the extraction units
		// as well
		if (!newConnection.getId().equals(oldConnection.getId())) {
			for (CRExtractionUnit eu : this.extractionUnits) {
				if (eu.getConnectionId().equals(oldConnection.getId())) {
					eu.setConnectionId(newConnection.getId());
				}
			}
		}
	}

	public void updateExtractionUnit(CRExtractionUnit oldEU, CRExtractionUnit newEU) {
		ListIterator<CRExtractionUnit> iter = this.extractionUnits.listIterator();
		String id = oldEU.getId();
		
		while (iter.hasNext()) {
			CRExtractionUnit currentEU = iter.next();
			if (id.equals(currentEU.getId())) {
				//newEU.setColumns(currentEU.getColumns());
				iter.remove();
				iter.add(newEU);
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((connections == null) ? 0 : connections.hashCode());
		result = prime * result + ((extractionUnits == null) ? 0 : extractionUnits.hashCode());
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
		if (!(obj instanceof ConnectorDefinition)) {
			return false;
		}
		ConnectorDefinition other = (ConnectorDefinition) obj;
		if (connections == null) {
			if (other.connections != null) {
				return false;
			}
		} else if (!connections.equals(other.connections)) {
			return false;
		}
		if (extractionUnits == null) {
			if (other.extractionUnits != null) {
				return false;
			}
		} else if (!extractionUnits.equals(other.extractionUnits)) {
			return false;
		}
		return true;
	}

	public String getSourceFile() {
		return sourceFile;
	}

	public void setSourceFile(String sourceFile) {
		this.sourceFile = sourceFile;
	}

}
