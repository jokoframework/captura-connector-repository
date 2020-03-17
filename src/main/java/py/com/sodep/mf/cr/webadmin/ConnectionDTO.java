package py.com.sodep.mf.cr.webadmin;

import java.util.ArrayList;
import java.util.List;

import py.com.sodep.mf.cr.conf.CRConnection;
import py.com.sodep.mf.cr.conf.CRExtractionUnit;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConnectionDTO {
	private CRConnection connection;
	
	private String label;
	private String id;
	
	private List<CRExtractionUnit> extractionUnits = new ArrayList<CRExtractionUnit>();
	
	public void addExtractionUnit(CRExtractionUnit eu) {
		this.extractionUnits.add(eu);
	}

	public CRConnection getConnection() {
		return connection;
	}

	public void setConnection(CRConnection connection) {
		this.connection = connection;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	@JsonProperty("children")
	public List<CRExtractionUnit> getExtractionUnits() {
		return extractionUnits;
	}

	public void setExtractionUnits(List<CRExtractionUnit> children) {
		this.extractionUnits = children;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

}
