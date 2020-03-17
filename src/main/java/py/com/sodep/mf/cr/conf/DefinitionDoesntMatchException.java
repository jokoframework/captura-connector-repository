package py.com.sodep.mf.cr.conf;

import py.com.sodep.mf.exchange.MFDataSetDefinition;

/**
 * The definition obtained from the SQL doesn't match the remote definition
 * 
 * @author danicricco
 * 
 */
public class DefinitionDoesntMatchException extends TemporalConfigurationError {

	private final MFDataSetDefinition remoteDefinition;
	private final MFDataSetDefinition definitionBasedOnExtractionUnit;
	private final String extractionUnit;

	public DefinitionDoesntMatchException(MFDataSetDefinition remoteDefinition,
			MFDataSetDefinition definitionBasedOnExtractionUnit, String extractionUnit) {
		super("The defintion obtained from the extractionUnit #" + extractionUnit
				+ " doesn't match the remote lookup table");
		this.remoteDefinition = remoteDefinition;
		this.definitionBasedOnExtractionUnit = definitionBasedOnExtractionUnit;
		this.extractionUnit = extractionUnit;
	}

	public MFDataSetDefinition getRemoteDefinition() {
		return remoteDefinition;
	}

	public MFDataSetDefinition getDefinitionBasedOnExtractionUnit() {
		return definitionBasedOnExtractionUnit;
	}

	public String getExtractionUnit() {
		return extractionUnit;
	}

}
