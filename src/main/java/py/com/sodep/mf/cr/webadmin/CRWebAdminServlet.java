package py.com.sodep.mf.cr.webadmin;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import py.com.sodep.mf.cr.CRServer;
import py.com.sodep.mf.cr.MultiDBManager;
import py.com.sodep.mf.cr.conf.CRColumn;
import py.com.sodep.mf.cr.conf.CRConfigurationParser;
import py.com.sodep.mf.cr.conf.CRConnection;
import py.com.sodep.mf.cr.conf.CRExtractionUnit;
import py.com.sodep.mf.cr.conf.ConnectorDefinition;
import py.com.sodep.mf.cr.conf.TemporalConfigurationError;
import py.com.sodep.mf.cr.exception.CRUnexpectedException;
import py.com.sodep.mf.cr.exception.ConnectionException;
import py.com.sodep.mf.exchange.MFField;

public class CRWebAdminServlet extends HttpServlet {

	private List<ConnectionDTO> connectionTree;
	private ConnectorDefinition connectorDefinition;

	// Not found in HttpServletRequest because it's a WebDAV extension
	private final int SC_UNPROCESSABLE_ENTITY = 422;

	public CRWebAdminServlet(ConnectorDefinition connectorDefinition) {
		super();
		this.connectorDefinition = connectorDefinition;
		buildConnectionTree();
	}

	@Override
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String uri = request.getRequestURI();
		String json;
		ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();

		if (uri.equals("/")) {
			respondWithIndex(request, response);
		} else if (uri.equals("/ajax/connectionTree")) {
			json = ow.writeValueAsString(this.connectionTree);
			respondWithJson(request, response, json);
		} else if (uri.equals("/ajax/getColumnTypes")) {
			json = ow.writeValueAsString(MFField.FIELD_TYPE.values());
			respondWithJson(request, response, json);
		} else {
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			response.getWriter().print("404 Not Found");
		}

	}

	@Override
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		String uri = request.getRequestURI();

		if (uri.equals("/ajax/updateConnection")) {
			respondUpdateConnection(request, response);
		} else if (uri.equals("/ajax/updateExtractionUnit")) {
			respondUpdateExtractionUnit(request, response);
		} else if (uri.equals("/ajax/testConnection")) {
			respondTestConnection(request, response);
		} else if (uri.equals("/ajax/saveConnection")) {
			respondSaveConnection(request, response);
		} else if (uri.equals("/ajax/saveExtractionUnit")) {
			respondSaveExtractionUnit(request, response);
		} else if (uri.equals("/ajax/getColumns")) {
			respondGetColumns(request, response);
		} else {
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			response.getWriter().print("404 Not Found");
		}
	}

	private void respondGetColumns(HttpServletRequest request, HttpServletResponse response) throws IOException {

		BufferedReader reader = request.getReader();
		String json = IOUtils.toString(reader);
		reader.close();

		if (!request.getContentType().contains("application/json")) {
			respondWithError(request, response, HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
			return;
		}

		ObjectMapper mapper = new ObjectMapper();
		CRExtractionUnit extractionUnit = null;

		try {
			extractionUnit = mapper.readValue(json, new TypeReference<CRExtractionUnit>() {
			});
		} catch (JsonParseException e) {
			respondWithError(request, response, HttpServletResponse.SC_BAD_REQUEST, CRWebAdminErrors.JSON_INVALID);
			return;
		} catch (JsonMappingException e) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (extractionUnit == null) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (extractionUnit.hasEmptyFields()) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.EMPTY_FIELDS);
			return;
		}

		CRConnection connection = null;
		List<CRColumn> columns = null;

		for (ConnectionDTO c : this.connectionTree) {
			if (c.getConnection().getId().equals(extractionUnit.getConnectionId())) {
				connection = c.getConnection();
			}
		}

		if (connection != null) {
			try {
				columns = CRServer.columnsForExtractionUnit(connection, extractionUnit);
			} catch (TemporalConfigurationError e) {
				respondWithError(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						e.getMessage());
			} catch (ConnectionException e) {
				respondWithError(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
						CRWebAdminErrors.CONNECTION_UNREACHABLE);
			}

			ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
			String jsonResponse = ow.writeValueAsString(columns);
			respondWithJson(request, response, jsonResponse);
		} else {
			respondWithError(request, response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
					CRWebAdminErrors.EU_UNKNOWN_CONNECTION);
		}

	}

	private void respondTestConnection(HttpServletRequest request, HttpServletResponse response) throws IOException {
		BufferedReader reader = request.getReader();
		String json = IOUtils.toString(reader);
		reader.close();

		if (!request.getContentType().contains("application/json")) {
			respondWithError(request, response, HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
			return;
		}

		ObjectMapper mapper = new ObjectMapper();
		CRConnection connection = null;

		try {
			connection = mapper.readValue(json, new TypeReference<CRConnection>() {
			});
		} catch (JsonParseException e) {
			respondWithError(request, response, HttpServletResponse.SC_BAD_REQUEST, CRWebAdminErrors.JSON_INVALID);
			return;
		} catch (JsonMappingException e) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		MultiDBManager manager = new MultiDBManager();
		boolean isReachable = true;
		try {
			manager.register(connection, true);
			manager.open(connection.getId());
		} catch (TemporalConfigurationError e) {
			isReachable = false;
		} catch (ConnectionException e) {
			isReachable = false;
		} catch (CRUnexpectedException e) {
			isReachable = false;
		}

		response.getWriter().print(isReachable);
	}

	private void respondUpdateExtractionUnit(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		BufferedReader reader = request.getReader();
		String json = IOUtils.toString(reader);
		reader.close();

		if (!request.getContentType().contains("application/json")) {
			respondWithError(request, response, HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
			return;
		}

		ObjectMapper mapper = new ObjectMapper();
		List<CRExtractionUnit> extractionUnits = null;

		try {
			extractionUnits = mapper.readValue(json, new TypeReference<List<CRExtractionUnit>>() {
			});
		} catch (JsonParseException e) {
			respondWithError(request, response, HttpServletResponse.SC_BAD_REQUEST, CRWebAdminErrors.JSON_INVALID);
			return;
		} catch (JsonMappingException e) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		// We need the old extractionUnit and the new one that'll replace it
		if (extractionUnits == null || extractionUnits.size() != 2) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		CRExtractionUnit previousEU = extractionUnits.get(0);
		CRExtractionUnit updatedEU = extractionUnits.get(1);

		if (previousEU == null || updatedEU == null) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (updatedEU.hasEmptyFields() || previousEU.hasEmptyFields()) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.EMPTY_FIELDS);
			return;
		}

		CRConfigurationParser parser = new CRConfigurationParser();
		FileWriter writer = new FileWriter(connectorDefinition.getSourceFile());
		connectorDefinition.updateExtractionUnit(previousEU, updatedEU);

		// write the changes to the xml file
		parser.write(connectorDefinition, writer);
		writer.close();

		// rebuild the tree after the change
		buildConnectionTree();

		response.setStatus(HttpServletResponse.SC_OK);

	}

	private void respondSaveExtractionUnit(HttpServletRequest request, HttpServletResponse response) throws IOException {
		BufferedReader reader = request.getReader();
		String json = IOUtils.toString(reader);
		reader.close();

		if (!request.getContentType().contains("application/json")) {
			respondWithError(request, response, HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
			return;
		}

		ObjectMapper mapper = new ObjectMapper();
		CRExtractionUnit extractionUnit = null;

		try {
			extractionUnit = mapper.readValue(json, new TypeReference<CRExtractionUnit>() {
			});
		} catch (JsonParseException e) {
			respondWithError(request, response, HttpServletResponse.SC_BAD_REQUEST, CRWebAdminErrors.JSON_INVALID);
			return;
		} catch (JsonMappingException e) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (extractionUnit == null) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (extractionUnit.hasEmptyFields()) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.EMPTY_FIELDS);
			return;
		}

		CRConfigurationParser parser = new CRConfigurationParser();
		FileWriter writer = new FileWriter(connectorDefinition.getSourceFile());
		connectorDefinition.addExtractionUnit(extractionUnit);

		// write the changes to the xml file
		parser.write(connectorDefinition, writer);
		writer.close();

		// rebuild the tree after the change
		buildConnectionTree();

		response.setStatus(HttpServletResponse.SC_OK);
	}

	private void respondUpdateConnection(HttpServletRequest request, HttpServletResponse response) throws IOException {
		BufferedReader reader = request.getReader();
		String json = IOUtils.toString(reader);
		reader.close();

		if (!request.getContentType().contains("application/json")) {
			respondWithError(request, response, HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
			return;
		}

		ObjectMapper mapper = new ObjectMapper();
		List<CRConnection> connections = null;

		try {
			connections = mapper.readValue(json, new TypeReference<List<CRConnection>>() {
			});
		} catch (JsonParseException e) {
			respondWithError(request, response, HttpServletResponse.SC_BAD_REQUEST, CRWebAdminErrors.JSON_INVALID);
			return;
		} catch (JsonMappingException e) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		// We need the old connection and the new one that'll replace it
		if (connections == null || connections.size() != 2) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		CRConnection previousConnection = connections.get(0);
		CRConnection updatedConnection = connections.get(1);

		if (previousConnection == null || updatedConnection == null) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (updatedConnection.hasEmptyFields(true) || previousConnection.hasEmptyFields(true)) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.EMPTY_FIELDS);
			return;
		}

		CRConfigurationParser parser = new CRConfigurationParser();
		FileWriter writer = new FileWriter(connectorDefinition.getSourceFile());
		connectorDefinition.updateConnection(previousConnection, updatedConnection);

		// write the changes to the xml file
		parser.write(connectorDefinition, writer);
		writer.close();

		// rebuild the tree after the change
		buildConnectionTree();

		response.setStatus(HttpServletResponse.SC_OK);
	}

	private void respondSaveConnection(HttpServletRequest request, HttpServletResponse response) throws IOException {
		BufferedReader reader = request.getReader();
		String json = IOUtils.toString(reader);
		reader.close();

		if (!request.getContentType().contains("application/json")) {
			respondWithError(request, response, HttpServletResponse.SC_UNSUPPORTED_MEDIA_TYPE);
			return;
		}

		ObjectMapper mapper = new ObjectMapper();
		CRConnection connection = null;

		try {
			connection = mapper.readValue(json, new TypeReference<CRConnection>() {
			});
		} catch (JsonParseException e) {
			respondWithError(request, response, HttpServletResponse.SC_BAD_REQUEST, CRWebAdminErrors.JSON_INVALID);
			return;
		} catch (JsonMappingException e) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (connection == null) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.JSON_UNPROCESSABLE_ENTITY);
			return;
		}

		if (connection.hasEmptyFields(true)) {
			respondWithError(request, response, SC_UNPROCESSABLE_ENTITY, CRWebAdminErrors.EMPTY_FIELDS);
			return;
		}

		CRConfigurationParser parser = new CRConfigurationParser();
		FileWriter writer = new FileWriter(connectorDefinition.getSourceFile());
		connectorDefinition.addConnection(connection);

		// write the changes to the xml file
		parser.write(connectorDefinition, writer);
		writer.close();

		// rebuild the tree after the change
		buildConnectionTree();

		response.setStatus(HttpServletResponse.SC_OK);
	}

	private void respondWithError(HttpServletRequest request, HttpServletResponse response, int statusCode)
			throws IOException {
		respondWithError(request, response, statusCode, null);
	}

	private void respondWithError(HttpServletRequest request, HttpServletResponse response, int statusCode,
			String message) throws IOException {
		response.setStatus(statusCode);
		response.getWriter().print(message);
	}

	private void respondWithIndex(HttpServletRequest request, HttpServletResponse response) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader("web/index.html"));
		String index = IOUtils.toString(reader);
		response.setContentType("text/html");
		response.setStatus(HttpServletResponse.SC_OK);
		response.getWriter().print(index);
		reader.close();
	}

	private void respondWithJson(HttpServletRequest request, HttpServletResponse response, String jsonString)
			throws IOException {
		response.setContentType("application/json");
		response.setStatus(HttpServletResponse.SC_OK);
		response.getWriter().print(jsonString);
	}

	private void buildConnectionTree() {
		this.connectionTree = new ArrayList<ConnectionDTO>();

		for (CRConnection conn : this.connectorDefinition.getConnections()) {
			ConnectionDTO connectionDTO = new ConnectionDTO();
			connectionDTO.setConnection(conn);
			connectionDTO.setLabel(conn.getId());
			connectionDTO.setId(conn.getId());

			for (CRExtractionUnit eu : this.connectorDefinition.getExtractionUnits()) {
				if (eu.getConnectionId().equals(conn.getId())) {
					connectionDTO.addExtractionUnit(eu);
				}
			}

			this.connectionTree.add(connectionDTO);
		}
	}

}
