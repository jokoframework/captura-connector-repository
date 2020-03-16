package py.com.sodep.mf.cr.webapi;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import py.com.sodep.mf.cr.webapi.exception.RestAuhtorizationException;
import py.com.sodep.mf.cr.webapi.exception.WebApiException;
import py.com.sodep.mf.exchange.ExchangeConstants;
import py.com.sodep.mf.exchange.MFLoookupTableDefinition;
import py.com.sodep.mf.exchange.PagedData;
import py.com.sodep.mf.exchange.objects.data.ConditionalCriteria;
import py.com.sodep.mf.exchange.objects.data.MFOperationResult;
import py.com.sodep.mf.exchange.objects.lookup.LookupTableDTO;
import py.com.sodep.mf.exchange.objects.lookup.LookupTableDefinitionException;
import py.com.sodep.mf.exchange.objects.lookup.LookupTableModificationRequest;
import py.com.sodep.mf.exchange.objects.lookup.LookupTableModificationRequest.OPERATION_TYPE;

import com.fasterxml.jackson.core.type.TypeReference;

/**
 * This is a class that encapsulates the REST details of the lookup table calls.
 * For more details of the methods refer to the REST API documentation
 * (swagger-ui),since this is just a handy way to invoke the REST services.
 * 
 * @author danicricco
 * 
 */
public class MFWebApiLookupFacade {

	private final WebApiClient restClient;

	public MFWebApiLookupFacade(WebApiClient restClient) {
		this.restClient = restClient;
	}

	public MFLoookupTableDefinition createLookupTable(MFLoookupTableDefinition def) throws IOException,
			RestAuhtorizationException, LookupTableDefinitionException, WebApiException {
		try {
			MFLoookupTableDefinition defCreated = restClient.post(ExchangeConstants.PATH.LOOKUPTABLE_CREATE, def,
					MFLoookupTableDefinition.class);
			return defCreated;
		} catch (WebApiException e) {
			if (e.getHttpResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
				throw new LookupTableDefinitionException(e.getDefaultCustomError().getErrorCode(), e
						.getDefaultCustomError().getMessage());
			}
			throw e;
		}
	}

	public MFLoookupTableDefinition getLookupTableById(Long lookupTableId) throws IOException, WebApiException,
			RestAuhtorizationException {
		HashMap<String, String> pathParameters = new HashMap<String, String>();
		pathParameters.put("lookupTableId", "" + lookupTableId);

		MFLoookupTableDefinition lookupDef = restClient.doGET(ExchangeConstants.PATH.LOOKUPTABLE_GETLOOKUPTABLE,
				pathParameters, null, MFLoookupTableDefinition.class);
		return lookupDef;

	}

	public List<LookupTableDTO> listAll(Long appId, String identifier) throws IOException, WebApiException,
			RestAuhtorizationException {
		HashMap<String, String> parameters = new HashMap<String, String>();
		parameters.put("appId", "" + appId);
		parameters.put("identifier", identifier);
		List<LookupTableDTO> lookupList = restClient.doGET(ExchangeConstants.PATH.LOOKUPTABLE_FINDTABLES, null, parameters,
				new TypeReference<List<LookupTableDTO>>() {
				});
		return lookupList;
	}

	public MFOperationResult insertData(Long lookupTableId, List<Map<String, String>> rows) throws IOException,
			RestAuhtorizationException, WebApiException {
		HashMap<String, String> pathParameters = new HashMap<String, String>();
		pathParameters.put("lookupTableId", "" + lookupTableId);
		try {
			MFOperationResult result = restClient.post(ExchangeConstants.PATH.LOOKUPTABLE_DATA, pathParameters, rows,
					MFOperationResult.class);
			return result;
		} catch (WebApiException e) {
			if (e.getHttpResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
				MFOperationResult customError = (MFOperationResult) e.getCustomError();
				return customError;
			}
			throw e;

		}
	}

	public PagedData<List<Map<String, Object>>> listData(Long lookupTableId, HashMap<String, String> parameters,
			int pageNumber, int pageSize, String orderBy, boolean asc) throws IOException, WebApiException,
			RestAuhtorizationException {
		HashMap<String, String> pathParameters = new HashMap<String, String>();
		pathParameters.put("lookupTableId", "" + lookupTableId);

		HashMap<String, String> _parameters = parameters;
		if (_parameters == null) {
			_parameters = new HashMap<String, String>();
		}
		_parameters.put("pageNumber", "" + pageNumber);
		_parameters.put("pageSize", "" + pageSize);
		if (orderBy != null) {
			_parameters.put("orderBy", "" + orderBy);
			_parameters.put("asc", "" + asc);
		}

		PagedData pagedData = restClient.doGET(ExchangeConstants.PATH.LOOKUPTABLE_DATA, pathParameters, _parameters,
				PagedData.class);
		return pagedData;
	}

	public MFOperationResult updateData(Long lookupTableId, Map<String, String> newData) throws IOException,
			RestAuhtorizationException, WebApiException {

		LookupTableModificationRequest request = new LookupTableModificationRequest();
		request.setNewData(newData);
		request.setOperationType(OPERATION_TYPE.UPDATE);
		return modify(lookupTableId, request);
	}

	public MFOperationResult delete(Long lookupTableId, ConditionalCriteria selector) throws IOException,
			RestAuhtorizationException, WebApiException {
		LookupTableModificationRequest request = new LookupTableModificationRequest();
		request.setSelector(selector);
		request.setOperationType(OPERATION_TYPE.DELETE);
		return modify(lookupTableId, request);
	}

	private MFOperationResult modify(Long lookupTableId, LookupTableModificationRequest request) throws IOException,
			RestAuhtorizationException, WebApiException {
		HashMap<String, String> pathParameters = new HashMap<String, String>();
		pathParameters.put("lookupTableId", "" + lookupTableId);

		MFOperationResult result;
		try {
			result = restClient.put(ExchangeConstants.PATH.LOOKUPTABLE_DATA, pathParameters, request,
					MFOperationResult.class);
			return result;
		} catch (WebApiException e) {
			if (e.getHttpResponseCode() == HttpURLConnection.HTTP_CONFLICT) {
				MFOperationResult customError = (MFOperationResult) e.getCustomError();
				return customError;
			}
			throw e;
		}

	}
}
