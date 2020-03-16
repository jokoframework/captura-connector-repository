package py.com.sodep.mf.cr.webapi.exception;

import py.com.sodep.mf.exchange.objects.error.ErrorResponse;

/***
 * Any other http status different than 2xx will be encapsulated within a
 * RestException. If the server answered with a json body it will be converted
 * to a {@link ExchangeError} that is accesible by the field
 * {@link #customError}
 * 
 * @author danicricco
 * 
 */
public class WebApiException extends Exception {

	private static final long serialVersionUID = 1L;
	private final int httpResponseCode;
	private final Object customErrorInfo;

	public WebApiException(int httpResponseCode, String msg) {
		super(msg);
		this.httpResponseCode = httpResponseCode;
		customErrorInfo = null;
	}

	public WebApiException(int httpResponseCode, Object customErrorInfo) {
		this.customErrorInfo = customErrorInfo;
		this.httpResponseCode = httpResponseCode;
	}

	public WebApiException(int httpCode) {
		this(httpCode, (String) null);
	}

	@Override
	public String getMessage() {
		if (getDefaultCustomError() != null) {
			return getDefaultCustomError().getMessage();

		} else {
			String originalMsg = super.getMessage();
			if (originalMsg != null) {
				return originalMsg;
			} else {
				return "httpResponseCode=" + httpResponseCode;
			}
		}
	}

	public int getHttpResponseCode() {
		return httpResponseCode;
	}

	/**
	 * This method will check if the custom error is an instance of
	 * {@link ErrorResponse} and return it. Otherwise, it will return null
	 * 
	 * @return
	 */
	public ErrorResponse getDefaultCustomError() {
		if (customErrorInfo instanceof ErrorResponse) {
			return (ErrorResponse) customErrorInfo;
		}
		return null;
	}

	/**
	 * This will return the custom error object as it was sent from the server.
	 * If the response contains a json object it will include a custom header
	 * field with the name of the java class that match the JSON
	 * ("x-sodep-mf-class")
	 * 
	 * @return
	 */
	public Object getCustomError() {
		return customErrorInfo;
	}

}
