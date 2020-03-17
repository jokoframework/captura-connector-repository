package py.com.sodep.mf.cr.webapi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import py.com.sodep.mf.cr.webapi.exception.RestAuhtorizationException;
import py.com.sodep.mf.cr.webapi.exception.WebApiException;
import py.com.sodep.mf.exchange.ExchangeConstants;
import py.com.sodep.mf.exchange.objects.error.ErrorResponse;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * <p>
 * Custom header response x-sodep-mf-class: If the body contains a json object
 * the header will include the name of the Java class that can be used to read
 * the content
 * <p>
 * 
 * @author danicricco
 * 
 */
//Para conectarse a mf_cr.rest.baseURL
public class WebApiClient {

	private static final Logger logger = Logger.getLogger(WebApiClient.class);
	private String baseURL;
	private String encoding = "UTF-8";
	private String user;
	private String pass;
	private String sessionKey;
	private Object cookieLock = new Object();
	private ObjectMapper jsonMapper = new ObjectMapper();
	private final String defaultFormat = "json";
	private static final String defaultEncoding = "UTF-8";
	
	@Autowired
	public WebApiClient(String baseURL, String user, String pass) {

		if (baseURL.endsWith("/")) {
			this.baseURL = baseURL.substring(0, baseURL.length() - 1);
		} else {
			this.baseURL = baseURL;
		}

		this.user = user;
		this.pass = pass;

	}

	public String getBaseURL() {
		return baseURL;
	}

	public static void trustAll() throws NoSuchAlgorithmException, KeyManagementException {
		TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
			public java.security.cert.X509Certificate[] getAcceptedIssuers() {
				return null;
			}

			public void checkClientTrusted(X509Certificate[] certs, String authType) {
			}

			public void checkServerTrusted(X509Certificate[] certs, String authType) {
			}

		} };

		SSLContext sc = SSLContext.getInstance("SSL");
		sc.init(null, trustAllCerts, new java.security.SecureRandom());
		HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

		javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(new javax.net.ssl.HostnameVerifier() {
			public boolean verify(String hostname, javax.net.ssl.SSLSession sslSession) {
				return true;
			}
		});
	}

	public static void trustOnHost(final String host) {

		javax.net.ssl.HttpsURLConnection.setDefaultHostnameVerifier(new javax.net.ssl.HostnameVerifier() {
			public boolean verify(String hostname, javax.net.ssl.SSLSession sslSession) {
				if (hostname.equals(host)) {
					return true;
				}
				return false;
			}
		});

	}

	public WebApiClient clone() {
		return new WebApiClient(baseURL, user, pass);
	}

	private void setSessionKey(String cookie) {
		synchronized (cookieLock) {
			this.sessionKey = cookie;
		}
	}

	private String getSessionKey() {
		synchronized (cookieLock) {
			return sessionKey;
		}
	}

	private String getCookie(HttpURLConnection c) {
		String cookie = c.getHeaderField("Set-Cookie");
		if (cookie == null) {
			cookie = c.getHeaderField("set-cookie");
		}
		return cookie;
	}

	public void logout() throws IOException, WebApiException, RestAuhtorizationException {
		post(ExchangeConstants.PATH.LOGOUT, null, null);
	}

	public boolean login() throws IOException {

		URL url = new URL(baseURL + "/login");
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("POST");
		conn.setDoOutput(true);
		conn.setDoInput(true);
		conn.setUseCaches(false);
		conn.setAllowUserInteraction(false);
		conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");

		OutputStreamWriter stream = new OutputStreamWriter(conn.getOutputStream());
		StringWriter w = new StringWriter();
		w.append("username=" + URLEncoder.encode(user, encoding) + "&")
				.append("password=" + URLEncoder.encode(pass, encoding) + "&").append("device=false");

		conn.connect();
		stream.write(w.toString());
		stream.flush();

		int httpResponse = conn.getResponseCode();
		logger.debug("Login of user answered" + httpResponse);

		boolean success = false;
		if (httpResponse == HttpURLConnection.HTTP_OK) {
			String cookie = getCookie(conn);
			if (cookie != null) {
				setSessionKey(cookie);
				success = true;
			}

			logger.debug("Cookie anwered was: " + cookie);

		}

		stream.close();
		conn.disconnect();
		return success;
	}

	public static String urlReplacement(String url, Map<String, String> parameters) {
		String finalURL = url;
		if (parameters != null) {
			Set<String> keys = parameters.keySet();
			for (String key : keys) {
				String value;
				try {
					value = URLEncoder.encode(parameters.get(key), defaultEncoding);
				} catch (UnsupportedEncodingException e) {
					// this is really an unexpected exception so it is better to
					// make it runtime error
					throw new RuntimeException(e);
				}
				finalURL = finalURL.replace("{" + key + "}", value);
			}
		}
		return finalURL;
	}

	private static String readString(InputStream in, String charset) throws IOException {

		ByteArrayOutputStream outStream = new ByteArrayOutputStream();
		byte buff[] = new byte[1024];
		int readBytes = 0;
		while ((readBytes = in.read(buff)) > 0) {
			outStream.write(buff, 0, readBytes);
		}
		String s = new String(outStream.toByteArray(), charset != null ? charset : "UTF-8");
		outStream.close();
		return s;
	}

	public <T> T doGET(String urlSTR, Map<String, String> pathParametes, Map<String, String> requestParameters,
			Class<T> t) throws IOException, WebApiException, RestAuhtorizationException {
		return doGET(urlSTR, pathParametes, requestParameters, t, null);
	}

	public <T> T doGET(String urlSTR, Map<String, String> pathParametes, Map<String, String> requestParameters,
			TypeReference<T> typeReference) throws IOException, WebApiException, RestAuhtorizationException {
		return doGET(urlSTR, pathParametes, requestParameters, null, typeReference);
	}

	@SuppressWarnings("unchecked")
	private <T> T doGET(String urlSTR, Map<String, String> pathParametes, Map<String, String> requestParameters,
			Class<T> t, TypeReference<T> typeReference) throws IOException, WebApiException, RestAuhtorizationException {
		String finalURL = buildURL(urlSTR, pathParametes, requestParameters);
		logger.debug("Performing GET with URL " + finalURL);
		URL url = new URL(baseURL + finalURL);

		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod("GET");
			String sessionKey = getSessionKey();
			if (sessionKey != null) {
				conn.setRequestProperty("cookie", sessionKey);
			} else {
				throw new RestAuhtorizationException("Connection not authenticated");
			}
			conn.connect();
			int httpCode = conn.getResponseCode();

			if (httpCode == HttpURLConnection.HTTP_OK) {

				// success making the GET
				if (conn.getContentLength() > 0) {
					if (t != null) {
						if (t.getClass().equals(String.class)) {
							InputStream stream = conn.getInputStream();
							return (T) readString(stream, conn.getContentEncoding());
						} else {
							InputStream stream = conn.getInputStream();
							return jsonMapper.readValue(stream, t);
						}
					} else {
						InputStream stream = conn.getInputStream();
						return jsonMapper.readValue(stream, typeReference);
					}

				} else {
					return null;
				}

			} else {
				throwCustomError(conn, httpCode);
				return null;
			}
		} finally {
			if (conn != null) {
				conn.disconnect();
			}

		}

	}

	private String buildURL(String urlSTR, Map<String, String> pathParametes, Map<String, String> requestParameters)
			throws UnsupportedEncodingException {
		Map<String, String> _pathParametes = pathParametes;
		if (_pathParametes == null) {
			_pathParametes = new HashMap<String, String>();
		}
		if (!_pathParametes.containsKey("format")) {
			_pathParametes.put("format", defaultFormat);
		}
		String finalURL = urlReplacement(urlSTR, _pathParametes);
		if (requestParameters != null) {
			Set<String> params = requestParameters.keySet();
			boolean isFirst = true;
			for (String p : params) {
				String value = requestParameters.get(p);
				if (isFirst) {
					finalURL += "?";
					isFirst = false;
				} else {
					finalURL += "&";
				}
				if (value != null) {
					finalURL += p + "=" + URLEncoder.encode(value, defaultEncoding);
				}

			}

		}
		return finalURL;
	}

	public <T> T post(String urlStr, Object json, Class<T> t) throws IOException, RestAuhtorizationException,
			WebApiException {
		return post(urlStr, null, json, t);
	}

	public <T> T post(String urlStr, Map<String, String> parameters, Object json, Class<T> t) throws IOException,
			RestAuhtorizationException, WebApiException {
		return httpCall("POST", urlStr, parameters, json, t);
	}

	public <T> T put(String urlStr, Map<String, String> parameters, Object json, Class<T> t) throws IOException,
			RestAuhtorizationException, WebApiException {
		return httpCall("PUT", urlStr, parameters, json, t);
	}

	private <T> T httpCall(String method, String urlStr, Map<String, String> parameters, Object json, Class<T> t)
			throws IOException, RestAuhtorizationException, WebApiException {
		Map<String, String> _parameters = parameters;
		if (_parameters == null) {
			_parameters = new HashMap<String, String>();
		}
		if (!_parameters.containsKey("format")) {
			_parameters.put("format", defaultFormat);
		}
		String finalURL = urlReplacement(urlStr, _parameters);
		URL url = new URL(baseURL + finalURL);
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection) url.openConnection();
			conn.setRequestMethod(method);
			conn.setDoOutput(true);
			conn.setDoInput(true);
			String sessionKey = getSessionKey();
			if (sessionKey != null) {
				conn.setRequestProperty("cookie", sessionKey);
			} else {
				throw new RestAuhtorizationException("Connection not authenticated");
			}

			conn.setUseCaches(false);
			conn.setAllowUserInteraction(false);
			conn.setRequestProperty("Content-Type", "application/json");

			OutputStream out = conn.getOutputStream();
			jsonMapper.writeValue(out, json);
			out.close();
			int httpCode = conn.getResponseCode();
			if (httpCode >= 200 && httpCode <= 299) {
				if (conn.getContentLength() > 0) {
					if (conn.getContentType().startsWith("application/json")) {
						InputStream stream = conn.getInputStream();
						T obj = jsonMapper.readValue(stream, t);
						return obj;
					} else {
						throw new IllegalStateException("Expected a json object but got " + conn.getContentType()
								+ " for URL " + finalURL);
					}
				} else {
					return null;
				}
			} else {
				throwCustomError(conn, httpCode);
				return null;
			}
		} finally {
			if (conn != null) {
				conn.disconnect();
			}
		}
	}

	private void throwCustomError(HttpURLConnection conn, int httpCode) throws IOException, RestAuhtorizationException,
			JsonParseException, JsonMappingException, WebApiException {
		String contentType = conn.getContentType();
		int contentLength = conn.getContentLength();
		if (httpCode == HttpURLConnection.HTTP_FORBIDDEN || httpCode == 444) {
			String bodyMsg = "Access forbidden to " + conn.getURL();
			if (contentLength > 0) {
				InputStream stream = conn.getErrorStream();
				bodyMsg = readString(stream, conn.getContentEncoding());
			}

			throw new RestAuhtorizationException(bodyMsg);
		} else {
			if (contentType.startsWith("application/json")) {
				// match "application/json;charset=UTF-8"
				InputStream stream = conn.getErrorStream();
				ErrorResponse errorInfo = null;
				if (stream != null) {
					errorInfo = jsonMapper.readValue(stream, ErrorResponse.class);
				}
				throw new WebApiException(httpCode, errorInfo);
			} else if (contentType.startsWith("text/plain")) {
				// match text/plain;charset=UTF-8
				InputStream stream = conn.getErrorStream();
				String bodyMsg = "";
				if (stream != null) {
					bodyMsg = readString(stream, conn.getContentEncoding());
				}

				throw new WebApiException(httpCode, bodyMsg);
			} else {
				throw new WebApiException(httpCode);
			}
		}
	}
	public void close() {

	}
}
