package py.com.sodep.mf.cr;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

import py.com.sodep.mf.cr.conf.CRConfigurationParser;
import py.com.sodep.mf.cr.conf.ConnectorDefinition;
import py.com.sodep.mf.cr.conf.DefinitionDoesntMatchException;
import py.com.sodep.mf.cr.webadmin.CRWebAdminServer;
import py.com.sodep.mf.cr.webapi.WebApiClient;
import py.com.sodep.mf.cr.webapi.exception.RestAuhtorizationException;
import py.com.sodep.mf.cr.webapi.exception.WebApiException;

@SpringBootApplication
public class CRServerLauncher {
	private static final Logger logger = LogManager.getLogger(CRServerLauncher.class);
	
	private static String propertyFilePath = "conf/mf_cr.properties"; //Settings path
	
	public static final String PROP_MODE = "mf_cr.mode";
	public static final String PROP_BASEURL = "mf_cr.rest.baseURL";	//Captura URL
	public static final String PROP_REST_APP_ID = "mf_cr.rest.applicationId";	//Captura ID
	public static final String PROP_REST_USER = "mf_cr.rest.user";	//Captura User
	public static final String PROP_REST_PASS = "mf_cr.rest.pass";	//Captura Password
	
	public static final String PROP_DB_USER = "mf_cr.db.user";	//DB H2 User
	public static final String PROP_DB_PASS = "mf_cr.db.pass";	//DB H2 Password
	public static final String PROP_DB_FILEPATH = "mf_cr.db.filePath";	//DB H2 PATH
	public static final String PROP_AUTHENTICATE_ON_STARTUP = "mf_cr.authenticateOnStartup";
	public static final String PROP_XML_FILE = "mf_cr.xml.filePath";	//Path to the xml that configures the connection to the external DB
	
	public static final String PROP_WEBADMIN_WAKE_ON_STARTUP = "mf_cr.webadmin.wakeOnStartup";
	public static final String PROP_WEBADMIN_PORT = "mf_cr.webadmin.port";
	public static final String PROP_WEBADMIN_USER = "mf_cr.webadmin.user";
	public static final String PROP_WEBADMIN_PASSWORD = "mf_cr.webadmin.password";
	
	private static OSSignalHandler osSignalhandler;
	
	// trust on all certificates
	private static boolean testingMode = true;
	
	public static void main(String[] args) {
		SpringApplication.run(CRServerLauncher.class, args);
	}
	
	@Bean
	public void launcher() throws Exception {
		//Imports the properties of mf_cr.propierties
		Properties properties = new Properties();
		Properties p = loadProperties(properties, propertyFilePath);
		
		if (p == null) {
			System.exit(CRServerErrors.PROPERTY_FILE_NOT_FOUND);
		}
		//The mf_cr.propierties are extracted
		String mode = PropReader.mode(p);
		String restBaseURL = (String) p.get(PROP_BASEURL);
		String restUser = (String) p.get(PROP_REST_USER);
		String restPass = (String) p.get(PROP_REST_PASS);
		String dbUser = (String) p.get(PROP_DB_USER);
		String dbPass = (String) p.get(PROP_DB_PASS);
		String dbFilePath = (String) p.get(PROP_DB_FILEPATH);
		String xmlFile = (String) p.get(PROP_XML_FILE);
		boolean webAdminWakeOnStartup = Boolean.parseBoolean((String) p.get(PROP_WEBADMIN_WAKE_ON_STARTUP));
		int webAdminPort = Integer.parseInt((String) p.get(PROP_WEBADMIN_PORT));
		String webAdminUser = (String) p.get(PROP_WEBADMIN_USER);
		String webAdminPassword = (String) p.get(PROP_WEBADMIN_PASSWORD);
		Long appId = PropReader.applicationId(p);
		boolean authenticateOnStartup = PropReader.authenticateOnStartup(p);

		if (testingMode) {
			WebApiClient.trustAll();
		}
		
		WebApiClient restClient = new WebApiClient(restBaseURL, restUser, restPass);
		//Login to mf_cr.rest.baseURL
		if (authenticateOnStartup) {
			logger.info("Testing connection...");
			boolean success;
			try {
				success = restClient.login();
				if (success) {
					logger.info("Successfully logged in to " + restBaseURL);
					logger.info("Login successful with user " + restUser);
					try {
						restClient.logout();
					} catch (WebApiException e) {
						logger.error("Problems on the first logout", e);
						System.exit(CRServerErrors.FIRST_LOGIN_FAILED);
					} catch (RestAuhtorizationException e) {
						logger.error("Problems on the first logout", e);
						System.exit(CRServerErrors.FIRST_LOGIN_FAILED);
					}
				} else {
					logger.error("Couldn't authenticate to the server");
					System.exit(CRServerErrors.FIRST_LOGIN_FAILED);
				}
			} catch (IOException e) {
				logger.error("Couldn't connect to MF server at " + restBaseURL);
				logger.debug("Couldn't connect to MF server at " + restBaseURL, e);
				System.exit(CRServerErrors.FIRST_LOGIN_FAILED);
			}

		}
		logger.info("Starting Connector Repository");

		CRServer server = null;
		CRWebAdminServer webAdminServer = null;
		ConnectorDefinition desDefinition = null;
		
		try {
			server = new CRServer(restClient, dbUser, dbPass, dbFilePath, appId);
			
			if (mode.equals("XML")) {
				CRConfigurationParser parser = new CRConfigurationParser();
				//This will create parent folders if do not exist and create a file if not exists 
				//and throw a exception if file object is a directory or cannot be written to.
				FileOutputStream s = FileUtils.openOutputStream(new File(xmlFile));
				FileReader file = new FileReader(xmlFile);
				desDefinition = parser.parse(file);
				if(desDefinition==null) 
				{
					desDefinition = new ConnectorDefinition();
				}
				desDefinition.setSourceFile(xmlFile);
				server.configure(desDefinition);
				int countOfStartedThreads = server.start();
				//if (countOfStartedThreads > 0) {
					osSignalhandler = new OSSignalHandler(server);
					osSignalhandler.initializeOSSignals();
				//} else {
					//logger.warn("There is no active extraction unit.");
				//}
				
			}
			
			if (webAdminWakeOnStartup) {
				webAdminServer = new CRWebAdminServer(webAdminPort, webAdminUser, webAdminPassword, desDefinition);
				osSignalhandler.setWebAdminServer(webAdminServer);
				webAdminServer.start();
			}
			
		} catch (Throwable e) {

			if (server != null) {
				try {
					server.shutdown();
				} catch (InterruptedException e1) {
					// do nothing, we are shutting down
				}
			}
			
			if (webAdminServer != null) {
				try {
					webAdminServer.stop();
				} catch (Exception webAdminException) {
					// do nothing, we are shutting down 
				}
			}
			
			
			if (e instanceof DefinitionDoesntMatchException) {
				DefinitionDoesntMatchException ex = (DefinitionDoesntMatchException) e;
				System.out.println(e.getMessage());
				ObjectMapper mapper = new ObjectMapper();
				ObjectWriter writer = mapper.writerWithDefaultPrettyPrinter();

				try {
					System.out.println("******* REMOTE DEFINITION *******");
					String json = writer.writeValueAsString(ex.getRemoteDefinition().getFields());
					System.out.println(json);
					System.out.println("******* SQL BASED DEFINITION *******");
					json = writer.writeValueAsString(ex.getDefinitionBasedOnExtractionUnit().getFields());
					System.out.println(json);

				} catch (JsonGenerationException e1) {
					logger.fatal("Couldn't report user error DefinitionDoesntMatchException because of "
							+ e1.getClass().getName(), e);
				} catch (JsonMappingException e1) {
					logger.fatal("Couldn't report user error DefinitionDoesntMatchException because of "
							+ e1.getClass().getName(), e);
				} catch (IOException e1) {
					logger.fatal("Couldn't report user error DefinitionDoesntMatchException because of "
							+ e1.getClass().getName(), e);
				}

			} else if (e instanceof ConfError) {
				System.out.println(e.getMessage());
			} else {
				logger.error("" + e.getClass().getName() + ": " + e.getMessage(), e);
			}
			System.exit(CRServerErrors.CONFIGURATION_ERROR);
		}
	}
	public static class ConfError extends RuntimeException {
		public ConfError(String msg) {
			super(msg);
		}
	}

	public static class PropReader {
		public static String mode(Properties p) {
			Object modeO = p.get(PROP_MODE);
			String mode = ((String) modeO).toString();
			if (!mode.equals("XML")) {
				throw new ConfError(mode + " is not a valid mode. XML is currently the only supported mode");
			}
			return (String) mode;
		}

		public static boolean authenticateOnStartup(Properties p) {
			String authenticateOnStartStr = ((String) p.get(PROP_AUTHENTICATE_ON_STARTUP)).trim();
			try {
				return new Boolean(authenticateOnStartStr);
			} catch (IllegalArgumentException e) {
				throw new ConfError("The property " + PROP_AUTHENTICATE_ON_STARTUP + " must be either true or false");
			}
		}

		public static Long applicationId(Properties p) {
			String applicationIdStr = ((String) p.get(PROP_REST_APP_ID)).trim();
			try {
				Long appId = Long.parseLong(applicationIdStr);
				return appId;
			} catch (NumberFormatException e) {
				throw new ConfError("The property " + PROP_REST_APP_ID + " must be an integer value ");
			}
		}
	}

	private static Properties loadProperties(Properties properties, String propertyFilePath) {
		FileInputStream in = null;
		try {
			in = new FileInputStream(propertyFilePath);
			properties.load(in);
			return properties;
		} catch (FileNotFoundException e) {
			e.printStackTrace(System.err);

		} catch (IOException e) {
			e.printStackTrace(System.err);

		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException e) {
					// do nothing here
				}
			}

		}
		return null;
	}
}
