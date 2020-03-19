package py.com.sodep.mf.cr;

import org.apache.commons.lang3.SystemUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import py.com.sodep.mf.cr.webadmin.CRWebAdminServer;
import sun.misc.Signal;
import sun.misc.SignalHandler;

public class OSSignalHandler implements SignalHandler {
	private static final Logger logger = LogManager.getLogger(OSSignalHandler.class);

	private final CRServer server;
	private CRWebAdminServer webAdminServer;
	
	public OSSignalHandler(CRServer server) {
		this(server, null);
	}

	public OSSignalHandler(CRServer server, CRWebAdminServer webAdminServer) {
		this.server = server;
		this.webAdminServer = webAdminServer;
	}

	public void setWebAdminServer(CRWebAdminServer webAdminServer) {
		this.webAdminServer = webAdminServer;
	}

	/**
	 * Register os signals to catch
	 */
	private SignalHandler registerSOSignals(String signalName) {
		logger.debug("Registering signal: " + signalName);
		Signal diagSignal = new Signal(signalName);
		SignalHandler handle = Signal.handle(diagSignal, this);
		return handle;
	}

	public void initializeOSSignals() throws InterruptedException {
		logger.info("OS: " + SystemUtils.OS_NAME);
		registerSOSignals("TERM");
		registerSOSignals("INT");
		if (SystemUtils.IS_OS_WINDOWS) {
			// windows doesn't support USR2 SIGNAL
			logger.warn("Skipping signal registering: USR2");
		} else {
			registerSOSignals("USR2");
		}
	}

	@Override
	public void handle(Signal sig) {
		logger.info(" Handling " + sig.getName());
		if (sig.getName().equals("USR2")) {
			logger.info("received usr2");
		} else {
			try {
				server.shutdown();
				if (webAdminServer != null) {
					webAdminServer.stop();
				}
			} catch (InterruptedException e) {
				// do nothing we are shutting down
			} catch (Exception e) {
				// do nothing we are shutting down
			}
		}

	}

}
