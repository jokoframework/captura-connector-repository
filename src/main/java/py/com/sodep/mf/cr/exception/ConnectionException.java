package py.com.sodep.mf.cr.exception;

import java.sql.SQLException;

import py.com.sodep.mf.cr.conf.CRConnection;


public class ConnectionException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private final CRConnection connectionDesc;

	public ConnectionException(CRConnection connectionDesc, SQLException cause) {
		super("Unable to connect to : " + connectionDesc.getUrl(), cause);
		this.connectionDesc = connectionDesc;
	}

	public CRConnection getConnectionDesc() {
		return connectionDesc;
	}

}
