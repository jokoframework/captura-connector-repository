package py.com.sodep.mf.cr.exception;

public class CRUnexpectedException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public CRUnexpectedException(String msg) {
		super(msg);
	}

	public CRUnexpectedException(Throwable cause) {
		super(cause);
	}
}
