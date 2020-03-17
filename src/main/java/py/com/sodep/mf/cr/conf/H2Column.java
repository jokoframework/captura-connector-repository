package py.com.sodep.mf.cr.conf;


public class H2Column {

	private String name;
	private String h2TypeName;
	private boolean pkMember;
	private int length;

	public H2Column(String name, String h2TypeName) {
		this.name = name;
		this.h2TypeName = h2TypeName;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getH2TypeName() {
		return h2TypeName;
	}

	public void setH2TypeName(String h2TypeName) {
		this.h2TypeName = h2TypeName;
	}

	public boolean isPkMember() {
		return pkMember;
	}

	public void setPkMember(boolean pkMember) {
		this.pkMember = pkMember;
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

}
