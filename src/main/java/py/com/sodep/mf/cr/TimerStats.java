package py.com.sodep.mf.cr;

public class TimerStats {

	private final Long startTime;

	private TimerStats() {
		startTime = System.currentTimeMillis();
	}

	public long elapsedTime() {
		return System.currentTimeMillis() - startTime;
	}

	public static TimerStats startTimer() {
		return new TimerStats();
	}

}
