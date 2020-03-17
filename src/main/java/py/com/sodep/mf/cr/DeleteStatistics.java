package py.com.sodep.mf.cr;

/**
 * This class summarizes the result of the delete process on the CR.
 * 
 * @author danicricco
 * 
 */
public class DeleteStatistics {

	private TimerStats startTime;
	private long numberOfRowsAnalyzed = 0;
	private long numberOfRowsDeleted = 0;

	public void incNumberOfRowsAnalyzed() {
		numberOfRowsAnalyzed++;
	}

	public void incNumberOfRowsDeleted() {
		numberOfRowsDeleted++;
	}

	public TimerStats getStartTime() {
		return startTime;
	}

	public long getNumberOfRowsAnalyzed() {
		return numberOfRowsAnalyzed;
	}

	public long getNumberOfRowsDeleted() {
		return numberOfRowsDeleted;
	}

	public void startTime() {
		startTime = TimerStats.startTimer();
	}

	public long elapsedTime() {
		return startTime.elapsedTime();
	}

	public long elapsedTimeInSeconds() {
		return startTime.elapsedTime() / 1000;
	}

	public String getStats() {
		return "Elapsed time: " + elapsedTimeInSeconds() + " sec. , deleted " + numberOfRowsDeleted + ", analyzed= "
				+ numberOfRowsAnalyzed;
	}
}
