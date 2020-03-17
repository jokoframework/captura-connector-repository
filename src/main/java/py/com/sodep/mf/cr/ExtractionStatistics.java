package py.com.sodep.mf.cr;

/**
 * This is a class that keeps of the time spend during the extraction of data.
 * There are global statistics (can't be reseted once the object was created)
 * and partial data that can be reseted with the {@link #resetStatsValues()} .
 * 
 * @author danicricco
 * 
 */
class ExtractionStatistics {

	private TimerStats startTime;
	private long numberOfRowsAnalyzed = 0;

	private long numberOfRowsInserted = 0;
	private long numberOfRowsUpdated = 0;
	private long numberOfThatDidntChange = 0;

	// Values that can be reset to track temporal statistics
	// this are for statistics purposes only and are reset every
	// time the information is logged.
	long st_numberOfRowsAnalyzed = 0;
	long st_numberOfRowsInserted = 0;
	long st_numberOfRowsUpdated = 0;

	private long st_findingRowTimeFull, st_findingRowTimeOverPK, st_UpdateTime, st_InsertTime, st_commit;

	public void startTime() {
		startTime = TimerStats.startTimer();
	}

	/**
	 * Increment the global and the static value for total analyzed rows
	 */
	public void incNumberOfRowsAnalyzed() {
		numberOfRowsAnalyzed++;
		st_numberOfRowsAnalyzed++;
	}

	/**
	 * Increment the global and the static value for total inserted rows
	 */
	public void incNumberOfRowsInserted() {
		numberOfRowsInserted++;
		st_numberOfRowsInserted++;
	}

	/**
	 * Increment the global and the static value for total updated rows
	 */
	public void incNumberOfRowsUpdated() {
		numberOfRowsUpdated++;
		st_numberOfRowsUpdated++;
	}

	/**
	 * Increment the global value for rows that didn't change (we only keep
	 * track of the global rows that didn't change)
	 */
	public void incNumberOfRowsDidntChange() {
		numberOfThatDidntChange++;
	}

	public void addSt_FindingRowTime(long t) {
		st_findingRowTimeFull += t;
	}

	public void addSt_FindingRowTimeOnPK(long t) {
		st_findingRowTimeFull += t;
	}

	public void addSt_UpdateTime(long t) {
		st_UpdateTime += t;
	}

	public void addSt_InsertTime(long t) {
		st_InsertTime += t;
	}

	public void addSt_Commit(long t) {
		st_commit += t;
	}

	/**
	 * Set to zero the quantity values that starts with st_
	 */
	public void resetStatsValues() {
		st_numberOfRowsAnalyzed = st_numberOfRowsInserted = st_numberOfRowsUpdated;
	}

	public long elapsedTime() {
		return startTime.elapsedTime();
	}

	public long elapsedTimeInSeconds() {
		return startTime.elapsedTime() / 1000;
	}

	public long getNumberOfRowsAnalyzed() {
		return numberOfRowsAnalyzed;
	}

	public long getNumberOfRowsInserted() {
		return numberOfRowsInserted;
	}

	public long getNumberOfRowsUpdated() {
		return numberOfRowsUpdated;
	}

	public long getNumberOfThatDidntChange() {
		return numberOfThatDidntChange;
	}

	public long getSt_numberOfRowsAnalyzed() {
		return st_numberOfRowsAnalyzed;
	}

	public long getSt_numberOfRowsInserted() {
		return st_numberOfRowsInserted;
	}

	public long getSt_numberOfRowsUpdated() {
		return st_numberOfRowsUpdated;
	}

	public long getSt_findingRowTimeFull() {
		return st_findingRowTimeFull;
	}

	public long getSt_findingRowTimeOverPK() {
		return st_findingRowTimeOverPK;
	}

	public long getSt_UpdateTime() {
		return st_UpdateTime;
	}

	public long getSt_InsertTime() {
		return st_InsertTime;
	}

	public long getSt_commit() {
		return st_commit;
	}

	public String getGlobalStats() {
		return "Elapsed time: " + elapsedTimeInSeconds() + " sec. , new =" + numberOfRowsInserted
				+ ", updated = " + numberOfRowsUpdated + " unchanged (or duplicates) = " + numberOfThatDidntChange
				+ ", analyzed= " + numberOfRowsAnalyzed;
	}

	public String getAvgExecutionTime() {
		long avg_findingTimeFull = st_findingRowTimeFull / st_numberOfRowsAnalyzed;
		long avg_findingTimeOverOK = 0;
		long avg_updateTime = 0;
		long avg_insertTime = 0;
		long avg_commitTime = 0;
		if (st_numberOfRowsInserted > 0 || st_numberOfRowsUpdated > 0) {
			avg_findingTimeOverOK = st_findingRowTimeOverPK / (st_numberOfRowsInserted + st_numberOfRowsUpdated);
			avg_commitTime = st_commit / (st_numberOfRowsInserted + st_numberOfRowsUpdated);
		}
		if (st_numberOfRowsUpdated > 0) {
			avg_updateTime = st_UpdateTime / st_numberOfRowsUpdated;
		}
		if (st_numberOfRowsInserted > 0) {
			avg_insertTime = st_InsertTime / st_numberOfRowsInserted;
		}

		return "Stats. avg_findingTimeFull = " + avg_findingTimeFull + ", avg_findingTimeOnPK = "
				+ avg_findingTimeOverOK + ", avg_updateTime = " + avg_updateTime + ", avg_insertTime = "
				+ avg_insertTime + ", avg_commitTime = " + avg_commitTime;

	}
}
