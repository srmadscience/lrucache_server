package lrucache.server;

import java.util.Date;

/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.lrucache.client.LRUCallback;
import org.voltdb.types.TimestampType;

public class GetByIdOLD extends VoltProcedure {

	public static final SQLStmt getById = new SQLStmt("SELECT * FROM subscriber WHERE s_id = ?;");

	public static final SQLStmt updateDate = new SQLStmt("UPDATE subscriber SET last_use_date =  ? WHERE s_id = ?;");

	public static final SQLStmt countRecs = new SQLStmt("SELECT count(*) how_many FROM subscriber;");

	public static final SQLStmt findOldestRecord = new SQLStmt(
			"select min(last_use_date) last_use_date from subscriber;");

	public static final SQLStmt findOldestRecordCount = new SQLStmt(
			"select count(*) how_many from subscriber where last_use_date = ?;");

	public static final SQLStmt findOldestRecordBeforexOLD = new SQLStmt(
			"select min(last_use_date) last_use_date from subscriber WHERE last_use_date > ? ;");

	public static final SQLStmt findOldestRecordBeforex = new SQLStmt(
			"select last_use_date from subscriber WHERE last_use_date > ? ORDER By last_use_date LIMIT 1;");

	public static final SQLStmt delStale = new SQLStmt(
			"delete from subscriber where last_use_date = ? and flush_date IS NOT NULL;");

	public static final SQLStmt evictId = new SQLStmt("INSERT INTO subscriber_export (s_id, sub_nbr, "
			+ "f_tinyint , f_smallint , f_integer , f_bigint , f_float , f_decimal , f_geography"
			+ " , f_geography_point , f_varchar , f_varbinary, last_use_date) " + "SELECT s_id, sub_nbr, "
			+ "f_tinyint , f_smallint , f_integer , f_bigint , f_float , f_decimal , f_geography"
			+ " , f_geography_point , f_varchar , f_varbinary, last_use_date"
			+ " FROM subscriber WHERE last_use_date = ? ORDER BY s_id;");

	public static final SQLStmt setFlushDate = new SQLStmt(
			"UPDATE subscriber SET flush_date = CURRENT_TIMESTAMP WHERE last_use_date = ?;");

	public static final SQLStmt touchLastUseDate = new SQLStmt(
			"UPDATE subscriber SET last_use_date = DATEADD(SECOND, ?, last_use_date), flush_date = CURRENT_TIMESTAMP WHERE last_use_date = ?;");

	private static final int MIN_LIFETIME_SECONDS = 60;

	public VoltTable[] run(long subscriberId, long maxSizePerPart) throws VoltAbortException {

		voltQueueSQL(getById, subscriberId);
		voltQueueSQL(updateDate, getTransactionTime(), subscriberId);
		voltQueueSQL(countRecs);

		VoltTable[] results = voltExecuteSQL();

		// If we didn't find anything request a retry...
		if (results[0].getRowCount() == 0) {
			setAppStatusCode(LRUCallback.CACHE_MISS_STATUS_CODE);
		}

		// See if we need to evict something...

		results[2].advanceRow();

		if (results[2].getLong("HOW_MANY") > maxSizePerPart) {

			// Delete evicted records older than x

			// Find oldest date
			voltQueueSQL(findOldestRecord);
			VoltTable[] oldestRecordResults = voltExecuteSQL();
			oldestRecordResults[0].advanceRow();
			TimestampType proposedDeleteDate = oldestRecordResults[0].getTimestampAsTimestamp(0);
			final TimestampType evictThreshold = new TimestampType(
					new Date(proposedDeleteDate.asExactJavaDate().getTime() - (MIN_LIFETIME_SECONDS * 1000)));
			final TimestampType minPossibleDeleteDate = new TimestampType(new Date(
					getTransactionTime().getTime() - (MIN_LIFETIME_SECONDS * 1000)));

			// Sanity check: Oldest record must be at least MIN_LIFETIME_SECONDS
			// OLD

			if (proposedDeleteDate.compareTo(minPossibleDeleteDate) == 1) {
				
				
				voltQueueSQL(findOldestRecordBeforex, evictThreshold);
				voltQueueSQL(findOldestRecordCount, proposedDeleteDate);
				VoltTable[] evictRecordResults = voltExecuteSQL();
				evictRecordResults[0].advanceRow();
				evictRecordResults[1].advanceRow();
				TimestampType evictTime = evictRecordResults[0].getTimestampAsTimestamp(0);
				final long howManyToDelete = evictRecordResults[1].getLong("how_many");

				// mark as flushed..
				voltQueueSQL(setFlushDate, evictTime);

				// Evict...
				voltQueueSQL(evictId, evictTime);

				// Delete records for oldest date.
				voltQueueSQL(delStale, proposedDeleteDate);

				VoltTable[] purgeRecordResults = voltExecuteSQL();
				purgeRecordResults[0].advanceRow();
				purgeRecordResults[1].advanceRow();
				purgeRecordResults[2].advanceRow();

				long howManyFlushed = purgeRecordResults[0].getLong(0);
				long howManyEvicted = purgeRecordResults[0].getLong(0);
				long howManyDeleted = purgeRecordResults[1].getLong(0);

				if (howManyToDelete > howManyDeleted) {
					
					System.out.println("Extra Needed "  + howManyToDelete + " " +  howManyDeleted); //TODO

					// Not all the records were deleted. This is because not all
					// of them
					// had a value for flush_date.
					// Flush remaining records and bump their last_use_dates
					voltQueueSQL(evictId, proposedDeleteDate);
					voltQueueSQL(touchLastUseDate, MIN_LIFETIME_SECONDS, proposedDeleteDate);
					VoltTable[] touchRecordResults = voltExecuteSQL(true);
					touchRecordResults[0].advanceRow();
					long howManyLostDeltesFlushed = purgeRecordResults[0].getLong(0);

					if (howManyLostDeltesFlushed != (howManyToDelete - howManyDeleted)) {
						throw new VoltAbortException("Not all lost deltas flushed : " + howManyLostDeltesFlushed);
					}

				}

				final String evictDeleteResultString = "Evict = " + howManyEvicted + " " + evictTime.toString()
						+ " Delete=" + howManyDeleted + " " + proposedDeleteDate.toString() + " Flushed=" + howManyFlushed + " "
						+ evictTime.toString();

				setAppStatusString(evictDeleteResultString);

			} else {
				setAppStatusString("Safe delete time (" + minPossibleDeleteDate.toString() + ") is ahead of proposed delete time (" 
			+ proposedDeleteDate.toString() + ")");
			}

			

		} else {
			// We don't need to evict or delete...
			setAppStatusString("Count = " + results[2].getLong("HOW_MANY") + ", limit=" + maxSizePerPart);
		}

		return results;
	}

}
