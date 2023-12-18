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
import org.voltdb.VoltType;
import org.voltdb.types.TimestampType;

public class Purge extends VoltProcedure {

	public static final SQLStmt countRecs = new SQLStmt("SELECT count(*) how_many FROM subscriber;");

	public static final SQLStmt findOldestLastSeendate = new SQLStmt(
			"select min(last_use_date) last_use_date from subscriber;");

	public static final SQLStmt delStale = new SQLStmt(
			"delete from subscriber where last_use_date = ? and flush_date IS NOT NULL;");

	public static final SQLStmt evictId = new SQLStmt("INSERT INTO subscriber_export (s_id, sub_nbr, "
			+ "f_tinyint , f_smallint , f_integer , f_bigint , f_float , f_decimal , f_geography"
			+ " , f_geography_point , f_varchar , f_varbinary, flush_date, last_use_date) " + "SELECT s_id, sub_nbr, "
			+ "f_tinyint , f_smallint , f_integer , f_bigint , f_float , f_decimal , f_geography"
			+ " , f_geography_point , f_varchar , f_varbinary, current_timestamp, last_use_date"
			+ " FROM subscriber WHERE last_use_date = ? AND flush_date IS NULL ORDER BY s_id;");

	public static final SQLStmt touchLastUseDate = new SQLStmt(
			"UPDATE subscriber SET last_use_date = DATEADD(SECOND, ?, last_use_date), flush_date = CURRENT_TIMESTAMP WHERE last_use_date = ?;");

	private static final int MIN_LIFETIME_SECONDS = 60;

	public VoltTable[] run(long subscriberId, long maxSizePerPart, int passes) throws VoltAbortException {

		final TimestampType latestPossibleDeleteDate = new TimestampType(
				new Date(getTransactionTime().getTime() - (MIN_LIFETIME_SECONDS * 1000)));

		VoltTable[] flushAndPurgeResults = null;

		voltQueueSQL(countRecs);

		VoltTable[] partitionRowCount = voltExecuteSQL();

		// See if we need to evict something...

		partitionRowCount[0].advanceRow();
		long howMany = partitionRowCount[0].getLong("HOW_MANY");

		int delete = 0;
		int flush = 0;
		int push = 0;
		TimestampType proposedDeleteDate = null;

		for (int i = 0; howMany > maxSizePerPart && i < passes; i++) {

			// Delete evicted records older than x

			// Find oldest date
			voltQueueSQL(findOldestLastSeendate);
			proposedDeleteDate = getTimestampFromDB();

			if (proposedDeleteDate.asExactJavaDate().getTime() < latestPossibleDeleteDate.asExactJavaDate().getTime()) {

				// Delete records for oldest date if they have been flushed
				voltQueueSQL(delStale, proposedDeleteDate);

				// .. OR touch LAST_SEEN to postpone deletion and flush them
				// flushed
				voltQueueSQL(evictId, proposedDeleteDate);
				voltQueueSQL(touchLastUseDate, MIN_LIFETIME_SECONDS, proposedDeleteDate);

				flushAndPurgeResults = voltExecuteSQL();

				flushAndPurgeResults[0].advanceRow();
				flushAndPurgeResults[1].advanceRow();
				flushAndPurgeResults[2].advanceRow();

				howMany -= flushAndPurgeResults[0].getLong(0);
				delete += flushAndPurgeResults[0].getLong(0);
				flush += flushAndPurgeResults[1].getLong(0);
				push += flushAndPurgeResults[2].getLong(0);

				setAppStatusString("Delete/Flush/Push@" + proposedDeleteDate.toString() + " = "
						+ flushAndPurgeResults[0].getLong(0) + "/" + flushAndPurgeResults[1].getLong(0) + "/"
						+ flushAndPurgeResults[2].getLong(0) + ";");

			} else {

				setAppStatusString("Safe delete time (" + latestPossibleDeleteDate.toString()
						+ ") is ahead of proposed delete time (" + proposedDeleteDate.toString() + ");");

			}

		}

		VoltTable[] purgeResults = { new VoltTable(new VoltTable.ColumnInfo("DELETE_DATE", VoltType.TIMESTAMP),
				new VoltTable.ColumnInfo("HOW_MANY", VoltType.BIGINT),
				new VoltTable.ColumnInfo("DELETE", VoltType.BIGINT), new VoltTable.ColumnInfo("FLUSH", VoltType.BIGINT),
				new VoltTable.ColumnInfo("PUSH", VoltType.BIGINT)) };

		purgeResults[0].addRow(proposedDeleteDate, howMany, delete, flush, push);

		return purgeResults;
	}

	TimestampType getTimestampFromDB() {

		VoltTable[] theResults = voltExecuteSQL();
		theResults[0].advanceRow();
		TimestampType newTimestampType = theResults[0].getTimestampAsTimestamp(0);

		return newTimestampType;

	}

}
