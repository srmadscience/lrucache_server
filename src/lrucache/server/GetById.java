package lrucache.server;

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

public class GetById extends VoltProcedure {

	public static final SQLStmt getById = new SQLStmt("SELECT * FROM subscriber WHERE s_id = ?;");

	public static final SQLStmt updateDate = new SQLStmt("UPDATE subscriber SET last_use_date =  ?, flush_date = null WHERE s_id = ?;");

	public VoltTable[] run(long subscriberId, long maxSizePerPart) throws VoltAbortException {

		voltQueueSQL(getById, subscriberId);
		voltQueueSQL(updateDate, getTransactionTime(), subscriberId);

		VoltTable[] results = voltExecuteSQL(true);

		// If we didn't find anything request a retry...
		if (results[0].getRowCount() == 0) {
			setAppStatusCode(LRUCallback.CACHE_MISS_STATUS_CODE);
		}

		return results;
	}

}
