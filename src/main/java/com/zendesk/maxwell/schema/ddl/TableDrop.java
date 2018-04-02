package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;
import com.zendesk.maxwell.schema.*;

public class TableDrop extends TableChange {
	final boolean ifExists;

	public TableDrop(String database, String table, boolean ifExists) {
		this.database = database;
		this.table = table;
		this.ifExists = ifExists;
	}

	@Override
	public ResolvedTableDrop resolve(Schema schema) {
		if ( ifExists ) {
			Database d = schema.findDatabase(this.database);
			if ( d == null || !d.hasTable(table) )
				return null;
		}

		return new ResolvedTableDrop(database, table);
	}

}
