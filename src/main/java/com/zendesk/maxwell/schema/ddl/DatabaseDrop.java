package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.schema.Schema;

public class DatabaseDrop extends DatabaseChange {
	public boolean ifExists;

	public DatabaseDrop(String database, boolean ifExists) {
		this.database = database;
		this.ifExists = ifExists;
	}

	@Override
	public ResolvedDatabaseDrop resolve(Schema schema) throws InvalidSchemaError {
		if ( ifExists && !schema.hasDatabase(database) )
			return null;

		return new ResolvedDatabaseDrop(this.database);
	}

}
