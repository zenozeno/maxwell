package com.zendesk.maxwell.schema.ddl;

import com.zendesk.maxwell.MaxwellFilter;

import java.util.Objects;

public abstract class TableChange extends SchemaChange{

	protected String database;
	protected String table;

	@Override
	public boolean isBlacklisted(MaxwellFilter filter) {
		return Objects.nonNull(filter) ? filter.isTableBlacklisted(database, table): false;
	}

	@Override
	public boolean isWhitelisted(MaxwellFilter filter) {
		return Objects.nonNull(filter) ? filter.isTableWhitelisted(database, table): true;
	}
}
