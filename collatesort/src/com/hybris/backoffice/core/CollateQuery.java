/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package com.hybris.backoffice.core;

import de.hybris.platform.core.GenericCondition;
import de.hybris.platform.core.GenericQuery;

public class CollateQuery extends GenericQuery
{
	public CollateQuery(final String typeCode, final GenericCondition condition, final boolean typeExclusive)
	{
		super(typeCode, condition, typeExclusive);
	}

	public CollateQuery(final String typeCode, final GenericCondition condition)
	{
		super(typeCode, condition);
	}

	public CollateQuery(final String typeCode)
	{
		super(typeCode);
	}

	public CollateQuery(final String typeCode, final boolean typeExclusive)
	{
		super(typeCode, typeExclusive);
	}


	@Override
	public boolean isTranslatableToPolyglotDialect()
	{
		return super.isTranslatableToPolyglotDialect() && !hasCollateSearchOrderBy();
	}

	private boolean hasCollateSearchOrderBy()
	{
		return getOrderByList().stream().anyMatch(CollateSearchOrderBy.class::isInstance);
	}
}
