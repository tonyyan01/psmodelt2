/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package com.hybris.backoffice.core;

import de.hybris.platform.core.GenericSearchField;
import de.hybris.platform.core.GenericSearchOrderBy;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.util.Chars;


public class CollateSearchOrderBy extends GenericSearchOrderBy
{
	private static final String COLLATE = "collate";

	private final String collateType;

	private static final String ASC = "ASC";
	private static final String DESC = "DESC";



	public CollateSearchOrderBy(final GenericSearchField field, final String collateType)
	{
		super(field);
		this.collateType = collateType;
	}

	public CollateSearchOrderBy(final GenericSearchField field, final boolean ascendingOrder, final String collateType)
	{
		super(field, ascendingOrder);
		this.collateType = collateType;
	}


	@Override
	public void toFlexibleSearch(final StringBuilder queryBuffer, final Map<String, String> typeIndexMap,
	                             final Map<String, Object> valueMap)
	{
		getField().toFlexibleSearch(queryBuffer, typeIndexMap, valueMap);
		queryBuffer.append(Chars.SPACE);
		if (StringUtils.isNotEmpty(collateType))
		{
			queryBuffer.append(COLLATE);
			queryBuffer.append(Chars.SPACE);
			queryBuffer.append(collateType);
			queryBuffer.append(Chars.SPACE);
		}
		queryBuffer.append(isAscending() ? ASC : DESC);
	}

	@Override
	public void toPolyglotSearch(final StringBuilder queryBuffer, final Map<String, String> aliasTypeMap,
	                             final Map<String, Object> valueMap)
	{
		getField().toPolyglotSearch(queryBuffer, aliasTypeMap, valueMap);
		queryBuffer.append(Chars.SPACE);
		if (StringUtils.isNotEmpty(collateType))
		{
			queryBuffer.append(COLLATE);
			queryBuffer.append(Chars.SPACE);
			queryBuffer.append(collateType);
			queryBuffer.append(Chars.SPACE);
		}
		queryBuffer.append(isAscending() ? ASC : DESC);
	}

	@Override
	public String toString()
	{
		return "CSOB(" + getField() + "," + isAscending() + "," + collateType + ")";
	}

	@Override
	public boolean equals(final Object o)
	{
		if (this == o) return true;
		if (!(o instanceof CollateSearchOrderBy)) return false;
		if (!super.equals(o)) return false;
		final CollateSearchOrderBy that = (CollateSearchOrderBy) o;
		return Objects.equals(collateType, that.collateType);
	}

	@Override
	public int hashCode()
	{
		return Objects.hash(super.hashCode(), collateType);
	}
}
