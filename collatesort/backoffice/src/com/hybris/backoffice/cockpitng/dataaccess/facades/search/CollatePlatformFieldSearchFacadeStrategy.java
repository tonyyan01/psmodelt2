/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */

package com.hybris.backoffice.cockpitng.dataaccess.facades.search;

import static de.hybris.platform.servicelayer.util.ServicesUtil.validateParameterNotNull;

import de.hybris.platform.core.GenericCondition;
import de.hybris.platform.core.GenericConditionList;
import de.hybris.platform.core.GenericQuery;
import de.hybris.platform.core.GenericSearchField;
import de.hybris.platform.core.GenericSearchFieldType;
import de.hybris.platform.core.GenericSearchOrderBy;
import de.hybris.platform.core.Operator;
import de.hybris.platform.core.model.ItemModel;
import de.hybris.platform.core.model.c2l.LanguageModel;
import de.hybris.platform.core.model.enumeration.EnumerationMetaTypeModel;
import de.hybris.platform.core.model.enumeration.EnumerationValueModel;
import de.hybris.platform.core.model.type.AtomicTypeModel;
import de.hybris.platform.core.model.type.AttributeDescriptorModel;
import de.hybris.platform.core.model.type.ComposedTypeModel;
import de.hybris.platform.core.model.type.TypeModel;
import de.hybris.platform.genericsearch.GenericSearchQuery;
import de.hybris.platform.servicelayer.i18n.CommonI18NService;
import de.hybris.platform.util.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.BooleanUtils;

import com.hybris.backoffice.cockpitng.search.builder.ConditionQueryBuilder;
import com.hybris.backoffice.core.CollateQuery;
import com.hybris.backoffice.core.CollateSearchOrderBy;
import com.hybris.cockpitng.search.data.SearchQueryCondition;
import com.hybris.cockpitng.search.data.SearchQueryData;

public class CollatePlatformFieldSearchFacadeStrategy extends DefaultPlatformFieldSearchFacadeStrategy
{
	private CommonI18NService ownCommonI18NService;

	private ConditionQueryBuilder ownGenericMultiConditionQueryBuilder;

	@Override
	protected GenericSearchOrderBy createSortCondition(final GenericQuery query, final String typeCode,
	                                                   final SearchQueryData searchQueryData)
	{
		GenericSearchOrderBy ret = null;
		if (searchQueryData.getSortData() != null && StringUtils.isNotBlank(searchQueryData.getSortData().getSortAttribute()))
		{
			final String qualifier = searchQueryData.getSortData().getSortAttribute();
			final AttributeDescriptorModel attDescriptor = getTypeService().getAttributeDescriptor(typeCode, qualifier);
			if (isAttributeSortable(attDescriptor))
			{
				final boolean asc = searchQueryData.getSortData().isAscending();
				if (attDescriptor.getAttributeType() instanceof EnumerationMetaTypeModel && sorEnumByLocalizedNameEnabled())
				{
					final ComposedTypeModel sortType = (ComposedTypeModel) attDescriptor.getAttributeType();
					ret = createEnumSortOrder(query, sortType.getCode(), qualifier, asc);
				}
				else
				{
					final GenericSearchField field = new GenericSearchField(typeCode,
							searchQueryData.getSortData().getSortAttribute());

					if (isCollateSupportEnabled() && Boolean.TRUE.equals(attDescriptor.getLocalized()))
					{
						field.addFieldType(GenericSearchFieldType.LOCALIZED);
						ret = new CollateSearchOrderBy(
								field,
								searchQueryData.getSortData().isAscending(), collate());
					}
					else
					{
						ret = new GenericSearchOrderBy(field,searchQueryData.getSortData().isAscending());
					}
				}
			}
		}
		return ret;
	}

	@Override
	protected GenericSearchQuery buildQuery(final SearchQueryData searchQueryData)
	{
		validateParameterNotNull(searchQueryData, "Parameter 'searchQueryData' must not be null!");
		validateParameterNotNull(searchQueryData.getSearchType(), "Parameter 'searchQueryData.typeCode' must not be empty!");

		final String typeCode = searchQueryData.getSearchType();
		final GenericQuery query = new CollateQuery(typeCode);

		if (CollectionUtils.isNotEmpty(searchQueryData.getConditions()))
		{
			final List<GenericCondition> conditions = new ArrayList<>();
			final List<GenericCondition> filteringConditions = new ArrayList<>();

			for (final SearchQueryCondition condition : searchQueryData.getConditions())
			{
				final List<GenericCondition> genericConditions = ownGenericMultiConditionQueryBuilder.buildQuery(query, typeCode,
						condition, searchQueryData);
				if (condition.isFilteringCondition())
				{
					filteringConditions.addAll(genericConditions);
				}
				else
				{
					conditions.addAll(genericConditions);
				}
			}
			final GenericConditionList allConditions = GenericCondition.createConditionList(conditions,
					getConditionsOperator(searchQueryData));
			final GenericConditionList allFilteringConditions = GenericCondition.createConditionList(filteringConditions,
					Operator.AND);

			final Optional<GenericCondition> optional = joinConditionsWithFilteringConditions(allConditions, allFilteringConditions);
			optional.ifPresent(query::addCondition);
		}

		final GenericSearchOrderBy orderBy = createSortCondition(query, typeCode, searchQueryData);
		if (orderBy != null)
		{
			query.addOrderBy(orderBy);
		}

		query.setTypeExclusive(!searchQueryData.isIncludeSubtypes());
		return new GenericSearchQuery(query);
	}

	protected boolean isCollateSupportEnabled()
	{
		if (!Config.isSQLServerUsed())
		{
			return false;
		}

		final LanguageModel currentLanguage = ownCommonI18NService.getCurrentLanguage();
		if (currentLanguage == null)
		{
			return false;
		}

		return StringUtils.isNotEmpty(Config.getParameter("collate." + currentLanguage.getIsocode()));
	}

	protected String collate()
	{
		return Config.getParameter("collate." + ownCommonI18NService.getCurrentLanguage().getIsocode());
	}

	@Override
	protected GenericSearchOrderBy createEnumSortOrder(final GenericQuery query, final String sortType, final String qualifier,
	                                                   final boolean asc)
	{
		final String aliasCode = String.format("%s_sort", sortType);

		query.addOuterJoin(sortType, aliasCode, GenericCondition.createJoinCondition(new GenericSearchField(qualifier),
				new GenericSearchField(aliasCode, ItemModel.PK)));

		final GenericSearchOrderBy orderBy;

		if (isCollateSupportEnabled())
		{
			orderBy = new CollateSearchOrderBy(
					new GenericSearchField(aliasCode, EnumerationValueModel.NAME), asc, collate());
		}
		else
		{
			orderBy = new GenericSearchOrderBy(new GenericSearchField(aliasCode, EnumerationValueModel.NAME), asc);
		}
		orderBy.getField().addFieldType(GenericSearchFieldType.LOCALIZED);

		final LanguageModel langModel = ownCommonI18NService.getCurrentLanguage();
		orderBy.getField().setLanguagePK(langModel.getPk());
		return orderBy;
	}

	private boolean isAttributeSortable(final AttributeDescriptorModel attributeDescriptor)
	{
		boolean ret = false;
		if (attributeDescriptor != null)
		{
			final TypeModel attributeType = attributeDescriptor.getAttributeType();
			ret = attributeType instanceof AtomicTypeModel || attributeType instanceof ComposedTypeModel
					|| BooleanUtils.toBoolean(attributeDescriptor.getLocalized());
		}
		return ret;
	}

	public void setOwnCommonI18NService(final CommonI18NService ownCommonI18NService)
	{
		this.ownCommonI18NService = ownCommonI18NService;
	}

	public void setOwnGenericMultiConditionQueryBuilder(
			final ConditionQueryBuilder ownGenericMultiConditionQueryBuilder)
	{
		this.ownGenericMultiConditionQueryBuilder = ownGenericMultiConditionQueryBuilder;
	}
}
