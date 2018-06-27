/**
 * 
 */
package de.hybris.advancedexport;

import de.hybris.advancedexport.jalo.AdvancedTypeExportConfiguration;
import de.hybris.platform.catalog.jalo.CatalogManager;
import de.hybris.platform.catalog.jalo.CatalogVersion;
import de.hybris.platform.impex.constants.ImpExConstants;
import de.hybris.platform.impex.jalo.ImpExManager;
import de.hybris.platform.impex.jalo.exp.generator.ExportScriptGenerator;
import de.hybris.platform.impex.jalo.exp.generator.MigrationScriptModifier;
import de.hybris.platform.jalo.Item;
import de.hybris.platform.jalo.JaloSession;
import de.hybris.platform.jalo.SearchResult;
import de.hybris.platform.jalo.c2l.Language;
import de.hybris.platform.jalo.flexiblesearch.FlexibleSearch;
import de.hybris.platform.jalo.security.UserRight;
import de.hybris.platform.jalo.type.AttributeDescriptor;
import de.hybris.platform.jalo.type.ComposedType;
import de.hybris.platform.jalo.type.RelationDescriptor;
import de.hybris.platform.jalo.type.TypeManager;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;


/**
 * @author brendan
 * 
 */
public class AdvancedTypeExportScriptGenerator extends ExportScriptGenerator
{
	private final Set<ComposedType> exportTypes;
	private final Set<CatalogVersion> catalogVersions;
	private final Set<AttributeDescriptor> blacklistedAttributes;
	private final Set<ComposedType> blacklistedTypes;
	private final boolean exportAttributesAsRelations;
	private AdvancedTypeExportConfiguration exportConfig;

	private static final Logger LOG = Logger.getLogger(AdvancedTypeExportScriptGenerator.class);

	public AdvancedTypeExportScriptGenerator(final List<ComposedType> types, final Set<CatalogVersion> catalogVersions,
			final Set<AttributeDescriptor> blacklistedAttributes, final Set<ComposedType> blacklistedTypes,
			final Set<Language> languages, final boolean exportAttributesAsRelations)
	{
		this.catalogVersions = catalogVersions;
		this.blacklistedAttributes = mergeWithSubDescriptors(blacklistedAttributes);
		this.blacklistedTypes = blacklistedTypes;
		this.exportTypes = findAllSubTypes(types);
		this.setLanguages(languages);
		this.registerScriptModifier(new MigrationScriptModifier());
		this.exportAttributesAsRelations = exportAttributesAsRelations;
	}

	public AdvancedTypeExportScriptGenerator(final AdvancedTypeExportConfiguration exportConfig)
	{
		this(exportConfig.getTypesToExport(), exportConfig.getCatalogVersionsToExport(), exportConfig.getBlacklistedAttributes(),
				exportConfig.getBlacklistedTypes(), exportConfig.getLanguages(), true);
		this.exportConfig = exportConfig;
	}

	protected Set<AttributeDescriptor> mergeWithSubDescriptors(final Set<AttributeDescriptor> blkAttrs)
	{
		final Set<AttributeDescriptor> allAttributeDescriptors = new HashSet<AttributeDescriptor>(blkAttrs);
		for (final AttributeDescriptor ad : blkAttrs)
		{
			allAttributeDescriptors.addAll(ad.getAllSubAttributeDescriptors());
		}
		return allAttributeDescriptors;
	}

	/**
	 * @param types
	 * @return
	 */
	private Set<ComposedType> findAllSubTypes(final List<ComposedType> types)
	{
		final Set<ComposedType> allExportTypes = new LinkedHashSet<ComposedType>();
		for (final ComposedType type : types)
		{
			allExportTypes.addAll(findSubTypes(type));
		}
		return allExportTypes;
	}

	@Override
	protected Set<ComposedType> determineInitialTypes()
	{
		return exportTypes;
	}

	protected Set<ComposedType> findSubTypes(final ComposedType parent)
	{

		assert parent != null;

		final Set<ComposedType> allSubTypes = new LinkedHashSet<ComposedType>();
		if (!parent.isAbstract() && haveItemsForType(parent) && !blacklistedTypes.contains(parent))
		{
			allSubTypes.add(parent);
		}

		if (CollectionUtils.isNotEmpty(parent.getSubTypes()))
		{
			for (final ComposedType type : parent.getSubTypes())
			{
				// append all the sub types of we have any
				allSubTypes.addAll(findSubTypes(type));
			}
		}

		return allSubTypes;
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	protected String generateQueryForType(final ComposedType type)
	{
		// query that returns all items but no subtypes
		String query = "SELECT {PK} FROM {" + type.getCode() + "!}";
		if (isCatalogItem(type) && CollectionUtils.isNotEmpty(catalogVersions))
		{
			query += " WHERE {" + getCatalogVersionAttribute(type).getQualifier() + "} IN (";
			for (final CatalogVersion catalogVersion : catalogVersions)
			{
				query += catalogVersion.getPK().toString() + ",";
			}

			query = StringUtils.removeEnd(query, ",");
			query += ")";
		}

		return query;
	}

	@Override
	protected void writeScript() throws IOException
	{
		writeComment(" -------------------------------------------------------");
		writeComment("# used 'header validation mode' during script generation was: "
				+ ImpExManager.getImportStrictMode().getCode());
		final Locale thisLocale = JaloSession.getCurrentSession().getSessionContext().getLanguage().getLocale();
		writeBeanShell("impex.setLocale( new Locale( \"" + thisLocale.getLanguage() + "\" , \"" + thisLocale.getCountry()
				+ "\" ) )");
		writeComment(" -------------------------------------------------------");

		for (final ComposedType type : getTypes())
		{
			if (LOG.isDebugEnabled())
			{
				LOG.info("generating script statements for type " + type.getCode());
			}

			getScriptWriter().writeSrcLine("");
			writeComment("---- Extension: " + type.getExtensionName() + " ---- Type: " + type.getCode() + " ----");

			writeTargetFileStatement(type, ".csv");
			writeHeader(type);
			writeExportStatement(type, false);
		}
	}

	/**
	 * Write the export statement, we use a flexible search so we don't export any sub types
	 * 
	 * @param type
	 * @param inclSubtypes
	 * @throws IOException
	 */
	@Override
	protected void writeExportStatement(final ComposedType type, final boolean includeSubTypes) throws IOException
	{
		// if we don't have a catalog item or we have one but no catalog versions to filter
		// then we use the normal exportItems method of Exporter
		if (!isCatalogItem(type) || CollectionUtils.isEmpty(catalogVersions))
		{
			writeBeanShell("impex.exportItems( \"" + type.getCode() + "\" , " + false + " )");
		}
		else
		// we have catalog versions then we use the flexible search method
		{
			writeBeanShell("impex.exportItemsFlexibleSearch( \"" + generateQueryForType(type) + "\")");
		}

	}

	/**
	 * Enhances the standard blacklisting functionality by adding a more generic blacklist of common attributes e.g.
	 * creationTime
	 */
	protected boolean isIgnoreColumn(final ComposedType type, final AttributeDescriptor ad)
	{

		final boolean ignore = CollectionUtils.isNotEmpty(blacklistedAttributes) && blacklistedAttributes.contains(ad)
				|| super.isIgnoreColumn(type, ad.getQualifier());
		if (ignore)
		{
			LOG.info("Ignoring qualifier [" + ad.getQualifier() + "] of composed type [" + type.getCode() + "]");
		}
		return ignore;
	}

	protected boolean overrideRelationAsAttribute(@SuppressWarnings("unused") final AttributeDescriptor ad)
	{
		return exportAttributesAsRelations;
	}

	/**
	 * Override to allow relations to be set inside the type, we need this to be able to handle deletes
	 */
	@Override
	protected void writeHeader(final ComposedType type) throws IOException
	{
		// special case USER RIGHT
		if (TypeManager.getInstance().getComposedType(UserRight.class).isAssignableFrom(type))
		{
			getScriptWriter()
					.writeComment(
							"SPECIAL CASE: Type UserRight will be exported with special logic (without header definition), see https://wiki.hybris.com/x/PIFvAg");
		}
		else
		{
			// gather columns string
			final Collection<AttributeDescriptor> attribs = type.getAttributeDescriptorsIncludingPrivate();
			boolean hasUnique = false;
			final Set<String> columns = new TreeSet<String>();
			for (final AttributeDescriptor ad : attribs)
			{
				if (!isIgnoreColumn(type, ad)
						&& !(ad instanceof RelationDescriptor && !(ad.isProperty()))
						&& !ad.getQualifier().equals(Item.PK)
						&& (ad.isInitial() || ad.isWritable())
						&& (!ad.getQualifier().equals("itemtype") || TypeManager.getInstance().getType("EnumerationValue")
								.isAssignableFrom(type))
						// export relations as attributes 
						|| ((!isIgnoreColumn(type, ad) && ad instanceof RelationDescriptor && overrideRelationAsAttribute(ad))))
				{
					if (!ad.isOptional())
					{
						addAdditionalModifier(type.getCode(), ad.getQualifier(), ImpExConstants.Syntax.Modifier.ALLOWNULL, "true");
					}
					if (ad.isUnique()
							|| getAdditionalModifiers(type, ad.getQualifier()).get(ImpExConstants.Syntax.Modifier.UNIQUE) != null)
					{
						hasUnique = true;
					}
					if (!ad.isWritable())
					{
						addAdditionalModifier(type.getCode(), ad.getQualifier(), ImpExConstants.Syntax.Modifier.FORCE_WRITE, "true");
					}
					if (ad.isLocalized())
					{
						for (final Language lang : this.getLanguages())
						{
							columns.add(generateColumn(ad, lang.getIsoCode()));
						}
					}
					else
					{
						columns.add(generateColumn(ad, null));
					}
				}
			}
			columns.addAll(getAdditionalColumns(type));
			final Map line = new HashMap();
			int index = 0;
			final String firstColumn = generateFirstHeaderColumn(type, hasUnique);
			line.put(Integer.valueOf(index), firstColumn);
			index++;

			if (isUseDocumentID())
			{
				line.put(Integer.valueOf(index), ImpExConstants.Syntax.DOCUMENT_ID_PREFIX + "Item");
			}
			else
			{
				line.put(Integer.valueOf(index), Item.PK
						+ (hasUnique ? "" : ImpExConstants.Syntax.MODIFIER_START + ImpExConstants.Syntax.Modifier.UNIQUE
								+ ImpExConstants.Syntax.MODIFIER_EQUAL + Boolean.TRUE + ImpExConstants.Syntax.MODIFIER_END));
			}
			index++;

			for (final Iterator<String> iter = columns.iterator(); iter.hasNext(); index++)
			{
				final String column = iter.next();
				if (column.length() != 0)
				{
					line.put(Integer.valueOf(index), column);
				}
			}
			getScriptWriter().write(line);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see de.hybris.platform.impex.jalo.exp.generator.AbstractScriptGenerator#generateScript()
	 */
	@Override
	public String generateScript()
	{
		final String result = super.generateScript();
		if (exportConfig != null)
		{
			exportConfig.setLastExport(new Date());
		}
		return result;
	}

	/**
	 * Check if we have any items for this type by doing a count on a flexible search
	 * 
	 * @param type
	 * @return
	 */
	protected boolean haveItemsForType(final ComposedType type)
	{
		final String query = generateQueryForType(type);
		final SearchResult result = FlexibleSearch.getInstance().search(query, type.getJaloClass());
		return result.getCount() > 0;
	}

	protected boolean isCatalogItem(final ComposedType type)
	{
		return CatalogManager.getInstance().isCatalogItem(type);
	}

	protected AttributeDescriptor getCatalogVersionAttribute(final ComposedType type)
	{
		return CatalogManager.getInstance().getCatalogVersionAttribute(type);
	}
}
