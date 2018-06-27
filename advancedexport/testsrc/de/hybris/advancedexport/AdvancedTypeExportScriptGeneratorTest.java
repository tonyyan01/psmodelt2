/**
 * 
 */
package de.hybris.advancedexport;

import de.hybris.platform.catalog.jalo.Catalog;
import de.hybris.platform.catalog.jalo.CatalogManager;
import de.hybris.platform.core.Registry;
import de.hybris.platform.jalo.type.ComposedType;
import de.hybris.platform.jalo.type.TypeManager;
import de.hybris.platform.servicelayer.ServicelayerTest;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import org.junit.Ignore;
import org.junit.Test;



/**
 * @author brendan
 * 
 */
public class AdvancedTypeExportScriptGeneratorTest extends ServicelayerTest
{

	@Test
	@Ignore
	public void testExportScript()
	{
		Registry.activateMasterTenant();
		final Set<ComposedType> types = new LinkedHashSet();
		types.add((ComposedType) TypeManager.getInstance().getType("SimpleCMSComponent"));
		types.add((ComposedType) TypeManager.getInstance().getType("ContentSlot"));
		types.add((ComposedType) TypeManager.getInstance().getType("CMSSite"));
		final Set<String> blackListedAttributes = new LinkedHashSet();
		blackListedAttributes.add("modifiedtime");
		blackListedAttributes.add("creationtime");

		final Set<String> blackListedTypes = new HashSet();
		blackListedTypes.add("CatalogVersionSyncScheduleMedia");
		blackListedTypes.add("HeaderLibrary");
		blackListedTypes.add("LogFile");
		blackListedTypes.add("ImpExMedia");

		final Catalog catalog = CatalogManager.getInstance().getCatalog("apparel-ukContentCatalog");
		Collections.singleton(catalog.getCatalogVersion("Staged"));

		//		final AdvancedTypeExportScriptGenerator generator = new AdvancedTypeExportScriptGenerator(types, null,
		//				blackListedAttributes, blackListedTypes);

		//System.out.println(generator.generateScript());

	}
}
