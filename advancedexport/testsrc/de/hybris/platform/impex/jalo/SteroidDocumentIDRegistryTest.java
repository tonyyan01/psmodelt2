/**
 * 
 */
package de.hybris.platform.impex.jalo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import de.hybris.bootstrap.annotations.UnitTest;

import java.util.ArrayList;

import org.junit.Test;


/**
 * Test class for enhanced DocumentIdRegistry. As part of PLA-13779.
 * 
 * @author mheuer
 * 
 */
@UnitTest
public class SteroidDocumentIDRegistryTest
{
	private final String qualifier = "someQualifier";

	@Test
	public void testRegisteringSinglePK()
	{
		final SteroidDocumentIDRegistry registry = new SteroidDocumentIDRegistry();

		final long pk = 12345l;
		final String docid = registry.registerPK(qualifier, pk);

		assertNotNull("DociId is null", docid);
		assertEquals("Returned PK does not match input", pk, registry.lookupID(qualifier, docid));
	}

	@Test
	public void testRegisteringMultiplePKs()
	{
		final SteroidDocumentIDRegistry registry = new SteroidDocumentIDRegistry();

		final ArrayList<String> docIds = new ArrayList();

		for (int i = 0; i < 10; i++)
		{
			docIds.add(registry.registerPK(qualifier, i));
		}

		for (int i = 0; i < 10; i++)
		{
			final String docId = docIds.get(i);
			assertNotNull("DociId is null", docId);
			assertEquals("Returned PK does not match input", i, registry.lookupID(qualifier, docId));
		}
	}
}
