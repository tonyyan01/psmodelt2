/**
 * 
 */
package de.hybris.platform.impex.jalo;

import de.hybris.bootstrap.annotations.PerformanceTest;

import org.apache.log4j.Logger;
import org.junit.Test;


/**
 * Performance Tests to compare the stock DocumentIDRegistry with the enhanced implementation.
 * 
 * Empirical results:
 * <table>
 * <tr>
 * <td></td>
 * <td>DocumentIDRegistry</td>
 * <td></td>
 * <td></td>
 * <td>SteroidDocumentIDRegistry</td>
 * <td></td>
 * <td></td>
 * </tr>
 * <tr>
 * <td>Num Docs</td>
 * <td>Register</td>
 * <td>Lookup by PK</td>
 * <td>Lookup by Item Id</td>
 * <td>Register</td>
 * <td>Lookup by PK</td>
 * <td>Lookup by Item Id</td>
 * </tr>
 * <tr>
 * <td>50000</td>
 * <td>27214</td>
 * <td>27345</td>
 * <td>28</td>
 * <td>66</td>
 * <td>3</td>
 * <td>6</td>
 * </tr>
 * <tr>
 * <td>100000</td>
 * <td>125179</td>
 * <td>127521</td>
 * <td>61</td>
 * <td>99</td>
 * <td>10</td>
 * <td>40</td>
 * </tr>
 * <tr>
 * <td>250000</td>
 * <td>790612</td>
 * <td>804570</td>
 * <td>30</td>
 * <td>128</td>
 * <td>13</td>
 * <td>34</td>
 * </tr>
 * <tr>
 * <td>500000</td>
 * <td>dnf</td>
 * <td>dnf</td>
 * <td>dnf</td>
 * <td>182</td>
 * <td>28</td>
 * <td>72</td>
 * </tr>
 * </table>
 * <p>
 * All times in milli secs.
 * 
 * @author mheuer
 * 
 */
@PerformanceTest
public class DocumentIDRegistryPerformanceTest
{
	private static final Logger LOG = Logger.getLogger(DocumentIDRegistryPerformanceTest.class.getName());

	private final String qualifier = "someQualifier";

	@Test
	public void testAdvancedExporterDocumentIDRegistry50k()
	{
		runTests(new SteroidDocumentIDRegistry(), 50000);
	}

	@Test
	public void testAdvancedExporterDocumentIDRegistry100k()
	{
		runTests(new SteroidDocumentIDRegistry(), 100000);
	}

	@Test
	public void testAdvancedExporterDocumentIDRegistry250k()
	{
		runTests(new SteroidDocumentIDRegistry(), 250000);
	}

	@Test
	public void testAdvancedExporterDocumentIDRegistry500k()
	{
		runTests(new SteroidDocumentIDRegistry(), 500000);
	}

	// Test the stock document id registry for comparison
	@Test
	public void testDocumentIDRegistry50k()
	{
		runTests(new DocumentIDRegistry(), 50000);
	}

	@Test
	public void testDocumentIDRegistry100k()
	{
		runTests(new DocumentIDRegistry(), 100000);
	}

	@Test
	public void testDocumentIDRegistry250k()
	{
		runTests(new DocumentIDRegistry(), 250000);
	}

	// aborted this test after waiting 2hrs for completion
	/*
	 * @Test public void testDocumentIDRegistry500k() { runTests(new DocumentIDRegistry(), 500000); }
	 */

	/**
	 * @param docRegistry
	 * @param numDocs
	 */
	private void runTests(final DocumentIDRegistry docRegistry, final int numDocs)
	{
		long startTime = System.currentTimeMillis();
		for (int i = 0; i < numDocs; i++)
		{
			docRegistry.registerPK(qualifier, i);
		}
		long stopTime = System.currentTimeMillis();
		long elapsedTime = stopTime - startTime;
		LOG.info(docRegistry.getClass().getName() + ": Register " + numDocs + " PKs in: " + elapsedTime + "ms");

		startTime = System.currentTimeMillis();
		for (int i = 0; i < numDocs; i++)
		{
			docRegistry.lookupPK(qualifier, i);
		}
		stopTime = System.currentTimeMillis();
		elapsedTime = stopTime - startTime;
		LOG.info(docRegistry.getClass().getName() + ": Lookup " + numDocs + " PKs in: " + elapsedTime + "ms");

		startTime = System.currentTimeMillis();
		for (int i = 0; i < numDocs; i++)
		{
			docRegistry.lookupID(qualifier, qualifier + i);
		}
		stopTime = System.currentTimeMillis();
		elapsedTime = stopTime - startTime;
		LOG.info(docRegistry.getClass().getName() + ": Lookup " + numDocs + " itemids in: " + elapsedTime + "ms");
	}

}
