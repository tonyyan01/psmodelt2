/*
 * [y] hybris Platform
 *
 * Copyright (c) 2000-2013 hybris AG
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of hybris
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with hybris.
 * 
 *  
 */
package de.hybris.platform.impex.jalo.cronjob;

import de.hybris.platform.cronjob.jalo.CronJob;
import de.hybris.platform.cronjob.jalo.CronJob.CronJobResult;
import de.hybris.platform.impex.jalo.SteroidDocumentIDRegistry;
import de.hybris.platform.impex.jalo.DocumentIDRegistry;
import de.hybris.platform.impex.jalo.exp.Export;
import de.hybris.platform.impex.jalo.exp.ExportConfiguration;
import de.hybris.platform.impex.jalo.exp.ExportUtils;
import de.hybris.platform.impex.jalo.exp.Exporter;
import de.hybris.platform.impex.jalo.exp.converter.ExportConverter;
import de.hybris.platform.impex.jalo.media.MediaDataTranslator;

import org.apache.log4j.Logger;


/**
 * This job will be used by <code>ImpExExportWizard</code>
 * 
 * 
 */
public class AdvancedExporterImpExExportJob extends GeneratedImpExExportJob
{
	private static final Logger log = Logger.getLogger(AdvancedExporterImpExExportJob.class.getName());

	@Override
	protected CronJobResult performCronJob(final CronJob cronJob)
	{
		boolean result = true;

		final ImpExExportCronJob cron = (ImpExExportCronJob) cronJob;
		cron.setDataExportTarget(ExportUtils.createDataExportTarget(cron.getDataExportMediaCode()));
		cron.setMediasExportTarget(ExportUtils.createMediasExportTarget(cron.getMediasExportMediaCode()));

		try
		{
			final ExportConfiguration config = new ExportConfiguration(cron.getJobMedia(), cron.getMode());
			DocumentIDRegistry registry = new SteroidDocumentIDRegistry();
			
			
			config.setDocumentIDRegistry(registry);
			config.setDataExportTarget(cron.getDataExportTarget());
			config.setMediasExportTarget(cron.getMediasExportTarget());
			config.setFieldSeparator(String.valueOf(cron.getFieldSeparator()));
			config.setCommentCharacter(String.valueOf(cron.getCommentCharacterAsPrimitive()));
			config.setQuoteCharacter(String.valueOf(cron.getQuoteCharacterAsPrimitive()));
			config.setSingleFile(cron.isSingleFileAsPrimitive());

			MediaDataTranslator.setMediaDataHandler(config.getMediaDataHandler());

			final Export export = new Exporter(config).export();
			cron.setExport(export);

			final ExportConverter converter = cron.getConverter();

			if (converter != null)
			{
				converter.setExport(export);
				converter.start();
			}
		}
		catch (final Exception e)
		{
			log.error(e.getMessage(), e);
			result = false;
		}
		finally
		{
			MediaDataTranslator.unsetMediaDataHandler();
		}
		return cronJob.getFinishedResult(result);
	}
}
