/**
 * 
 */
package de.hybris.advancedexport;

import static java.io.File.separatorChar;

import de.hybris.advancedexport.jalo.AdvancedTypeExportConfiguration;
import de.hybris.advancedexport.model.AdvancedTypeExportCronJobModel;
import de.hybris.platform.cronjob.enums.CronJobResult;
import de.hybris.platform.cronjob.enums.CronJobStatus;
import de.hybris.platform.impex.model.ImpExMediaModel;
import de.hybris.platform.servicelayer.config.ConfigurationService;
import de.hybris.platform.servicelayer.cronjob.AbstractJobPerformable;
import de.hybris.platform.servicelayer.cronjob.PerformResult;
import de.hybris.platform.servicelayer.impex.ExportConfig;
import de.hybris.platform.servicelayer.impex.ExportConfig.ValidationMode;
import de.hybris.platform.servicelayer.impex.ExportResult;
import de.hybris.platform.servicelayer.impex.ExportService;
import de.hybris.platform.servicelayer.impex.ImpExResource;
import de.hybris.platform.servicelayer.impex.impl.StreamBasedImpExResource;
import de.hybris.platform.util.CSVConstants;

import java.io.File;
import java.io.IOException;
import java.io.StringBufferInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;


/**
 * @author brendan
 * 
 */
@SuppressWarnings("deprecation")
public class AdvancedTypeExportJob extends AbstractJobPerformable<AdvancedTypeExportCronJobModel>
{

	// private ModelService modelService;
	private ExportService exportService;

	@Autowired
	private ConfigurationService configurationService;

	private static final Logger LOG = Logger.getLogger(AdvancedTypeExportJob.class);

	@Override
	public PerformResult perform(final AdvancedTypeExportCronJobModel cron)
	{
		final String exportString = generateExportScript(cron);

		if (LOG.isInfoEnabled())
		{
			LOG.info("***** Exporting types ********");
			LOG.info("Using script.......");
			LOG.info(exportString);
		}

		final ImpExResource exportResource = new StreamBasedImpExResource(new StringBufferInputStream(exportString),
				CSVConstants.HYBRIS_ENCODING);

		final ExportConfig exportConfig = new ExportConfig();
		exportConfig.setFailOnError(false);
		exportConfig.setValidationMode(ValidationMode.RELAXED);
		exportConfig.setScript(exportResource);
		exportConfig.setSingleFile(true);

		cron.setExportScript(exportResource.getMedia());
		modelService.save(cron);

		final ExportResult result = exportService.exportData(exportConfig);
		if (result.isSuccessful())
		{
			copyExportedMediaToExportDir(result);
			cron.setExportedData(result.getExportedData());
			cron.setExportedMedia(result.getExportedMedia());
            modelService.save(cron);
		}
		

		return result.isSuccessful() ? new PerformResult(CronJobResult.SUCCESS, CronJobStatus.FINISHED) : new PerformResult(
				CronJobResult.FAILURE, CronJobStatus.FINISHED);
	}

	/**
	 * @param result
	 */
	private void copyExportedMediaToExportDir(final ExportResult result)
	{
		final String exportDir = configurationService.getConfiguration().getString("advancedexport.export.dir");
		if (StringUtils.isNotBlank(exportDir))
		{
			final File dir = new File(exportDir);
			try
			{
				if (!dir.exists())
				{
					if (!dir.mkdirs())
					{
						LOG.error("Directory " + exportDir + " does not exist. Unable to create it.");
					}
				}
				else if (dir.isDirectory() && dir.canWrite())
				{
					final Path dirPath = Paths.get(dir.getAbsolutePath());
					copyExportedMediaFile(dirPath, result.getExportedData());
					copyExportedMediaFile(dirPath, result.getExportedMedia());
				}
				else
				{
					LOG.error("Unable to write to " + exportDir + " or it is not a directory");
				}
			}
			catch (final IOException ioe)
			{
				LOG.error("Unable to copy generated script files to " + exportDir, ioe);
			}
		}
	}

	private void copyExportedMediaFile(final Path targetDir, final ImpExMediaModel impexModel) throws IOException
	{
		Files.copy(Paths.get(findRealMediaPath(impexModel)), targetDir.resolve(impexModel.getRealFileName()));
	}

	private String findRealMediaPath(final ImpExMediaModel impexModel)
	{
		final StringBuilder sb = new StringBuilder(64);
		sb.append(configurationService.getConfiguration().getProperty("HYBRIS_DATA_DIR")).append(separatorChar);
		sb.append("media").append(separatorChar);
		sb.append("sys_").append(impexModel.getFolder().getTenantId());
		sb.append(separatorChar).append(impexModel.getLocation());
		return sb.toString();
	}

	/**
	 * Use a custom export script generator to dynamically create an export script
	 * 
	 * @param cron
	 * @return script payload
	 */
	protected String generateExportScript(final AdvancedTypeExportCronJobModel cron)
	{
		final AdvancedTypeExportScriptGenerator generator = new AdvancedTypeExportScriptGenerator(
				(AdvancedTypeExportConfiguration) modelService.getSource(cron.getExportConfiguration()));
		return generator.generateScript();
	}

	/**
	 * @param exportService
	 *           the exportService to set
	 */
	public void setExportService(final ExportService exportService)
	{
		this.exportService = exportService;
	}

}
