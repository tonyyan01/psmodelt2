/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company. All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package org.hacfix.setup;

import static org.hacfix.constants.HacfixConstants.PLATFORM_LOGO_CODE;

import de.hybris.platform.core.initialization.SystemSetup;

import java.io.InputStream;

import org.hacfix.constants.HacfixConstants;
import org.hacfix.service.HacfixService;


@SystemSetup(extension = HacfixConstants.EXTENSIONNAME)
public class HacfixSystemSetup
{
	private final HacfixService hacfixService;

	public HacfixSystemSetup(final HacfixService hacfixService)
	{
		this.hacfixService = hacfixService;
	}

	@SystemSetup(process = SystemSetup.Process.INIT, type = SystemSetup.Type.ESSENTIAL)
	public void createEssentialData()
	{
		hacfixService.createLogo(PLATFORM_LOGO_CODE);
	}

	private InputStream getImageStream()
	{
		return HacfixSystemSetup.class.getResourceAsStream("/hacfix/sap-hybris-platform.png");
	}
}
