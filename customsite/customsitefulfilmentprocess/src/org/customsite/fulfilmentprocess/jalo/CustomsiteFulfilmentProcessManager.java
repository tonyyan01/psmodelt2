/*
 * [y] hybris Platform
 *
 * Copyright (c) 2018 SAP SE or an SAP affiliate company.  All rights reserved.
 *
 * This software is the confidential and proprietary information of SAP
 * ("Confidential Information"). You shall not disclose such Confidential
 * Information and shall use it only in accordance with the terms of the
 * license agreement you entered into with SAP.
 */
package org.customsite.fulfilmentprocess.jalo;

import de.hybris.platform.jalo.JaloSession;
import de.hybris.platform.jalo.extension.ExtensionManager;
import org.customsite.fulfilmentprocess.constants.CustomsiteFulfilmentProcessConstants;

public class CustomsiteFulfilmentProcessManager extends GeneratedCustomsiteFulfilmentProcessManager
{
	public static final CustomsiteFulfilmentProcessManager getInstance()
	{
		ExtensionManager em = JaloSession.getCurrentSession().getExtensionManager();
		return (CustomsiteFulfilmentProcessManager) em.getExtension(CustomsiteFulfilmentProcessConstants.EXTENSIONNAME);
	}
	
}
