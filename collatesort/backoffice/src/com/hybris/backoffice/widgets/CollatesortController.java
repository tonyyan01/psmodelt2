/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved
 */
package com.hybris.backoffice.widgets;

import org.zkoss.zk.ui.Component;
import org.zkoss.zk.ui.select.annotation.WireVariable;
import org.zkoss.zul.Label;

import com.hybris.cockpitng.util.DefaultWidgetController;

import com.hybris.backoffice.services.CollatesortService;


public class CollatesortController extends DefaultWidgetController
{
	private static final long serialVersionUID = 1L;
	private Label label;

	@WireVariable
	private transient CollatesortService collatesortService;

	@Override
	public void initialize(final Component comp)
	{
		super.initialize(comp);
		label.setValue(collatesortService.getHello() + " CollatesortController");
	}
}
