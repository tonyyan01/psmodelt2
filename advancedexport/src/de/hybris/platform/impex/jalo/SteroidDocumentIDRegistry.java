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
package de.hybris.platform.impex.jalo;

import de.hybris.platform.jalo.JaloSystemException;
import de.hybris.platform.util.CSVReader;
import de.hybris.platform.util.CSVWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.collections4.BidiMap;
import org.apache.commons.collections4.bidimap.DualHashBidiMap;
import org.apache.log4j.Logger;


/**
 * <b>NOTE:</b> This class extends the stock hybris {@link de.hybris.platform.impex.jalo.DocumentIDRegistry} with a new
 * data structure to ensure support large data sets (250k records+) for export. This is done using a bidirectional
 * hashmap, whereas the stock implementation iterates over all map entries and checks the value for the PK. <br>
 * As part of PLA-13779.
 * <p>
 * 
 * Represents a registry for document ID's. A document ID is a placeholder for PK's of not unique items which are
 * currently not known. For example, assume the import of customers with additional specification of their delivery
 * address. When importing a customer, the delivery address itself is not yet imported and because addresses do not have
 * unique attributes you can not declare the reference to address. So you can use a placeholder (the document ID) to
 * reference a address. When importing the address you also declare a special column for assigning the ID. While import,
 * the mapping between ID's and PK's will be created and managed with usage of this registry class. It holds for each
 * qualifier a map with the existing mappings. These mappings can be made persistent with usage of readers and writers.
 * (see {@link #SteroidDocumentIDRegistry(CSVReader, CSVWriter)}). For declaring a column holding ID's you have to use a
 * qualifier started with an &amp;. The ID is unique within the scope of this qualifier.
 * <p>
 * Example script:<br>
 * <code>
 * INSERT_UPDATE Customer; uid[unique=true]; defaultPaymentAddress( &payAddress ); defaultShipmentAddress( &delAddress )<br>
 * ; andy ; payAddress0 ; delAddress1 ;<br>
 * ; rigge; payAddress1 ; delAddress0 ;<br>
 * INSERT Address; &payAddress; &delAddress ; owner( Customer.uid ) ; department<br>
 * ; payAddress0 ; delAddress0 ; andy  ; a1<br>
 * ; payAddress1 ; delAddress1 ; andy  ; a2<br></code>
 * <p>
 * This script is used for importing a customer with referenced addresses. When importing, first the customer will be
 * created but marked as unresolved (here the unresolved of the <code>ImpExImportReader</code> is meant, because the
 * used document IDs can not be found in registry. When importing the addresses, the specified IDs will be registered
 * with the PKs of the addresses and in second import cycle the customers can be finished.<br>
 * When using the example script for export (without value lines), the customers will be exported first. Because there
 * are no IDs used for the referenced addresses, new IDs will be generated, but stored in the storage with unresolved
 * IDs. When exporting the addresses the registry recognizes the already used but unresolved IDs for the addresses and
 * moves them to the resolved storage. So you can be sure, when you export an item A which references to an item B, that
 * item B is also exported by simply calling <code>hasUnresolvedIDs</code> after the export process
 * 
 * @author jkim
 */
public class SteroidDocumentIDRegistry extends DocumentIDRegistry
{
	/**
	 * A map holding all resolved qualifiers and there id mappings.
	 */
	private Map<String, BidiMap<String, Long>> resolvedQualifiers;
	/**
	 * A map holding all qualifiers and there id mappings which are unresolved.
	 */
	private Map<String, BidiMap<String, Long>> unresolvedQualifiers;
	/**
	 * A writer for making each mapping persistent and reusable.
	 */
	private final CSVWriter writer;
	/**
	 * A reader for import of mappings written with the writer.
	 */
	private final CSVReader reader;
	/**
	 * Logger instance.
	 */
	private static final Logger LOGGER = Logger.getLogger(SteroidDocumentIDRegistry.class.getName());

	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock readLock = lock.readLock();
	private final Lock writeLock = lock.writeLock();


	/**
	 * Enumeration representing the two modes of storing an ID => resolved and unresolved. So when you use mode RESOLVED
	 * the context will always use storage <code>resolvedQualifiers</code>. When using UNRESOLVED the context always use
	 * storage <code>unresolvedQualifiers</code>.<br>
	 * The resolved storage is always used within import process.
	 */
	public static enum MODE
	{
		/**
		 * An ID is resolved and therefore has to be stored in RESOLVED storage when the ID is used within import process,
		 * or in export process and the item with mapped pk is exported.
		 */
		RESOLVED,
		/**
		 * An ID is unresolved when the ID is used within export process and an item A references to the item B with
		 * mapped pk, but item B is not exported until yet.
		 */
		UNRESOLVED;
	}

	/* Constructor */
	/**
	 * Creates a registry without import of mappings and without export of new mappings. You can start an import manually
	 * using {@link #importIDs(CSVReader)} later.
	 */
	public SteroidDocumentIDRegistry()
	{
		this(null, null);
	}

	/**
	 * Creates a registry without import of mappings and with export of new mappings. You have to call
	 * {@link #closeStreams()} before usage of the exported stream data. You can start an import manually using
	 * {@link #importIDs(CSVReader)} later.
	 * 
	 * @param documentIDWriter
	 *           writer to which new mappings will be exported.
	 */
	public SteroidDocumentIDRegistry(final CSVWriter documentIDWriter)
	{
		this(null, documentIDWriter);
	}

	/**
	 * Creates a registry with import of mappings and without export of new mappings.
	 * 
	 * @param documentIDReader
	 *           the reader from which mappings will be read and imported when instantiating.
	 */
	public SteroidDocumentIDRegistry(final CSVReader documentIDReader)
	{
		this(documentIDReader, null);
	}

	/**
	 * Creates a registry with import of mappings and with export of new mappings. You have to call
	 * {@link #closeStreams()} before usage of the exported stream data.
	 * 
	 * @param documentIDReader
	 *           the reader from which mappings will be read and imported when instantiating.
	 * @param documentIDWriter
	 *           writer to which new mappings will be exported.
	 */
	public SteroidDocumentIDRegistry(final CSVReader documentIDReader, final CSVWriter documentIDWriter)
	{
		this.writer = documentIDWriter;
		this.reader = documentIDReader;
		// start import
		importIDs(documentIDReader);
	}

	/* Import/Export */
	/**
	 * Imports a set of ID<->PK mappings from given reader.
	 * 
	 * @param documentIDReader
	 *           the reader with which the mappings will be read
	 */
	@Override
	public void importIDs(final CSVReader documentIDReader)
	{
		if (documentIDReader != null)
		{
			while (documentIDReader.readNextLine())
			{
				final Map<Integer, String> line = documentIDReader.getLine();
				// check amount of separated fields. It have to be exact three for a correct mapping (qualifier;ID;PK)
				if (line.size() == 3)
				{
					// add the mapping to the registry
					try
					{
						addID(line.get(Integer.valueOf(0)), line.get(Integer.valueOf(1)), Long.parseLong(line.get(Integer.valueOf(2))),
								MODE.RESOLVED);
					}
					catch (final NumberFormatException e)
					{
						LOGGER.warn("Can not parse pk of line, will skip it:" + line, e);
					}
					catch (final ImpExException e)
					{
						LOGGER.warn("Can not create entry for line, will skip it:" + e.getMessage(), e);
					}
				}
				else
				{
					LOGGER.warn("Can not interprete line, will skip it:" + line);
				}
			}
		}
	}

	/**
	 * Exports an existing mapping to the local writer.
	 * 
	 * @param qualifier
	 *           the qualifier to which the ID is related
	 * @param documentID
	 *           the ID
	 * @param pk
	 *           the PK to which the ID is mapped
	 */
	@Override
	protected void exportID(final String qualifier, final String documentID, final long pk)
	{
		if (this.writer != null)
		{
			try
			{
				final Map<Integer, String> toWrite = new HashMap<Integer, String>();
				toWrite.put(Integer.valueOf(0), qualifier);
				toWrite.put(Integer.valueOf(1), documentID);
				toWrite.put(Integer.valueOf(2), Long.toString(pk));
				writer.write(toWrite);
			}
			catch (final IOException e)
			{
				LOGGER.warn(
						"error while writing document id entry: " + qualifier + ":" + documentID + ":" + pk + ":" + e.getMessage(), e);
			}
		}
	}

	/* Registration */
	/**
	 * Registers a new mapping ID<->PK to the registry. If the pair is already existent, it will be replaced. Used for
	 * import to register the ID of an item.
	 * 
	 * @param qualifier
	 *           the qualifier to which the ID is related
	 * @param documentID
	 *           the new ID
	 * @param pk
	 *           the PK to which the ID will be mapped
	 * @return the document ID mapped to the given PK (same ID as given one)
	 * @throws ImpExException
	 *            the ID already exists and maps to another PK
	 */
	@Override
	public String registerID(final String qualifier, final String documentID, final long pk) throws ImpExException
	{
		addID(qualifier, documentID, pk, MODE.RESOLVED);
		return documentID;
	}

	/**
	 * Registers a new mapping ID<->PK to the registry. Checks if there is always an ID for the given PK in scope of the
	 * qualifier, if not, a new ID will be generated. Used for export to give each item a unique ID related to its PK.
	 * 
	 * @param qualifier
	 *           qualifier in whose scope the ID will be generated
	 * @param pk
	 *           the pk for which an ID mapping is needed
	 * @return the (new) ID mapped to the given PK
	 */
	@Override
	public String registerPK(final String qualifier, final long pk)
	{
		// check whether an ID already exist
		String id = getID(qualifier, pk, MODE.RESOLVED);
		if (id == null)
		{
			id = getID(qualifier, pk, MODE.UNRESOLVED);
			if (id != null)
			{
				resolveID(qualifier, id, pk);
			}
		}
		// if not, generate a new unresolved one ..
		if (id == null)
		{
			id = calculateNextID(qualifier);
			try
			{
				addID(qualifier, id, pk, MODE.RESOLVED);
			}
			catch (final ImpExException e)
			{
				throw new JaloSystemException(e, "Error while adding document id where I have already checked containing", 0);
			}
		}
		return id;
	}

	/* Lookup */
	/**
	 * Checks if there is always an ID for the given PK in scope of the qualifier, if not, a new ID will be generated
	 * marked as unresolved. Used for export to use the correct ID for an item reference. Is the referenced item not
	 * exported the lookup will generate an unresolved ID and will be resolved when item is exported.
	 * 
	 * @param qualifier
	 *           qualifier in whose scope the ID will be generated
	 * @param pk
	 *           the pk for which an ID mapping is needed
	 * @return the (new) ID mapped to the given PK
	 */
	@Override
	public String lookupPK(final String qualifier, final long pk)
	{
		// check whether an resolved ID already exists
		String id = getID(qualifier, pk, MODE.RESOLVED);
		// check whether an unresolved ID already exists
		if (id == null)
		{
			id = getID(qualifier, pk, MODE.UNRESOLVED);
		}
		// if not, generate a new unresolved one ..
		if (id == null)
		{
			id = calculateNextID(qualifier);
			try
			{
				addID(qualifier, id, pk, MODE.UNRESOLVED);
			}
			catch (final ImpExException e)
			{
				throw new JaloSystemException(e, "Error while adding document id where I have already checked containing", 0);
			}
		}
		return id;
	}

	/**
	 * Gets the PK to which the given ID is mapped. Used while import for resolving an ID to an PK.
	 * 
	 * @param qualifier
	 *           scope of the ID
	 * @param documentID
	 *           ID for which the PK will be returned
	 * @return the PK to the given ID or -1
	 */
	@Override
	public long lookupID(final String qualifier, final String documentID)
	{
		return getPK(qualifier, documentID, MODE.RESOLVED);
	}

	/**
	 * Checks whether the registry contains an mapping with the given ID in scope of the qualifier. Checks within
	 * resolved and unresolved mappings.
	 * 
	 * @param qualifier
	 *           the scope in which the ID is valid
	 * @param documentID
	 *           the ID which will be checked
	 * @return true if a mapping with the given ID to a PK is existent
	 */
	@Override
	public boolean containsID(final String qualifier, final String documentID)
	{
		return getPK(qualifier, documentID, MODE.RESOLVED) == -1 && getPK(qualifier, documentID, MODE.UNRESOLVED) == -1;
	}

	/**
	 * Checks whether the registry contains an mapping with the given PK in scope of the qualifier. Checks within
	 * resolved and unresolved mappings.
	 * 
	 * @param qualifier
	 *           the scope which will be searched
	 * @param pk
	 *           the PK which will be checked for an ID
	 * @return true if a mapping with the given PK to a ID is existent within the qualifier
	 */
	@Override
	public boolean containsPK(final String qualifier, final long pk)
	{
		return getID(qualifier, pk, MODE.RESOLVED) == null && getID(qualifier, pk, MODE.UNRESOLVED) == null;
	}

	/* public Utilities */
	/**
	 * Checks whether there are unresolved IDs in registry. An ID is unresolved, if it is used while export and the item
	 * with mapped pk is not export until yet.
	 * 
	 * @return true if there are unresolved IDs, otherwise false
	 */
	@Override
	public boolean hasUnresolvedIDs()
	{
		return !getQualifiersMap(MODE.UNRESOLVED).isEmpty();
	}

	/**
	 * Checks a given ID if it is in unresolved mode. An ID is unresolved, if it is used while export and the item with
	 * mapped pk is not exported until yet.
	 * 
	 * @param qualifier
	 *           the scope which will be searched
	 * @param documentID
	 *           the ID which will be checked
	 * @return true if the given ID in given scope is in unresolved mode, otherwise false
	 */
	@Override
	public boolean isUnresolved(final String qualifier, final String documentID)
	{
		return getPK(qualifier, documentID, MODE.UNRESOLVED) != -1;
	}

	/**
	 * Checks a given ID if it is in resolved mode. An ID is resolved, if it is used while import, or export and the item
	 * with mapped pk is already exported
	 * 
	 * @param qualifier
	 *           the scope which will be searched
	 * @param documentID
	 *           the ID which will be checked
	 * @return true if the given ID in given scope is in resolved mode, otherwise false
	 */
	@Override
	public boolean isResolved(final String qualifier, final String documentID)
	{
		return getPK(qualifier, documentID, MODE.RESOLVED) != -1;
	}

	/**
	 * Gathers all unresolved IDs and prints each one in a list entry. So one entry of the resulting list represents one
	 * unresolved ID with format <code>qualifier+separator+id+separator+pk</code>.
	 * 
	 * @param separator
	 *           the separator symbol used between the attributes of a documentID in resulting list
	 * @return list of all unresolved ID with one ID per entry
	 */
	@Override
	public List<String> printUnresolvedIDs(final String separator)
	{
		readLock.lock();
		try
		{
			List<String> ret;
			if (hasUnresolvedIDs())
			{
				ret = new ArrayList<String>();
				for (final Entry<String, BidiMap<String, Long>> qualifierEntry : getQualifiersMap(MODE.UNRESOLVED).entrySet())
				{
					for (final Entry<String, Long> idEntry : qualifierEntry.getValue().entrySet())
					{
						ret.add("Scope:" + qualifierEntry.getKey() + separator + "ID:" + idEntry.getKey() + separator + "PK:"
								+ idEntry.getValue());
					}
				}
			}
			else
			{
				ret = Collections.EMPTY_LIST;
			}
			return ret;
		}
		finally
		{
			readLock.unlock();
		}
	}

	/**
	 * Closes all used streams (for import and export).
	 */
	@Override
	public void closeStreams()
	{
		// close writer
		if (this.writer != null)
		{
			try
			{
				this.writer.close();
			}
			catch (final IOException e)
			{
				LOGGER.warn("Error while closing csv writer: " + e.getMessage());
			}
		}
		// close reader
		if (this.reader != null)
		{
			try
			{
				this.reader.close();
			}
			catch (final IOException e)
			{
				LOGGER.warn("Error while closing csv reader: " + e.getMessage());
			}
		}
	}

	/* private Utilities */
	/**
	 * Gets the ID to which the given PK is mapped.
	 * 
	 * @param qualifier
	 *           scope in which will be searched
	 * @param pk
	 *           the PK for which the ID will be returned
	 * @param mode
	 *           defines the ID storage in which will be searched
	 * @return the ID to the given PK or null
	 */
	protected String getID(final String qualifier, final long pk, final MODE mode)
	{
		readLock.lock();
		try
		{
			final BidiMap<String, Long> qualifierMap = getQualifiersMap(mode).get(qualifier);
			if (qualifierMap == null)
			{
				return null;
			}
			// original look-up method. Not very performing for large data sets.
			/*
			 * for (final Entry<String, Long> e : qualifierMap.entrySet()) { if (e.getValue().longValue() == pk) { return
			 * e.getKey(); } }
			 */

			final String key = qualifierMap.inverseBidiMap().get(Long.valueOf(pk));

			return key;
		}
		finally
		{
			readLock.unlock();
		}
	}

	/**
	 * Gets the PK to which the given ID is mapped. Used while import for resolving an ID to an PK.
	 * 
	 * @param qualifier
	 *           scope of the ID
	 * @param documentID
	 *           ID for which the PK will be returned
	 * @param mode
	 *           defines the ID storage in which will be searched
	 * @return the PK to the given ID or -1
	 */
	protected long getPK(final String qualifier, final String documentID, final MODE mode)
	{
		readLock.lock();
		try
		{
			// get the related qualifier map
			final Map<String, Long> qualifierMap = getQualifiersMap(mode).get(qualifier);
			if (qualifierMap == null)
			{
				return -1;
			}
			// get the PK
			final Long ret = qualifierMap.get(documentID);
			return ret == null ? -1 : ret.longValue();
		}
		finally
		{
			readLock.unlock();
		}
	}

	/**
	 * Generates a new unused ID containing the given qualifier name. Generation uses the qualifier name concatenated
	 * with sum of sizes of the storages. Afterwards it increments the resulting number while the id is already used.
	 * 
	 * @param qualifier
	 *           text which will be part of the generated id
	 * @return a new id in format <code>qualifier+number</code>
	 */
	@Override
	protected String calculateNextID(final String qualifier)
	{
		readLock.lock();
		try
		{
			int number = 0;
			String id;
			Map map = getQualifiersMap(MODE.RESOLVED).get(qualifier);
			if (map != null)
			{
				number = number + map.size();
			}
			map = getQualifiersMap(MODE.UNRESOLVED).get(qualifier);
			if (map != null)
			{
				number = number + map.size();
			}
			id = qualifier + number;
			while (getQualifiersMap(MODE.RESOLVED).containsKey(id) || getQualifiersMap(MODE.UNRESOLVED).containsKey(id))
			{
				number++;
				id = qualifier + number;
			}
			return id;
		}
		finally
		{
			readLock.unlock();
		}
	}

	/**
	 * Adds a new mapping ID<->PK to the registry. If the pair is already existent, it will be replaced. T
	 * 
	 * @param qualifier
	 *           the qualifier to which the ID is related
	 * @param documentID
	 *           the new ID
	 * @param pk
	 *           the PK to which the ID will be mapped
	 * @param mode
	 *           defines the ID storage where the ID will be added
	 * @throws ImpExException
	 *            the ID already exists and maps to another OK
	 */
	protected void addID(final String qualifier, final String documentID, final long pk, final MODE mode) throws ImpExException
	{
		writeLock.lock();
		try
		{
			BidiMap<String, Long> qualifierMap = getQualifiersMap(mode).get(qualifier);
			if (qualifierMap == null)
			{
				qualifierMap = new DualHashBidiMap<String, Long>();
				getQualifiersMap(mode).put(qualifier, qualifierMap);
			}
			// add the new mapping
			final Long old = qualifierMap.put(documentID, Long.valueOf(pk));
			// check if there was an old mapping with another PK
			if (old != null && old.longValue() != pk)
			{
				qualifierMap.put(documentID, old);
				throw new ImpExException("id " + documentID + " for qualifier " + qualifier + " already used for item with pk=" + old);
			}
			// export the mapping
			if (mode == MODE.RESOLVED)
			{
				exportID(qualifier, documentID, pk);
			}
		}
		finally
		{
			writeLock.unlock();
		}

	}

	/**
	 * Resloves an unresolved ID. Tries to remove the ID from the unresolved ID storage and to add it to the resolved
	 * storage.
	 * 
	 * @param qualifier
	 *           the qualifier to which the ID is related
	 * @param documentID
	 *           the id which will be moved from unresolved to resolved
	 * @param pk
	 *           the mapped pk of the ID
	 */
	@Override
	protected void resolveID(final String qualifier, final String documentID, final long pk)
	{
		writeLock.lock();
		try
		{

			// get the related qualifier map
			final Map<String, Long> qualifierMap = this.getQualifiersMap(MODE.UNRESOLVED).get(qualifier);
			if (qualifierMap != null)
			{
				final Long storedPK = qualifierMap.remove(documentID);
				if (storedPK.longValue() != pk)
				{
					throw new JaloSystemException("Document ID <" + qualifier + "," + documentID + "," + pk
							+ "> will be resolved, but existing unresolved entry is ,mapped to PK=" + storedPK.longValue());
				}
				if (qualifierMap.isEmpty())
				{
					this.getQualifiersMap(MODE.UNRESOLVED).remove(qualifier);
				}
			}
			try
			{
				addID(qualifier, documentID, pk, MODE.RESOLVED);
			}
			catch (final ImpExException e)
			{
				throw new JaloSystemException(e, "Error while adding document id where I have already checked containing", 0);
			}
		}
		finally
		{
			writeLock.unlock();
		}
	}

	/**
	 * Returns the storage related to the given mode.
	 * 
	 * @param mode
	 *           the mode to which the associated storage is needed
	 * @return storage associated to given mode
	 */
	protected Map<String, BidiMap<String, Long>> getQualifiersMap(final MODE mode)
	{
		if (mode == MODE.RESOLVED)
		{
			if (this.resolvedQualifiers == null)
			{
				this.resolvedQualifiers = new HashMap<String, BidiMap<String, Long>>();
			}
			return this.resolvedQualifiers;
		}
		else
		{
			if (this.unresolvedQualifiers == null)
			{
				this.unresolvedQualifiers = new HashMap<String, BidiMap<String, Long>>();
			}
			return this.unresolvedQualifiers;
		}
	}
}
