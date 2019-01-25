package de.uni_mannheim.informatik.dws.GMKT.util;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.logging.log4j.Logger;

import au.com.bytecode.opencsv.CSVReader;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.GMKT.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.GMKT.match.data.DataSets;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;

/**
 * Represents a set of correspondences (from the schema Match)
 * 
 * @author Alexander Brinkmann (albrinkm@mail.uni-mannheim.de)
 * 
 * @param <RecordType>
 *            the type that represents a record
 */

public class InstanceCorrespondenceSet {

	private static final Logger logger = WinterLogManager.getLogger();

	private ArrayList<Correspondence<MatchableTableRow, MatchableTableColumn>> cors = null;

	/**
	 * Loads correspondences from a file.
	 * 
	 * @param correspondenceFile
	 *            the to load from
	 * @param kb
	 *            the knowledge base that contains the column on the left-hand
	 *            side of the correspondences
	 * @param web
	 *            the web table that contains the column on the right-hand side
	 *            of the correspondences
	 * @throws IOException
	 *             thrown if there is a problem loading the file
	 */
	public void loadInstanceCorrespondences(File correspondenceFile, DataSets kb, DataSets web, String tableName)
			throws IOException {

		CSVReader reader = new CSVReader(new FileReader(correspondenceFile));

		cors = new ArrayList<>();
		String[] values = null;

		while ((values = reader.readNext()) != null) {
			// check if the ids exist in the provided datasets
			if (kb.getRecords().getRecord(values[0]) == null) {
				logger.error(String.format("Row %s not found in knowledge base", values[0]));
				continue;
			}

			if (web.getRecords().getRecord(values[1]) == null) {
				logger.error(String.format("Column %s not found in provided web tables", values[1]));
				continue;
			}

			// load correspondences
			Correspondence<MatchableTableRow, MatchableTableColumn> cor = new Correspondence<>(
					kb.getRecords().getRecord(values[0]), web.getRecords().getRecord(values[1]), 1.0);
			cors.add(cor);
		}

		reader.close();
	}

	public ArrayList<Correspondence<MatchableTableRow, MatchableTableColumn>> getInstanceCorrespondences() {
		return this.cors;
	}

}
