package de.uni_mannheim.informatik.dws.GMKT.match.data;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;

//import objectexplorer.MemoryMeasurer;
//import objectexplorer.ObjectGraphMeasurer;
//import objectexplorer.ObjectGraphMeasurer.Footprint;
// import de.metanome.algorithm_integration.AlgorithmExecutionException;

import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleDataSet;
import de.uni_mannheim.informatik.dws.winter.model.FusibleParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.ProgressReporter;
import de.uni_mannheim.informatik.dws.winter.utils.WinterLogManager;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.CsvTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

public class DataSets {
	private static final Logger logger = WinterLogManager.getLogger();

	// data that will be matched: records and schema
	private FusibleDataSet<MatchableTableRow, MatchableTableColumn> records = new FusibleParallelHashedDataSet<>();
	private DataSet<MatchableTableColumn, MatchableTableColumn> schema = new ParallelHashedDataSet<>();


	// matched web tables and their key columns
	private HashMap<Integer, MatchableTableColumn> keys = new HashMap<>();

	// translation for web table identifiers
	private HashMap<String, String> columnHeaders = new HashMap<>();

	// translation from table name to table id
	private HashMap<String, Integer> tableIndices = new HashMap<>();

	// lookup for tables by id
	private HashMap<Integer, Table> tables = null;

	private boolean measure = false;

	public void setMeasureMemoryUsage(boolean measure) {
		this.measure = measure;
	}

	public void setKeepTablesInMemory(boolean keep) {
		if (keep) {
			tables = new HashMap<>();
		} else {
			tables = null;
		}
	}

	private boolean convertValues = true;

	/**
	 * @param convertValues
	 *            the convertValues to set
	 */
	public void setConvertValues(boolean convertValues) {
		this.convertValues = convertValues;
	}

	private boolean inferSchema = true;

	/**
	 * @param inferSchema
	 *            the inferSchema to set
	 */
	public void setInferSchema(boolean inferSchema) {
		this.inferSchema = inferSchema;
	}

	public static DataSets loadWebTables(File location, boolean keepTablesInMemory, boolean inferSchema,
			boolean convertValues, boolean serialise) throws FileNotFoundException {
		return loadWebTables(location, keepTablesInMemory, inferSchema, convertValues, serialise, 0);
	}

	public static DataSets loadWebTables(File location, boolean keepTablesInMemory, boolean inferSchema,
			boolean convertValues, boolean serialise, int firstTableId) throws FileNotFoundException {
		// look for serialised version
		File ser = new File(location.getParentFile(), location.getName() + ".bin");

		if (ser.exists() && serialise) {
			DataSets web = DataSets.deserialise(ser);

			if (web != null) {
				web.printLoadStats();
				return web;
			}
		}

		DataSets web = new DataSets();
		web.setKeepTablesInMemory(keepTablesInMemory);
		web.setInferSchema(inferSchema);
		web.setConvertValues(convertValues);
		web.load(location, firstTableId);

		// Serialise only if we loaded more than one table (otherwise we would
		// generate .bin files in folders that contain many web tables which
		// would lead to problem when loading the whole folder)
		if (web.getRecords().size() > 1 && serialise) {
			web.serialise(ser);
		}

		return web;
	}

	private static final Pattern tableIdPattern = Pattern.compile("(\\d+)\\.json");

	public void load(File location, int firstTableId) {
		CsvTableParser csvParser = new CsvTableParser();
		JsonTableParser jsonParser = new JsonTableParser();
		jsonParser.setRunGC(false);

		csvParser.setConvertValues(convertValues);

		jsonParser.setConvertValues(convertValues);
		jsonParser.setInferSchema(inferSchema);

		List<File> webFiles = null;

		if (location.isDirectory()) {
			webFiles = Arrays.asList(location.listFiles());
		} else {
			webFiles = Arrays.asList(new File[] { location });
		}

		ProgressReporter progress = new ProgressReporter(webFiles.size(), "Loading Web Tables");

		int nextTableId = firstTableId;

		Queue<File> toLoad = new LinkedList<>(webFiles);
		// for(File f : webFiles) {
		while (toLoad.size() > 0) {
			File f = toLoad.poll();

			if (f.isDirectory()) {
				toLoad.addAll(Arrays.asList(f.listFiles()));
				progress = new ProgressReporter(toLoad.size(), "Loading Web Tables", progress.getProcessedElements());
			} else {

				// System.out.println("Loading Web Table " + f.getName());
				try {
					Table web = null;

					if (f.getName().endsWith("csv")) {
						web = csvParser.parseTable(f);
					} else if (f.getName().endsWith("json")) {
						web = jsonParser.parseTable(f);
					} else {
						logger.info(String.format("Unknown table format: %s", f.getName()));
					}

					if (web == null) {
						continue;
					}

					int tblIdx = 0;
					Matcher matcher = tableIdPattern.matcher(f.getName());
					if (matcher.matches()) {
						String name = matcher.group(1);
						tblIdx = Integer.parseInt(name);

						if (tblIdx < firstTableId || tables.containsKey(tblIdx)) {
							// if a minimum table id was specified, we cannot
							// assign any id that is lower
							tblIdx = nextTableId++;
						}
					} else {
						tblIdx = nextTableId++;
					}
					if (tblIdx >= nextTableId) {
						nextTableId = tblIdx + 1;
					}

					// System.out.println(String.format("Table %s was assigned
					// id #%d", f.getName(), tblIdx));

					if (tables != null) {
						if (tables.containsKey(tblIdx)) {
							logger.error(String.format("Table id #%d for table %s already assigned to table %s", tblIdx,
									f.getName(), tables.get(tblIdx).getPath()));
						}

						tables.put(tblIdx, web);
						web.setTableId(tblIdx);
					}

					for (TableColumn tc : web.getSchema().getRecords()) {
						logger.info(String.format("{%s} [%d] %s (%s)", web.getPath(), tc.getColumnIndex(),
								tc.getHeader(), tc.getDataType()));
					}

					tableIndices.put(web.getPath(), tblIdx);

					// list schema
					LinkedList<MatchableTableColumn> schemaColumns = new LinkedList<>();
					TableColumn identifierColumn = null;
					for (TableColumn c : web.getSchema().getRecords()) {
						MatchableTableColumn mc = new MatchableTableColumn(tblIdx, c);
						schema.add(mc);
						schemaColumns.add(mc);
						columnHeaders.put(mc.getIdentifier(), c.getHeader());
						if (c.getHeader().equals("uri")) {
							identifierColumn = c;
						}
						if (web.hasSubjectColumn() && web.getSubjectColumnIndex() == c.getColumnIndex()) {
							keys.put(mc.getTableId(), mc);
						}
					}

					// // create the matchable table record
					// MatchableTable mt = new MatchableTable(web,
					// Q.toArrayFromCollection(schemaColumns,
					// MatchableTableColumn.class));
					// tableRecords.add(mt);

					// list records
					for (TableRow r : web.getRows()) {
						MatchableTableRow row = new MatchableTableRow(r, tblIdx,
								Q.toArrayFromCollection(schemaColumns, MatchableTableColumn.class));
						
						if (identifierColumn != null) {
							row.updateIdentifierFromColumn(identifierColumn.getColumnIndex());
						}
						
						records.add(row);
					}

					// tblIdx++;
				} catch (Exception e) {
					logger.error(String.format("Could not load table %s", f.getAbsolutePath()));
					e.printStackTrace();
				}

				progress.incrementProgress();
				progress.report();
			}
		}

		printLoadStats();
	}

	public void reloadRecords() {

		records = new FusibleParallelHashedDataSet<>();

		for (Table t : getTables().values()) {

			MatchableTableColumn[] tableSchema = Q.toArrayFromCollection(
					schema.where((c) -> c.getTableId() == t.getTableId()).get(), MatchableTableColumn.class);
			
			MatchableTableColumn identifierColumn = null;
			for (MatchableTableColumn c : schema.get()) {

				if (c.getHeader().equals("uri")) {
					identifierColumn = c;
				}
			}
			
			for (TableRow r : t.getRows()) {

				MatchableTableRow row = new MatchableTableRow(r, t.getTableId(), tableSchema);
				
				if (identifierColumn != null) {
					row.updateIdentifierFromColumn(identifierColumn.getColumnIndex());
				}
				
				records.add(row);
			}

		}

	}

	public void reloadSchema() {

		schema = new FusibleParallelHashedDataSet<>();

		for (Table t : getTables().values()) {

			LinkedList<MatchableTableColumn> schemaColumns = new LinkedList<>();
			for (TableColumn c : t.getSchema().getRecords()) {
				MatchableTableColumn mc = new MatchableTableColumn(t.getTableId(), c);
				schema.add(mc);
				schemaColumns.add(mc);
				columnHeaders.put(mc.getIdentifier(), c.getHeader());
				if (t.hasSubjectColumn() && t.getSubjectColumnIndex() == c.getColumnIndex()) {
					keys.put(mc.getTableId(), mc);
				}
			}

		}
	}

	public void printSchemata(boolean printTypes) {
		if (tables != null && tables.size() > 0) {
			for (Table t : tables.values()) {
				logger.info(String.format("Table #%d %s {%s} / %d rows / %d columns", t.getTableId(), t.getPath(),
						StringUtils.join(Q.project(t.getColumns(),
								(c) -> String.format("[%d]%s%s", c.getColumnIndex(), c.getHeader(),
										(printTypes ? ":" + c.getDataType().toString() : ""))),
								","),
						t.getSize(), t.getColumns().size()));
			}
		}
	}

	public void printDensityReport() {
		if (tables != null && tables.size() > 0) {

			logger.info("*** Web Table Densities ***");

			for (Table t : tables.values()) {

				logger.info(String.format("\t%s", t.getPath()));

				Map<TableColumn, Integer> valuesByColumn = new HashMap<>();
				Map<TableColumn, Set<Object>> domainByColumn = new HashMap<>();

				for (TableRow r : t.getRows()) {

					for (TableColumn c : t.getColumns()) {

						if (r.get(c.getColumnIndex()) != null) {
							MapUtils.increment(valuesByColumn, c);

							Set<Object> domain = domainByColumn.get(c);
							if (domain == null) {
								domain = new HashSet<>();
								domainByColumn.put(c, domain);
							}
							domain.add(r.get(c.getColumnIndex()));
						}

					}

				}

				for (TableColumn c : t.getColumns()) {

					Integer values = valuesByColumn.get(c);
					if (values == null) {
						values = 0;
					}
					double density = values / (double) t.getRows().size();

					Set<Object> domain = domainByColumn.get(c);
					int domainSize = domain == null ? 0 : domain.size();
					double uniqueness = domainSize / (double) t.getRows().size();
					logger.info(String.format("\t\t%s: %.6f (%d/%d) - uniqueness: %.6f (%d/%d)", c, density, values,
							t.getRows().size(), uniqueness, domainSize, t.getRows().size()));

				}

			}

		}
	}

	void printLoadStats() {
		logger.info(String.format("%,d Web Tables Instances", records.size()));
		logger.info(String.format("%,d Web Tables Schema Elements", schema.size()));
		if (tables != null) {
			logger.info(String.format("%,d Web Tables", tables.size()));
		}

		if (measure) {
			logger.info("Measuring Memory Usage");
			measure(records, "Web Tables Dataset");
			measure(schema, "Web Tables Schema");
			measure(columnHeaders, "Web Tables Column Headers");
			measure(keys, "Web Table Keys");
		}
	}

	void measure(Object obj, String name) {
		// long memory = MemoryMeasurer.measureBytes(obj);
		//
		// System.out.println(String.format("%s Memory Size: %,d", name,
		// memory));
		//
		// Footprint footprint = ObjectGraphMeasurer.measure(obj);
		// System.out.println(String.format("%s Graph Footprint: \n\tObjects:
		// %,d\n\tReferences %,d", name, footprint.getObjects(),
		// footprint.getReferences()));
	}

	public FusibleDataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
		return records;
	}

	public void setRecords(FusibleDataSet<MatchableTableRow, MatchableTableColumn> records) {
		this.records = records;
	}

	public DataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
		return schema;
	}

	public HashMap<Integer, MatchableTableColumn> getKeys() {
		return keys;
	}

	// /**
	// * @return the tableRecords
	// */
	// public DataSet<MatchableTable, MatchableTableColumn> getTableRecords() {
	// return tableRecords;
	// }

	/**
	 * A map (Column Identifier) -> (Column Header)
	 * 
	 * @return
	 */
	public HashMap<String, String> getColumnHeaders() {
		return columnHeaders;
	}

	/**
	 * @return the tables
	 */
	public HashMap<Integer, Table> getTables() {
		return tables;
	}

	/**
	 * A map (Table Path) -> (Table Id)
	 * 
	 * @return the tableIndices
	 */
	public HashMap<String, Integer> getTableIndices() {
		return tableIndices;
	}

	public static DataSets deserialise(File location) throws FileNotFoundException {
		logger.info("Deserialising Web Tables");

		Kryo kryo = new Kryo();

		try {
			// kryo.setReferences(false); // fix for large objects: Otherwise a
			// NegativeArraySizeException is thrown
			Input input = new Input(new FileInputStream(location));
			DataSets web = kryo.readObject(input, DataSets.class);
			input.close();

			return web;
		} catch (Exception e) {
			logger.error("Error during de-serialisation!");
			e.printStackTrace();
			return null;
		}
	}

	public void serialise(File location) throws FileNotFoundException {
		logger.info("Serialising Web Tables");

		Kryo kryo = new Kryo();
		try {
			// kryo.setReferences(false); // fix for large objects: Otherwise a
			// NegativeArraySizeException is thrown
			Output output = new Output(new FileOutputStream(location));
			kryo.writeObject(output, this);
			output.close();
		} catch (Exception e) {
			logger.error("Error during serialisation!");
			e.printStackTrace();
		}
	}

	public static void writeTables(Collection<Table> tables, File jsonLocation, File csvLocation) throws IOException {
		for (Table t : tables) {
			if (jsonLocation != null) {
				JsonTableWriter jtw = new JsonTableWriter();
				jtw.write(t, new File(jsonLocation, t.getPath()));
			}

			if (csvLocation != null) {
				CSVTableWriter tw = new CSVTableWriter();
				tw.write(t, new File(csvLocation, t.getPath()));
			}
		}
	}
}
