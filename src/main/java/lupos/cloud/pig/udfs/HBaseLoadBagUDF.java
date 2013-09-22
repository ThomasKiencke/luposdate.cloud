/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * 
 * This Class is a Copy of org.apache.pig.backend.hadoop.hbase.HBaseStorage 
 * with some changes to enable some features. 
 */
package lupos.cloud.pig.udfs;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.HashMap;
import java.util.Properties;
import java.util.SortedMap;
import java.util.TreeMap;

import lupos.cloud.hbase.bulkLoad.HBaseKVMapper;
import lupos.cloud.hbase.filter.BitvectorFilter;
import lupos.cloud.testing.BitvectorManager;

import org.joda.time.DateTime;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.ColumnPaginationFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.WhileMatchFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.ColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.OrderedLoadFunc;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;
import org.apache.pig.backend.hadoop.hbase.HBaseStorage;
import org.apache.pig.backend.hadoop.hbase.HBaseTableInputFormat.HBaseTableIFBuilder;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.PigContext;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

/**
 * Diese UDF Funktion ist eine angepasste Variante der originalen HBaseStorage()
 * UDF Funktion. Diese wurde zum größten Teil übernommen und an einigen Stellen
 * angepasst. Eine wichtige erweiterung ist die Möglichkeit nur einen bestimmten
 * rowKey zu laden, anstatt den gesamten Datenbestand einer Tabelle.
 */
public class HBaseLoadBagUDF extends LoadFunc implements StoreFuncInterface,
		LoadPushDown, OrderedLoadFunc {

	private static final Log LOG = LogFactory.getLog(HBaseLoadBagUDF.class);

	private final static String STRING_CASTER = "UTF8StorageConverter";
	private final static String BYTE_CASTER = "HBaseBinaryConverter";
	private final static String CASTER_PROPERTY = "pig.hbase.caster";
	private final static String ASTERISK = "*";
	private final static String COLON = ":";
	private final static String HBASE_SECURITY_CONF_KEY = "hbase.security.authentication";
	private final static String HBASE_CONFIG_SET = "hbase.config.set";
	private final static String HBASE_TOKEN_SET = "hbase.token.set";

	private List<ColumnInfo> columnInfo_ = Lists.newArrayList();
	private HTable m_table;

	// Use JobConf to store hbase delegation token
	private JobConf m_conf;
	private RecordReader reader;
	private RecordWriter writer;
	private TableOutputFormat outputFormat = null;
	private Scan scan;
	private String contextSignature = null;

	private final CommandLine configuredOptions_;
	private final static Options validOptions_ = new Options();
	private final static CommandLineParser parser_ = new GnuParser();

	private boolean loadRowKey_;
	private String delimiter_;
	private boolean ignoreWhitespace_;
	private final long limit_;
	private final int caching_;
	private final boolean noWAL_;
	private final long minTimestamp_;
	private final long maxTimestamp_;
	private final long timestamp_;

	protected transient byte[] gt_;
	protected transient byte[] gte_;
	protected transient byte[] lt_;
	protected transient byte[] lte_;

	private LoadCaster caster_;

	private ResourceSchema schema_;
	private RequiredFieldList requiredFieldList;

	private BitSet bitvector1 = null;
	private BitSet bitvector2 = null;
	private FileSystem fs = null;
	private Path bitvectorPath1 = null;
	private Path bitvectorPath2 = null;

	private static void populateValidOptions() {
		validOptions_.addOption("loadKey", false, "Load Key");
		validOptions_.addOption("gt", true,
				"Records must be greater than this value "
						+ "(binary, double-slash-escaped)");
		validOptions_
				.addOption("lt", true,
						"Records must be less than this value (binary, double-slash-escaped)");
		validOptions_.addOption("gte", true,
				"Records must be greater than or equal to this value");
		validOptions_.addOption("lte", true,
				"Records must be less than or equal to this value");
		validOptions_.addOption("caching", true,
				"Number of rows scanners should cache");
		validOptions_.addOption("limit", true, "Per-region limit");
		validOptions_.addOption("delim", true, "Column delimiter");
		validOptions_.addOption("ignoreWhitespace", true,
				"Ignore spaces when parsing columns");
		validOptions_
				.addOption(
						"caster",
						true,
						"Caster to use for converting values. A class name, "
								+ "HBaseBinaryConverter, or Utf8StorageConverter. For storage, casters must implement LoadStoreCaster.");
		validOptions_
				.addOption(
						"noWAL",
						false,
						"Sets the write ahead to false for faster loading. To be used with extreme caution since this could result in data loss (see http://hbase.apache.org/book.html#perf.hbase.client.putwal).");
		validOptions_.addOption("minTimestamp", true,
				"Record must have timestamp greater or equal to this value");
		validOptions_.addOption("maxTimestamp", true,
				"Record must have timestamp less then this value");
		validOptions_.addOption("timestamp", true,
				"Record must have timestamp equal to this value");

	}

	/**
	 * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or
	 * store the cells of the provided columns.
	 * 
	 * @param columnList
	 *            columnlist that is a presented string delimited by space
	 *            and/or commas. To retreive all columns in a column family
	 *            <code>Foo</code>, specify a column as either <code>Foo:</code>
	 *            or <code>Foo:*</code>. To fetch only columns in the CF that
	 *            start with <I>bar</I>, specify <code>Foo:bar*</code>. The
	 *            resulting tuple will always be the size of the number of
	 *            tokens in <code>columnList</code>. Items in the tuple will be
	 *            scalar values when a full column descriptor is specified, or a
	 *            map of column descriptors to values when a column family is
	 *            specified.
	 * 
	 * @throws ParseException
	 *             when unable to parse arguments
	 * @throws IOException
	 */
//	public HBaseLoadBagUDF(String columnList, String rowKey)
//			throws ParseException, IOException {
//		this(columnList, "", rowKey, null, null);
//	}

	public HBaseLoadBagUDF(String columnList, String optString, String rowKey,
			String bitvectorPath) throws ParseException, IOException {
		this(columnList, optString, rowKey, new Path(bitvectorPath), null);

		// this.bitvectorPath1 = new Path(bitvectorPath);

	}

	public HBaseLoadBagUDF(String columnList, String optString, String rowKey,
			String bitvectorPath1, String bitvectorPath2)
			throws ParseException, IOException {
		this(columnList, optString, rowKey, new Path(bitvectorPath1), new Path(bitvectorPath2));

//		this.bitvectorPath1 = new Path(bitvectorPath1);
//		this.bitvectorPath2 = new Path(bitvectorPath2);

	}

	private BitSet readBloomfilter(Path path) {
		BitSet bitvector = null;
		try {
			if (fs == null) {
				fs = FileSystem.get(HBaseConfiguration.create());
			}
			FSDataInputStream input = fs.open(path);
			bitvector = fromByteArray(ByteStreams.toByteArray(input));
			// bitvector = longToBitSet(input.readLong());
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bitvector;
	}

	private byte[] readBloomfilterToByte(Path path) {
		byte[] bitvector = null;
		try {
			if (fs == null) {
				fs = FileSystem.get(new Configuration());
			}
			FSDataInputStream input = fs.open(path);
			bitvector = ByteStreams.toByteArray(input);
			// bitvector = longToBitSet(input.readLong());
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return bitvector;
	}

	public static BitSet fromByteArray(byte[] bytes) {
		BitSet bits = new BitSet();
		for (int i = 0; i < bytes.length * 8; i++) {
			if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
				bits.set(i);
			}
		}
		return bits;
	}

	/**
	 * Constructor. Construct a HBase Table LoadFunc and StoreFunc to load or
	 * store.
	 * 
	 * @param columnList
	 * @param optString
	 *            Loader options. Known options:
	 *            <ul>
	 *            <li>-loadKey=(true|false) Load the row key as the first column
	 *            <li>-gt=minKeyVal
	 *            <li>-lt=maxKeyVal
	 *            <li>-gte=minKeyVal
	 *            <li>-lte=maxKeyVal
	 *            <li>-limit=numRowsPerRegion max number of rows to retrieve per
	 *            region
	 *            <li>-delim=char delimiter to use when parsing column names
	 *            (default is space or comma)
	 *            <li>-ignoreWhitespace=(true|false) ignore spaces when parsing
	 *            column names (default true)
	 *            <li>-caching=numRows number of rows to cache (faster scans,
	 *            more memory).
	 *            <li>-noWAL=(true|false) Sets the write ahead to false for
	 *            faster loading.
	 *            <li>-minTimestamp= Scan's timestamp for min timeRange
	 *            <li>-maxTimestamp= Scan's timestamp for max timeRange
	 *            <li>-timestamp= Scan's specified timestamp
	 *            <li>-caster=(HBaseBinaryConverter|Utf8StorageConverter)
	 *            Utf8StorageConverter is the default To be used with extreme
	 *            caution, since this could result in data loss (see
	 *            http://hbase.apache.org/book.html#perf.hbase.client.putwal).
	 *            </ul>
	 * @throws ParseException
	 * @throws IOException
	 */
	public HBaseLoadBagUDF(String columnList, String optString, String rowKey,
			Path bitvectorPath1, Path bitvectorPath2) throws ParseException,
			IOException {
		populateValidOptions();

		this.bitvectorPath1 = bitvectorPath1;
		this.bitvectorPath2 = bitvectorPath2;

		String[] optsArr = optString.split(" ");
		try {
			configuredOptions_ = parser_.parse(validOptions_, optsArr);
		} catch (ParseException e) {
			HelpFormatter formatter = new HelpFormatter();
			formatter
					.printHelp(
							"[-loadKey] [-gt] [-gte] [-lt] [-lte] [-columnPrefix] [-caching] [-caster] [-noWAL] [-limit] [-delim] [-ignoreWhitespace] [-minTimestamp] [-maxTimestamp] [-timestamp]",
							validOptions_);
			throw e;
		}

		loadRowKey_ = configuredOptions_.hasOption("loadKey");

		delimiter_ = ",";
		if (configuredOptions_.getOptionValue("delim") != null) {
			delimiter_ = configuredOptions_.getOptionValue("delim");
		}

		ignoreWhitespace_ = true;
		if (configuredOptions_.hasOption("ignoreWhitespace")) {
			String value = configuredOptions_
					.getOptionValue("ignoreWhitespace");
			if (!"true".equalsIgnoreCase(value)) {
				ignoreWhitespace_ = false;
			}
		}

		columnInfo_ = parseColumnList(columnList, delimiter_, ignoreWhitespace_);

		String defaultCaster = UDFContext.getUDFContext()
				.getClientSystemProps()
				.getProperty(CASTER_PROPERTY, STRING_CASTER);
		String casterOption = configuredOptions_.getOptionValue("caster",
				defaultCaster);
		if (STRING_CASTER.equalsIgnoreCase(casterOption)) {
			caster_ = new Utf8StorageConverter();
		} else if (BYTE_CASTER.equalsIgnoreCase(casterOption)) {
			caster_ = new HBaseBinaryConverter();
		} else {
			try {
				caster_ = (LoadCaster) PigContext
						.instantiateFuncFromSpec(casterOption);
			} catch (ClassCastException e) {
				LOG.error("Configured caster does not implement LoadCaster interface.");
				throw new IOException(e);
			} catch (RuntimeException e) {
				LOG.error("Configured caster class not found.", e);
				throw new IOException(e);
			}
		}
		LOG.debug("Using caster " + caster_.getClass());

		caching_ = Integer.valueOf(configuredOptions_.getOptionValue("caching",
				"100"));
		limit_ = Long.valueOf(configuredOptions_.getOptionValue("limit", "-1"));
		noWAL_ = configuredOptions_.hasOption("noWAL");

		if (configuredOptions_.hasOption("minTimestamp")) {
			minTimestamp_ = Long.parseLong(configuredOptions_
					.getOptionValue("minTimestamp"));
		} else {
			minTimestamp_ = Long.MIN_VALUE;
		}

		if (configuredOptions_.hasOption("maxTimestamp")) {
			maxTimestamp_ = Long.parseLong(configuredOptions_
					.getOptionValue("maxTimestamp"));
		} else {
			maxTimestamp_ = Long.MAX_VALUE;
		}

		if (configuredOptions_.hasOption("timestamp")) {
			timestamp_ = Long.parseLong(configuredOptions_
					.getOptionValue("timestamp"));
		} else {
			timestamp_ = 0;
		}

		initScan(rowKey);
	}

	/**
	 * Returns UDFProperties based on <code>contextSignature</code>.
	 */
	private Properties getUDFProperties() {
		return UDFContext.getUDFContext().getUDFProperties(this.getClass(),
				new String[] { contextSignature });
	}

	/**
	 * @return <code> contextSignature + "_projectedFields" </code>
	 */
	private String projectedFieldsName() {
		return contextSignature + "_projectedFields";
	}

	/**
	 * 
	 * @param columnList
	 * @param delimiter
	 * @param ignoreWhitespace
	 * @return
	 */
	private List<ColumnInfo> parseColumnList(String columnList,
			String delimiter, boolean ignoreWhitespace) {
		List<ColumnInfo> columnInfo = new ArrayList<ColumnInfo>();

		// Default behavior is to allow combinations of spaces and delimiter
		// which defaults to a comma. Setting to not ignore whitespace will
		// include the whitespace in the columns names
		String[] colNames = columnList.split(delimiter);
		if (ignoreWhitespace) {
			List<String> columns = new ArrayList<String>();

			for (String colName : colNames) {
				String[] subColNames = colName.split(" ");

				for (String subColName : subColNames) {
					subColName = subColName.trim();
					if (subColName.length() > 0)
						columns.add(subColName);
				}
			}

			colNames = columns.toArray(new String[columns.size()]);
		}

		for (String colName : colNames) {
			columnInfo.add(new ColumnInfo(colName));
		}

		return columnInfo;
	}

	private void initScan(String rowKey) throws IOException {
		byte[] bvector1 = readBloomfilterToByte(bitvectorPath1);
		byte[] bvector2 = "0".getBytes();
		if (bitvectorPath2 != null) {
			bvector2 = readBloomfilterToByte(bitvectorPath2);

		}

		scan = new Scan();
		
		if (bitvectorPath2 == null) {
//			scan.setFilter(new BitvectorFilter(bvector1));
		} else {
//			scan.setFilter(new BitvectorFilter(bvector1, bvector2));
		}

		// scan.setRaw(true);

		scan.setBatch(1);

		scan.setCaching(10000);

		if (rowKey != null) {
			scan.setStartRow(Bytes.toBytes(rowKey));
			// add random string because stopRow is exclusiv
			scan.setStopRow(Bytes.toBytes(rowKey + "z"));
		}

		// Map-reduce jobs should not run with cacheBlocks
		scan.setCacheBlocks(false);

		// Set filters, if any.
		if (configuredOptions_.hasOption("gt")) {
			gt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_
					.getOptionValue("gt")));
			addRowFilter(CompareOp.GREATER, gt_);
			scan.setStartRow(gt_);
		}
		if (configuredOptions_.hasOption("lt")) {
			lt_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_
					.getOptionValue("lt")));
			addRowFilter(CompareOp.LESS, lt_);
			scan.setStopRow(lt_);
		}
		if (configuredOptions_.hasOption("gte")) {
			gte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_
					.getOptionValue("gte")));
			scan.setStartRow(gte_);
		}
		if (configuredOptions_.hasOption("lte")) {
			lte_ = Bytes.toBytesBinary(Utils.slashisize(configuredOptions_
					.getOptionValue("lte")));
			byte[] lt = increment(lte_);
			if (LOG.isDebugEnabled()) {
				LOG.debug(String
						.format("Incrementing lte value of %s from bytes %s to %s to set stop row",
								Bytes.toString(lte_), toString(lte_),
								toString(lt)));
			}

			if (lt != null) {
				scan.setStopRow(increment(lte_));
			}

			// The WhileMatchFilter will short-circuit the scan after we no
			// longer match. The
			// setStopRow call will limit the number of regions we need to scan
			addFilter(new WhileMatchFilter(new RowFilter(
					CompareOp.LESS_OR_EQUAL, new BinaryComparator(lte_))));
		}
		if (configuredOptions_.hasOption("minTimestamp")
				|| configuredOptions_.hasOption("maxTimestamp")) {
			scan.setTimeRange(minTimestamp_, maxTimestamp_);
		}
		if (configuredOptions_.hasOption("timestamp")) {
			scan.setTimeStamp(timestamp_);
		}

		// if the group of columnInfos for this family doesn't contain a prefix,
		// we don't need
		// to set any filters, we can just call addColumn or addFamily. See
		// javadocs below.
		boolean columnPrefixExists = false;
		for (ColumnInfo columnInfo : columnInfo_) {
			if (columnInfo.getColumnPrefix() != null) {
				columnPrefixExists = true;
				break;
			}
		}

		if (!columnPrefixExists) {
			addFiltersWithoutColumnPrefix(columnInfo_);
		} else {
			addFiltersWithColumnPrefix(columnInfo_);
		}
	}

	/**
	 * If there is no column with a prefix, we don't need filters, we can just
	 * call addColumn and addFamily on the scan
	 */
	private void addFiltersWithoutColumnPrefix(List<ColumnInfo> columnInfos) {
		for (ColumnInfo columnInfo : columnInfos) {
			if (columnInfo.columnName != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding column to scan via addColumn with cf:name = "
							+ Bytes.toString(columnInfo.getColumnFamily())
							+ ":" + Bytes.toString(columnInfo.getColumnName()));
				}
				scan.addColumn(columnInfo.getColumnFamily(),
						columnInfo.getColumnName());
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Adding column family to scan via addFamily with cf:name = "
							+ Bytes.toString(columnInfo.getColumnFamily()));
				}
				scan.addFamily(columnInfo.getColumnFamily());
			}
		}
	}

	/**
	 * If we have a qualifier with a prefix and a wildcard (i.e. cf:foo*), we
	 * need a filter on every possible column to be returned as shown below.
	 * This will become very inneficient for long lists of columns mixed with a
	 * prefixed wildcard.
	 * 
	 * FilterList - must pass ALL of - FamilyFilter - AND a must pass ONE
	 * FilterList of - either Qualifier - or ColumnPrefixFilter
	 * 
	 * If we have only column family filters (i.e. cf:*) or explicit column
	 * descriptors (i.e., cf:foo) or a mix of both then we don't need filters,
	 * since the scan will take care of that.
	 */
	private void addFiltersWithColumnPrefix(List<ColumnInfo> columnInfos) {
		// we need to apply a CF AND column list filter for each family
		FilterList allColumnFilters = null;
		Map<String, List<ColumnInfo>> groupedMap = groupByFamily(columnInfos);
		for (String cfString : groupedMap.keySet()) {
			List<ColumnInfo> columnInfoList = groupedMap.get(cfString);
			byte[] cf = Bytes.toBytes(cfString);

			// all filters roll up to one parent OR filter
			if (allColumnFilters == null) {
				allColumnFilters = new FilterList(
						FilterList.Operator.MUST_PASS_ONE);
			}

			// each group contains a column family filter AND (all) and an OR
			// (one of) of
			// the column filters
			FilterList thisColumnGroupFilter = new FilterList(
					FilterList.Operator.MUST_PASS_ALL);
			thisColumnGroupFilter.addFilter(new FamilyFilter(CompareOp.EQUAL,
					new BinaryComparator(cf)));
			FilterList columnFilters = new FilterList(
					FilterList.Operator.MUST_PASS_ONE);
			for (ColumnInfo colInfo : columnInfoList) {
				if (colInfo.isColumnMap()) {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Adding family:prefix filters with values "
								+ Bytes.toString(colInfo.getColumnFamily())
								+ COLON
								+ Bytes.toString(colInfo.getColumnPrefix()));
					}

					// add a PrefixFilter to the list of column filters
					if (colInfo.getColumnPrefix() != null) {
						columnFilters.addFilter(new ColumnPrefixFilter(colInfo
								.getColumnPrefix()));
					}
				} else {

					if (LOG.isDebugEnabled()) {
						LOG.debug("Adding family:descriptor filters with values "
								+ Bytes.toString(colInfo.getColumnFamily())
								+ COLON
								+ Bytes.toString(colInfo.getColumnName()));
					}

					// add a QualifierFilter to the list of column filters
					columnFilters.addFilter(new QualifierFilter(
							CompareOp.EQUAL, new BinaryComparator(colInfo
									.getColumnName())));
				}
			}
			thisColumnGroupFilter.addFilter(columnFilters);
			allColumnFilters.addFilter(thisColumnGroupFilter);
		}
		if (allColumnFilters != null) {
			addFilter(allColumnFilters);
		}
	}

	private void addRowFilter(CompareOp op, byte[] val) {
		if (LOG.isDebugEnabled()) {
			LOG.debug("Adding filter " + op.toString() + " with value "
					+ Bytes.toStringBinary(val));
		}
		addFilter(new RowFilter(op, new BinaryComparator(val)));
	}

	private void addFilter(Filter filter) {
		FilterList scanFilter = (FilterList) scan.getFilter();
		if (scanFilter == null) {
			scanFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		}
		scanFilter.addFilter(filter);
		scan.setFilter(scanFilter);
	}

	/**
	 * Returns the ColumnInfo list for so external objects can inspect it. This
	 * is available for unit testing. Ideally, the unit tests and the main
	 * source would each mirror the same package structure and this method could
	 * be package private.
	 * 
	 * @return ColumnInfo
	 */
	public List<ColumnInfo> getColumnInfoList() {
		return columnInfo_;
	}

	@Override
	public Tuple getNext() throws IOException {
		try {
			// if (bitvector1 == null) {
			// bitvector1 = readBloomfilter(bitvectorPath1);
			// if (bitvectorPath2 != null) {
			// bitvector2 = readBloomfilter(bitvectorPath2);
			// }
			// }

			if (reader.nextKeyValue()) {
				Result result = (Result) reader.getCurrentValue();

				Tuple tuple = null;

				// use a map of families -> qualifiers with the most recent
				// version of the cell. Fetching multiple vesions could be a
				// useful feature.
				NavigableMap<byte[], NavigableMap<byte[], byte[]>> resultsMap = result
						.getNoVersionMap();

				ArrayList<String> tupleList = new ArrayList<String>();

				if (loadRowKey_) {
					ImmutableBytesWritable rowKey = (ImmutableBytesWritable) reader
							.getCurrentKey();
					tupleList.add(Bytes.toString(rowKey.get()));
				}

				for (int i = 0; i < columnInfo_.size(); ++i) {
					// int currentIndex = startIndex + i;

					ColumnInfo columnInfo = columnInfo_.get(i);
					if (columnInfo.isColumnMap()) {
						// It's a column family so we need to iterate and set
						// all
						// values found
						NavigableMap<byte[], byte[]> cfResults = resultsMap
								.get(columnInfo.getColumnFamily());
						// Map<String, DataByteArray> cfMap = new
						// HashMap<String, DataByteArray>();
						// Map<String, DataByteArray> cfMap = new
						// LinkedHashMap<String, DataByteArray>();
						if (cfResults != null) {
							for (byte[] quantifier : cfResults.keySet()) {
								// We need to check against the prefix filter to
								// see if this value should be included. We
								// can't
								// just rely on the server-side filter, since a
								// user could specify multiple CF filters for
								// the
								// same CF.
								if (columnInfo.getColumnPrefix() == null
										|| columnInfo
												.hasPrefixMatch(quantifier)) {
									String toSplit = Bytes.toString(quantifier);
									if (toSplit.contains(",")) {
										// boolean element1IsNecessary = true;
										// boolean element2IsNecessary = true;
										// 1
										String toAdd1 = toSplit.substring(0,
												toSplit.indexOf(","));
										// if (bitvector1 != null
										// && !isElementPartOfBitvector(
										// toAdd1, bitvector1)) {
										// element1IsNecessary = false;
										// return TupleFactory.getInstance()
										// .newTuple(tupleList.size());
										// }
										tupleList.add(toAdd1);

										// 2
										String toAdd2 = toSplit.substring(
												toSplit.indexOf(",") + 1,
												toSplit.length());

										// if (bitvector2 != null
										// && !isElementPartOfBitvector(
										// toAdd2, bitvector2)) {
										// // element2IsNecessary = false;
										// return TupleFactory.getInstance()
										// .newTuple(tupleList.size());
										// }
										tupleList.add(toAdd2);

										// if (!element1IsNecessary ||
										// !element2IsNecessary) {
										// // return null;
										// return
										// TupleFactory.getInstance().newTuple(0);
										// }
									} else {
										String toAdd = Bytes
												.toString(quantifier);
										// if (bitvector1 != null
										// && !isElementPartOfBitvector(
										// toAdd, bitvector1)) {
										// // return null;
										// return TupleFactory.getInstance()
										// .newTuple(tupleList.size());
										// }
										tupleList.add(toAdd);
									}

								}
							}
						}
						tuple = TupleFactory.getInstance().newTuple(
								tupleList.size());
						int tuplePos = 0;
						for (String elem : tupleList) {
							tuple.set(tuplePos, elem);
							tuplePos++;
						}
					} else {
						// kommt nicht vor
						// It's a column so set the value
						// byte[] cell = result.getValue(
						// columnInfo.getColumnFamily(),
						// columnInfo.getColumnName());
						// DataByteArray value = cell == null ? null
						// : new DataByteArray(cell);
						// tuple.set(currentIndex, value);
					}
				}

				// if (LOG.isDebugEnabled()) {
				// for (int i = 0; i < tuple.size(); i++) {
				// LOG.debug("tuple value:" + tuple.get(i));
				// }
				// }

				return tuple;
			}
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
		return null;
	}

	@Deprecated
	private boolean isElementPartOfBitvector(String element, BitSet bitvector) {
		Integer position = BitvectorManager.hash(element.getBytes());
		if (bitvector.get(position)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public InputFormat getInputFormat() {
		TableInputFormat inputFormat = new HBaseTableIFBuilder()
				.withLimit(limit_).withGt(gt_).withGte(gte_).withLt(lt_)
				.withLte(lte_).withConf(m_conf).build();
		return inputFormat;
	}

	@Override
	public void prepareToRead(RecordReader reader, PigSplit split) {
		this.reader = reader;
	}

	@Override
	public void setUDFContextSignature(String signature) {
		this.contextSignature = signature;
	}

	@Override
	public void setLocation(String location, Job job) throws IOException {
		Properties udfProps = getUDFProperties();
		job.getConfiguration().setBoolean("pig.noSplitCombination", true);

		initialiseHBaseClassLoaderResources(job);
		m_conf = initializeLocalJobConfig(job);
		String delegationTokenSet = udfProps.getProperty(HBASE_TOKEN_SET);
		if (delegationTokenSet == null) {
			addHBaseDelegationToken(m_conf, job);
			udfProps.setProperty(HBASE_TOKEN_SET, "true");
		}

		String tablename = location;
		if (location.startsWith("hbase://")) {
			tablename = location.substring(8);
		}
		if (m_table == null) {
			m_table = new HTable(m_conf, tablename);
		}
		m_table.setScannerCaching(caching_);
		m_conf.set(TableInputFormat.INPUT_TABLE, tablename);

		String projectedFields = udfProps.getProperty(projectedFieldsName());
		if (projectedFields != null) {
			// update columnInfo_
			pushProjection((RequiredFieldList) ObjectSerializer
					.deserialize(projectedFields));
		}

		for (ColumnInfo columnInfo : columnInfo_) {
			// do we have a column family, or a column?
			if (columnInfo.isColumnMap()) {
				scan.addFamily(columnInfo.getColumnFamily());
			} else {
				scan.addColumn(columnInfo.getColumnFamily(),
						columnInfo.getColumnName());
			}

		}
		if (requiredFieldList != null) {
			Properties p = UDFContext.getUDFContext().getUDFProperties(
					this.getClass(), new String[] { contextSignature });
			p.setProperty(contextSignature + "_projectedFields",
					ObjectSerializer.serialize(requiredFieldList));
		}
		m_conf.set(TableInputFormat.SCAN, convertScanToString(scan));
	}

	private void initialiseHBaseClassLoaderResources(Job job)
			throws IOException {
		// Make sure the HBase, ZooKeeper, and Guava jars get shipped.
		TableMapReduceUtil.addDependencyJars(job.getConfiguration(),
				org.apache.hadoop.hbase.client.HTable.class,
				com.google.common.collect.Lists.class,
				org.apache.zookeeper.ZooKeeper.class);

	}

	private JobConf initializeLocalJobConfig(Job job) {
		Properties udfProps = getUDFProperties();
		Configuration jobConf = job.getConfiguration();
		JobConf localConf = new JobConf(jobConf);
		if (udfProps.containsKey(HBASE_CONFIG_SET)) {
			for (Entry<Object, Object> entry : udfProps.entrySet()) {
				localConf.set((String) entry.getKey(),
						(String) entry.getValue());
			}
		} else {
			Configuration hbaseConf = HBaseConfiguration.create();
			for (Entry<String, String> entry : hbaseConf) {
				// JobConf may have some conf overriding ones in hbase-site.xml
				// So only copy hbase config not in job config to UDFContext
				// Also avoids copying core-default.xml and core-site.xml
				// props in hbaseConf to UDFContext which would be redundant.
				if (jobConf.get(entry.getKey()) == null) {
					udfProps.setProperty(entry.getKey(), entry.getValue());
					localConf.set(entry.getKey(), entry.getValue());
				}
			}
			udfProps.setProperty(HBASE_CONFIG_SET, "true");
		}
		return localConf;
	}

	/**
	 * Get delegation token from hbase and add it to the Job
	 * 
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void addHBaseDelegationToken(Configuration hbaseConf, Job job) {

		if (!UDFContext.getUDFContext().isFrontend()) {
			return;
		}

		if ("kerberos".equalsIgnoreCase(hbaseConf.get(HBASE_SECURITY_CONF_KEY))) {
			// Will not be entering this block for 0.20.2 as it has no security.
			try {
				// getCurrentUser method is not public in 0.20.2
				Method m1 = UserGroupInformation.class
						.getMethod("getCurrentUser");
				UserGroupInformation currentUser = (UserGroupInformation) m1
						.invoke(null, (Object[]) null);
				// hasKerberosCredentials method not available in 0.20.2
				Method m2 = UserGroupInformation.class
						.getMethod("hasKerberosCredentials");
				boolean hasKerberosCredentials = (Boolean) m2.invoke(
						currentUser, (Object[]) null);
				if (hasKerberosCredentials) {
					// Class and method are available only from 0.92 security
					// release
					Class tokenUtilClass = Class
							.forName("org.apache.hadoop.hbase.security.token.TokenUtil");
					Method m3 = tokenUtilClass.getMethod("obtainTokenForJob",
							new Class[] { Configuration.class,
									UserGroupInformation.class, Job.class });
					m3.invoke(null,
							new Object[] { hbaseConf, currentUser, job });
				} else {
					LOG.info("Not fetching hbase delegation token as no Kerberos TGT is available");
				}
			} catch (ClassNotFoundException cnfe) {
				throw new RuntimeException("Failure loading TokenUtil class, "
						+ "is secure RPC available?", cnfe);
			} catch (RuntimeException re) {
				throw re;
			} catch (Exception e) {
				throw new UndeclaredThrowableException(e,
						"Unexpected error calling TokenUtil.obtainTokenForJob()");
			}
		}
	}

	@Override
	public String relativeToAbsolutePath(String location, Path curDir)
			throws IOException {
		return location;
	}

	private static String convertScanToString(Scan scan) {
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			DataOutputStream dos = new DataOutputStream(out);
			scan.write(dos);
			return Base64.encodeBytes(out.toByteArray());
		} catch (IOException e) {
			LOG.error(e);
			return "";
		}

	}

	/**
	 * Set up the caster to use for reading values out of, and writing to,
	 * HBase.
	 */
	@Override
	public LoadCaster getLoadCaster() throws IOException {
		return caster_;
	}

	/*
	 * StoreFunc Methods
	 * 
	 * @see org.apache.pig.StoreFuncInterface#getOutputFormat()
	 */

	@Override
	public OutputFormat getOutputFormat() throws IOException {
		if (outputFormat == null) {
			if (m_conf == null) {
				throw new IllegalStateException(
						"setStoreLocation has not been called");
			} else {
				this.outputFormat = new TableOutputFormat();
				this.outputFormat.setConf(m_conf);
			}
		}
		return outputFormat;
	}

	@Override
	public void checkSchema(ResourceSchema s) throws IOException {
		if (!(caster_ instanceof LoadStoreCaster)) {
			LOG.error("Caster must implement LoadStoreCaster for writing to HBase.");
			throw new IOException("Bad Caster " + caster_.getClass());
		}
		schema_ = s;
		getUDFProperties().setProperty(contextSignature + "_schema",
				ObjectSerializer.serialize(schema_));
	}

	// Suppressing unchecked warnings for RecordWriter, which is not
	// parameterized by StoreFuncInterface
	@Override
	public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer)
			throws IOException {
		this.writer = writer;
	}

	// Suppressing unchecked warnings for RecordWriter, which is not
	// parameterized by StoreFuncInterface
	@SuppressWarnings("unchecked")
	@Override
	public void putNext(Tuple t) throws IOException {
		ResourceFieldSchema[] fieldSchemas = (schema_ == null) ? null : schema_
				.getFields();
		byte type = (fieldSchemas == null) ? DataType.findType(t.get(0))
				: fieldSchemas[0].getType();
		long ts = System.currentTimeMillis();

		Put put = createPut(t.get(0), type);

		if (LOG.isDebugEnabled()) {
			LOG.debug("putNext -- WAL disabled: " + noWAL_);
			for (ColumnInfo columnInfo : columnInfo_) {
				LOG.debug("putNext -- col: " + columnInfo);
			}
		}

		for (int i = 1; i < t.size(); ++i) {
			ColumnInfo columnInfo = columnInfo_.get(i - 1);
			if (LOG.isDebugEnabled()) {
				LOG.debug("putNext - tuple: " + i + ", value=" + t.get(i)
						+ ", cf:column=" + columnInfo);
			}

			if (!columnInfo.isColumnMap()) {
				put.add(columnInfo.getColumnFamily(),
						columnInfo.getColumnName(),
						ts,
						objToBytes(
								t.get(i),
								(fieldSchemas == null) ? DataType.findType(t
										.get(i)) : fieldSchemas[i].getType()));
			} else {
				Map<String, Object> cfMap = (Map<String, Object>) t.get(i);
				for (String colName : cfMap.keySet()) {
					if (LOG.isDebugEnabled()) {
						LOG.debug("putNext - colName=" + colName + ", class: "
								+ colName.getClass());
					}
					// TODO deal with the fact that maps can have types now.
					// Currently we detect types at
					// runtime in the case of storing to a cf, which is
					// suboptimal.
					put.add(columnInfo.getColumnFamily(),
							Bytes.toBytes(colName.toString()),
							ts,
							objToBytes(cfMap.get(colName),
									DataType.findType(cfMap.get(colName))));
				}
			}
		}

		try {
			writer.write(null, put);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	/**
	 * Public method to initialize a Put. Used to allow assertions of how Puts
	 * are initialized by unit tests.
	 * 
	 * @param key
	 * @param type
	 * @return new put
	 * @throws IOException
	 */
	public Put createPut(Object key, byte type) throws IOException {
		Put put = new Put(objToBytes(key, type));

		if (noWAL_) {
			put.setWriteToWAL(false);
		}

		return put;
	}

	@SuppressWarnings("unchecked")
	private byte[] objToBytes(Object o, byte type) throws IOException {
		LoadStoreCaster caster = (LoadStoreCaster) caster_;
		if (o == null)
			return null;
		switch (type) {
		case DataType.BYTEARRAY:
			return ((DataByteArray) o).get();
		case DataType.BAG:
			return caster.toBytes((DataBag) o);
		case DataType.CHARARRAY:
			return caster.toBytes((String) o);
		case DataType.DOUBLE:
			return caster.toBytes((Double) o);
		case DataType.FLOAT:
			return caster.toBytes((Float) o);
		case DataType.INTEGER:
			return caster.toBytes((Integer) o);
		case DataType.LONG:
			return caster.toBytes((Long) o);
		case DataType.BOOLEAN:
			return caster.toBytes((Boolean) o);
		case DataType.DATETIME:
			return caster.toBytes((DateTime) o);

			// The type conversion here is unchecked.
			// Relying on DataType.findType to do the right thing.
		case DataType.MAP:
			return caster.toBytes((Map<String, Object>) o);

		case DataType.NULL:
			return null;
		case DataType.TUPLE:
			return caster.toBytes((Tuple) o);
		case DataType.ERROR:
			throw new IOException("Unable to determine type of " + o.getClass());
		default:
			throw new IOException("Unable to find a converter for tuple field "
					+ o);
		}
	}

	@Override
	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return location;
	}

	@Override
	public void setStoreFuncUDFContextSignature(String signature) {
		this.contextSignature = signature;
	}

	@Override
	public void setStoreLocation(String location, Job job) throws IOException {
		if (location.startsWith("hbase://")) {
			job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE,
					location.substring(8));
		} else {
			job.getConfiguration()
					.set(TableOutputFormat.OUTPUT_TABLE, location);
		}

		String serializedSchema = getUDFProperties().getProperty(
				contextSignature + "_schema");
		if (serializedSchema != null) {
			schema_ = (ResourceSchema) ObjectSerializer
					.deserialize(serializedSchema);
		}

		initialiseHBaseClassLoaderResources(job);
		m_conf = initializeLocalJobConfig(job);
		// Not setting a udf property and getting the hbase delegation token
		// only once like in setLocation as setStoreLocation gets different Job
		// objects for each call and the last Job passed is the one that is
		// launched. So we end up getting multiple hbase delegation tokens.
		addHBaseDelegationToken(m_conf, job);
	}

	@Override
	public void cleanupOnFailure(String location, Job job) throws IOException {
	}

	@Override
	public void cleanupOnSuccess(String location, Job job) throws IOException {
	}

	/*
	 * LoadPushDown Methods.
	 */

	@Override
	public List<OperatorSet> getFeatures() {
		return Arrays.asList(LoadPushDown.OperatorSet.PROJECTION);
	}

	@Override
	public RequiredFieldResponse pushProjection(
			RequiredFieldList requiredFieldList) throws FrontendException {
		List<RequiredField> requiredFields = requiredFieldList.getFields();
		List<ColumnInfo> newColumns = Lists
				.newArrayListWithExpectedSize(requiredFields.size());

		if (this.requiredFieldList != null) {
			// in addition to PIG, this is also called by this.setLocation().
			LOG.debug("projection is already set. skipping.");
			return new RequiredFieldResponse(true);
		}

		/*
		 * How projection is handled : - pushProjection() is invoked by PIG on
		 * the front end - pushProjection here both stores serialized projection
		 * in the context and adjusts columnInfo_. - setLocation() is invoked on
		 * the backend and it reads the projection from context. setLocation
		 * invokes this method again so that columnInfo_ is adjected.
		 */

		// colOffset is the offset in our columnList that we need to apply to
		// indexes we get from requiredFields
		// (row key is not a real column)
		int colOffset = loadRowKey_ ? 1 : 0;
		// projOffset is the offset to the requiredFieldList we need to apply
		// when figuring out which columns to prune.
		// (if key is pruned, we should skip row key's element in this list when
		// trimming colList)
		int projOffset = colOffset;
		this.requiredFieldList = requiredFieldList;

		if (requiredFieldList != null
				&& requiredFields.size() > (columnInfo_.size() + colOffset)) {
			throw new FrontendException(
					"The list of columns to project from HBase is larger than HBaseStorage is configured to load.");
		}

		// remember the projection
		try {
			getUDFProperties().setProperty(projectedFieldsName(),
					ObjectSerializer.serialize(requiredFieldList));
		} catch (IOException e) {
			throw new FrontendException(e);
		}

		if (loadRowKey_
				&& (requiredFields.size() < 1 || requiredFields.get(0)
						.getIndex() != 0)) {
			loadRowKey_ = false;
			projOffset = 0;
		}

		for (int i = projOffset; i < requiredFields.size(); i++) {
			int fieldIndex = requiredFields.get(i).getIndex();
			newColumns.add(columnInfo_.get(fieldIndex - colOffset));
		}

		if (LOG.isDebugEnabled()) {
			LOG.debug("pushProjection After Projection: loadRowKey is "
					+ loadRowKey_);
			for (ColumnInfo colInfo : newColumns) {
				LOG.debug("pushProjection -- col: " + colInfo);
			}
		}
		columnInfo_ = newColumns;
		return new RequiredFieldResponse(true);
	}

	// old buggy code
	// @Override
	// public WritableComparable<InputSplit> getSplitComparable(InputSplit
	// split)
	// throws IOException {
	// return new WritableComparable<InputSplit>() {
	// TableSplit tsplit = new TableSplit();
	//
	// @Override
	// public void readFields(DataInput in) throws IOException {
	// tsplit.readFields(in);
	// }
	//
	// @Override
	// public void write(DataOutput out) throws IOException {
	// tsplit.write(out);
	// }
	//
	// @Override
	// public int compareTo(InputSplit split) {
	// return tsplit.compareTo((TableSplit) split);
	// }
	// };
	// }

	public WritableComparable<TableSplit> getSplitComparable(InputSplit split)
			throws IOException {
		if (split instanceof TableSplit) {
			return new TableSplitComparable((TableSplit) split);
		} else {
			throw new RuntimeException(
					"LoadFunc expected split of type TableSplit");
		}
	}

	/**
	 * Class to encapsulate logic around which column names were specified in
	 * each position of the column list. Users can specify columns names in one
	 * of 4 ways: 'Foo:', 'Foo:*', 'Foo:bar*' or 'Foo:bar'. The first 3 result
	 * in a Map being added to the tuple, while the last results in a scalar.
	 * The 3rd form results in a prefix-filtered Map.
	 */
	public class ColumnInfo {

		final String originalColumnName; // always set
		final byte[] columnFamily; // always set
		final byte[] columnName; // set if it exists and doesn't contain '*'
		final byte[] columnPrefix; // set if contains a prefix followed by '*'

		public ColumnInfo(String colName) {
			originalColumnName = colName;
			String[] cfAndColumn = colName.split(COLON, 2);

			// CFs are byte[1] and columns are byte[2]
			columnFamily = Bytes.toBytes(cfAndColumn[0]);
			if (cfAndColumn.length > 1 && cfAndColumn[1].length() > 0
					&& !ASTERISK.equals(cfAndColumn[1])) {
				if (cfAndColumn[1].endsWith(ASTERISK)) {
					columnPrefix = Bytes.toBytes(cfAndColumn[1].substring(0,
							cfAndColumn[1].length() - 1));
					columnName = null;
				} else {
					columnName = Bytes.toBytes(cfAndColumn[1]);
					columnPrefix = null;
				}
			} else {
				columnPrefix = null;
				columnName = null;
			}
		}

		public byte[] getColumnFamily() {
			return columnFamily;
		}

		public byte[] getColumnName() {
			return columnName;
		}

		public byte[] getColumnPrefix() {
			return columnPrefix;
		}

		public boolean isColumnMap() {
			return columnName == null;
		}

		public boolean hasPrefixMatch(byte[] qualifier) {
			return Bytes.startsWith(qualifier, columnPrefix);
		}

		@Override
		public String toString() {
			return originalColumnName;
		}
	}

	/**
	 * Group the list of ColumnInfo objects by their column family and returns a
	 * map of CF to its list of ColumnInfo objects. Using String as key since it
	 * implements Comparable.
	 * 
	 * @param columnInfos
	 *            the columnInfo list to group
	 * @return a Map of lists, keyed by their column family.
	 */
	static Map<String, List<ColumnInfo>> groupByFamily(
			List<ColumnInfo> columnInfos) {
		Map<String, List<ColumnInfo>> groupedMap = new HashMap<String, List<ColumnInfo>>();
		for (ColumnInfo columnInfo : columnInfos) {
			String cf = Bytes.toString(columnInfo.getColumnFamily());
			List<ColumnInfo> columnInfoList = groupedMap.get(cf);
			if (columnInfoList == null) {
				columnInfoList = new ArrayList<ColumnInfo>();
			}
			columnInfoList.add(columnInfo);
			groupedMap.put(cf, columnInfoList);
		}
		return groupedMap;
	}

	static String toString(byte[] bytes) {
		if (bytes == null) {
			return null;
		}

		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < bytes.length; i++) {
			if (i > 0) {
				sb.append("|");
			}
			sb.append(bytes[i]);
		}
		return sb.toString();
	}

	/**
	 * Increments the byte array by one for use with setting stopRow. If all
	 * bytes in the array are set to the maximum byte value, then the original
	 * array will be returned with a 0 byte appended to it. This is because
	 * HBase compares bytes from left to right. If byte array B is equal to byte
	 * array A, but with an extra byte appended, A will be < B. For example
	 * {@code}A = byte[] {-1}{@code} increments to {@code}B = byte[] {-1, 0}
	 * {@code} and {@code}A < B{@code}
	 * 
	 * @param bytes
	 *            array to increment bytes on
	 * @return a copy of the byte array incremented by 1
	 */
	static byte[] increment(byte[] bytes) {
		boolean allAtMax = true;
		for (int i = 0; i < bytes.length; i++) {
			if ((bytes[bytes.length - i - 1] & 0x0ff) != 255) {
				allAtMax = false;
				break;
			}
		}

		if (allAtMax) {
			return Arrays.copyOf(bytes, bytes.length + 1);
		}

		byte[] incremented = bytes.clone();
		for (int i = bytes.length - 1; i >= 0; i--) {
			boolean carry = false;
			int val = bytes[i] & 0x0ff;
			int total = val + 1;
			if (total > 255) {
				carry = true;
				total %= 256;
			} else if (total < 0) {
				carry = true;
			}
			incremented[i] = (byte) total;
			if (!carry)
				return incremented;
		}
		return incremented;
	}
}
