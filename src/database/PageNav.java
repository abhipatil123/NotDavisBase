package database;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.TreeSet;
import java.util.Map.Entry;

public class PageNav {

	public static int PAGESIZE = 512;// page size

	// variables
	public byte noOfCols;
	public byte[] dataType;
	public String[] data;
	public int pageNumber;
	public short payLoadSize;
	public int rowId;
	public PageNav payload;
	public short location;
	public int pageNo;
	public byte pageType;
	public Map<Integer, PageNav> tuples;

	public PageNav getPayload() {
		return payload;
	}

	public void setPayload(PageNav payload) {
		this.payload = payload;
	}
	
	public static void writeToPage(RandomAccessFile table, int parent, int newPage, int midKey) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int noRecs = table.read();
			int mid = (int) Math.ceil((double) noRecs / 2);
			int noRecs1 = mid - 1;
			int noRecs2 = noRecs - noRecs1;
			int size = PAGESIZE;
			for (int i = noRecs1; i < noRecs; i++) {
				table.seek((parent - 1) * PAGESIZE + 8 + 2 * i);
				short offset = table.readShort();
				table.seek(offset);
				byte[] data = new byte[8];
				table.read(data);
				size = size - 8;
				table.seek((newPage - 1) * PAGESIZE + size);
				table.write(data);

				table.seek((newPage - 1) * PAGESIZE + 8 + (i - noRecs1) * 2);
				table.writeShort(size);

			}

			table.seek((parent - 1) * PAGESIZE + 1);
			table.write(noRecs1);

			table.seek((newPage - 1) * PAGESIZE + 1);
			table.write(noRecs2);

			int tree_parent = TreeFunctions.getParent(table, parent);
			if (tree_parent == 0) {
				int new_tree_parent = createPage(table);
				TreeFunctions.setParent(table, new_tree_parent, parent, midKey);
				table.seek((new_tree_parent - 1) * PAGESIZE + 4);
				table.writeInt(newPage);
			} else {
				if (rightPointer(table, tree_parent, parent)) {
					TreeFunctions.setParent(table, tree_parent, parent, midKey);
					table.seek((tree_parent - 1) * PAGESIZE + 4);
					table.writeInt(newPage);
				} else
					TreeFunctions.setParent(table, tree_parent, newPage, midKey);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static boolean rightPointer(RandomAccessFile table, int parent, int rightPointer) {

		try {
			table.seek((parent - 1) * PAGESIZE + 4);
			if (table.readInt() == rightPointer)
				return true;
		} catch (IOException e) {
			e.printStackTrace();
		}
		return false;
	}

	public static boolean checkCapacity(RandomAccessFile table, int parent) {
		try {
			table.seek((parent - 1) * PAGESIZE + 1);
			int noRecs = table.read();
			short Buildercontent = table.readShort();
			int size = 8 + noRecs * 2 + Buildercontent;
			size = PAGESIZE - size;
			if (size >= 8)
				return true;
		} catch (IOException e) {

			e.printStackTrace();
		}
		return false;
	}

	public static int createPage(RandomAccessFile table) {
		int noOfPages = 0;
		try {
			noOfPages = (int) (table.length() / PAGESIZE);
			noOfPages++;
			table.setLength(table.length() + PAGESIZE);
			table.seek((noOfPages - 1) * PAGESIZE);
			table.write(0x05);

		} catch (IOException e) {
			e.printStackTrace();
		}
		return noOfPages;
	}

	
	public void setData(String[] data) {
		this.data = data;
	}

	
	public static int createNewPage(RandomAccessFile table) {

		try {
			int noOfPages = (int) table.length() / PAGESIZE;
			noOfPages = noOfPages + 1;
			table.setLength(noOfPages * PAGESIZE);
			table.seek((noOfPages - 1) * PAGESIZE);
			table.writeByte(0x0D);
			return noOfPages;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return -1;
	}


	public static void payload(RandomAccessFile file, PageNav PageNav, int BuilderLocation) {

		try {

			file.seek(BuilderLocation);
			file.writeShort(PageNav.payLoadSize);
			file.writeInt(PageNav.rowId);

			PageNav payload = PageNav.getPayload();
			file.writeByte(payload.noOfCols);

			byte[] dataTypes = payload.dataType;
			file.write(dataTypes);

			String data[] = payload.data;

			for (int i = 0; i < dataTypes.length; i++) {
				switch (dataTypes[i]) {
				case 0x00:
					file.writeByte(0);
					break;
				case 0x01:
					file.writeShort(0);
					break;
				case 0x02:
					file.writeInt(0);
					break;
				case 0x03:
					file.writeLong(0);
					break;
				case 0x04:
					file.writeByte(new Byte(data[i + 1]));
					break;
				case 0x05:
					file.writeShort(new Short(data[i + 1]));
					break;
				case 0x06:
					file.writeInt(new Integer(data[i + 1]));
					break;
				case 0x07:
					file.writeLong(new Long(data[i + 1]));
					break;
				case 0x08:
					file.writeFloat(new Float(data[i + 1]));
					break;
				case 0x09:
					file.writeDouble(new Double(data[i + 1]));
					break;
				case 0x0A:
					long datetime = file.readLong();
					ZoneId zoneId = ZoneId.of("America/Chicago");
					Instant x = Instant.ofEpochSecond(datetime);
					ZonedDateTime zdt2 = ZonedDateTime.ofInstant(x, zoneId);
					zdt2.toLocalTime();
					break;
				case 0x0B:
					long date = file.readLong();
					ZoneId zoneId1 = ZoneId.of("America/Chicago");
					Instant x1 = Instant.ofEpochSecond(date);
					ZonedDateTime zdt3 = ZonedDateTime.ofInstant(x1, zoneId1);
					zdt3.toLocalTime();
					break;
				default:
					file.writeBytes(data[i + 1]);
					break;
				}
			}

			file.seek((PAGESIZE * PageNav.pageNumber) + 1);
			int noOfBlds = file.readByte();

			file.seek((PAGESIZE * PageNav.pageNumber) + 1);
			file.writeByte((byte) (noOfBlds + 1));

			Map<Integer, Short> updatePage = new TreeMap<Integer, Short>();
			short[] BldLocn = new short[noOfBlds];
			int[] keys = new int[noOfBlds];

			for (int location = 0; location < noOfBlds; location++) {

				file.seek((PAGESIZE * PageNav.pageNumber) + 8 + (location * 2));
				BldLocn[location] = file.readShort();
				file.seek(BldLocn[location] + 2);
				keys[location] = file.readInt();
				updatePage.put(keys[location], BldLocn[location]);
			}
			updatePage.put(PageNav.rowId, (short) BuilderLocation);

			file.seek((PAGESIZE * PageNav.pageNumber) + 8);
			for (Map.Entry<Integer, Short> entry : updatePage.entrySet()) {
				short offset = entry.getValue();
				file.writeShort(offset);
			}

			file.seek((PAGESIZE * PageNav.pageNumber) + 2);
			file.writeShort(BuilderLocation);
			file.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static PageNav AddPage(int pageNo, int primaryKey, short payLoadSize, byte[] dataType, String[] values) {
		PageNav PageNav = new PageNav();
		PageNav.pageNumber = pageNo;
		PageNav.rowId = primaryKey;
		PageNav.payLoadSize = payLoadSize;

		PageNav payload = new PageNav();
		payload.noOfCols = (Byte.parseByte(values.length - 1 + ""));
		payload.dataType = dataType;
		payload.setData(values);

		PageNav.setPayload(payload);

		return PageNav;
	}

	public static int getPayloadSize(String tableName, String[] values, byte[] plDataType, String[] dataType) {
		int size = 1 + dataType.length - 1;
		for (int i = 0; i < values.length - 1; i++) {
			plDataType[i] = dataType(values[i + 1], dataType[i + 1]);
			size = size + dataLength(plDataType[i]);
		}
		return size;
	}

	private static byte dataType(String value, String dataType) {
		if (value.equals("null")) {
			switch (dataType) {
			case "TINYINT":
				return 0x00;
			case "SMALLINT":
				return 0x01;
			case "INT":
				return 0x02;
			case "BIGINT":
				return 0x03;
			case "REAL":
				return 0x02;
			case "DOUBLE":
				return 0x03;
			case "DATETIME":
				return 0x03;
			case "DATE":
				return 0x03;
			case "TEXT":
				return 0x03;
			default:
				return 0x00;
			}
		} else {
			switch (dataType) {
			case "TINYINT":
				return 0x04;
			case "SMALLINT":
				return 0x05;
			case "INT":
				return 0x06;
			case "BIGINT":
				return 0x07;
			case "REAL":
				return 0x08;
			case "DOUBLE":
				return 0x09;
			case "DATETIME":
				return 0x0A;
			case "DATE":
				return 0x0B;
			case "TEXT":
				return (byte) (value.length() + 0x0C);
			default:
				return 0x00;
			}
		}
	}

	public static int getPage(String tableName, int key) {
		try {

			tableName = tableName.trim();
			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";

			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, PageNav> records = new LinkedHashMap<Integer, PageNav>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfBuilders = table.readByte();
					short[] BuilderLocations = new short[noOfBuilders];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfBuilders; location++) {
						BuilderLocations[location] = table.readShort();
					}
					Map<Integer, PageNav> recBlds = new LinkedHashMap<Integer, PageNav>();
					recBlds = retreivePayload(table, BuilderLocations, i);

					Set<Integer> rowIds = recBlds.keySet();

					Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);

					Integer rows[] = sortedRowIds.toArray(new Integer[sortedRowIds.size()]);

					// last page
					table.seek((PAGESIZE * i) + 4);
					int filePointer = table.readInt();

					if (rowIds.size() == 0) {
						table.close();
						return 0;}
					if (rows[0] <= key && key <= rows[rows.length - 1]) {
						table.close();
						return i;}
					else if (filePointer == -1 && rows[rows.length - 1] < key) {
						table.close();
						return i;
					}
				}
			}
			
		}

		catch (Exception e) {
			e.printStackTrace();
		}
		return -1;
	}

	public static Map<Integer, PageNav> getData(String tableName, String[] columnNames, String[] condition) {
		try {

			tableName = tableName.trim();
			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";

			RandomAccessFile table = new RandomAccessFile(path, "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			Map<Integer, PageNav> pageInfo = new LinkedHashMap<Integer, PageNav>();

			Map<Integer, String> colNames = getColumnNames(tableName);
			Map<Integer, PageNav> records = new LinkedHashMap<Integer, PageNav>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					PageNav page = new PageNav();
					page.setPageNo(i);
					page.setPageType(pageType);

					int noOfBuilders = table.readByte();
					short[] BuilderLocations = new short[noOfBuilders];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfBuilders; location++) {
						BuilderLocations[location] = table.readShort();
					}
					Map<Integer, PageNav> recordBuilders = new LinkedHashMap<Integer, PageNav>();
					recordBuilders = retreivePayload(table, BuilderLocations, i);

					page.tuples = recordBuilders;
					pageInfo.put(i, page);

					records.putAll(recordBuilders);
				}
			}

			if (condition.length > 0) {
				Map<Integer, PageNav> filteredRecords = filterTuples(colNames, records, columnNames, condition);
				table.close();
				return filteredRecords;
			} else {
				table.close();
				return records;
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;

	}

	public static String[] getDataType(Map<Integer, PageNav> column) {
		int count = 0;
		String[] dataType = new String[column.size()];
		for (Map.Entry<Integer, PageNav> entry : column.entrySet()) {

			PageNav PageNav = entry.getValue();
			PageNav payload = PageNav.getPayload();
			String[] data = payload.data;
			dataType[count] = data[2];
			count++;
		}
		return dataType;
	}

	public static Map<Integer, PageNav> getcolumn(String tableName, String[] columnNames, String[] condition) {

		try {

			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			Map<Integer, String> colNames = getColumnNames("davisbase_columns");
			Map<Integer, PageNav> records = new LinkedHashMap<Integer, PageNav>();
			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfBuilders = table.readByte();
					short[] BuilderLocations = new short[noOfBuilders];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfBuilders; location++) {
						BuilderLocations[location] = table.readShort();
					}
					Map<Integer, PageNav> recordBuilders = new LinkedHashMap<Integer, PageNav>();
					recordBuilders = retreivePayload(table, BuilderLocations, i);
					records.putAll(recordBuilders);
				}
			}

			if (condition.length > 0) {
				Map<Integer, PageNav> filteredRecords = filterTuples(colNames, records, columnNames, condition);
				table.close();
				return filteredRecords;
			} else {
				table.close();
				return records;
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return null;

	}
	
	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}
	
	public static void printTable(Map<Integer, String> colNames, Map<Integer, PageNav> records) {
		String col = "";
		String row = "";
		ArrayList<String> recList = new ArrayList<String>();
		System.out.println(line("-", 120));
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {

			String colName = entry.getValue();
			System.out.format("%20s%10s", colName, "|");
		}
		System.out.println();
		System.out.println(line("-", 120));
		for (Map.Entry<Integer, PageNav> entry : records.entrySet()) {

			PageNav PageNav = entry.getValue();
			row += PageNav.rowId;
			String data[] = PageNav.getPayload().data;
			System.out.format("%20s%10s", row, "|");
			for (String dataS : data) {
				System.out.format("%20s%10s", dataS, "|");
			}
			row = "";
			System.out.println();
		}
		System.out.println(line("-", 120));
	}

	
	public void setPageType(byte pageType) {
		this.pageType = pageType;
	}

	public void setPageNo(int pageNo) {
		this.pageNo = pageNo;
	}

	public static Map<Integer, PageNav> filterTuples(Map<Integer, String> colNames, Map<Integer, PageNav> records,
			String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, PageNav> filteredTuples = new LinkedHashMap<Integer, PageNav>();

		int wherePos = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				wherePos = entry.getKey();
			}
		}
		Set<Integer> oPos = colNames.keySet();
		for (Map.Entry<Integer, PageNav> entry : records.entrySet()) {
			PageNav PageNav = entry.getValue();
			PageNav payload = PageNav.getPayload();
			String[] data = payload.data;
			byte[] dataTypeCodes = payload.dataType;

			boolean result;
			if (wherePos == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[wherePos - 2], data[wherePos - 2], condition);

			if (result)
				filteredTuples.put(entry.getKey(), entry.getValue());
		}

		return filteredTuples;

	}

	public static Map<Integer, PageNav> filterTuplesByData(Map<Integer, String> colNames,
			Map<Integer, PageNav> records, String[] resultColumnNames, String[] condition) {

		Set<String> resultColumnSet = new HashSet<String>(Arrays.asList(resultColumnNames));
		Map<Integer, PageNav> filteredRecords = new LinkedHashMap<Integer, PageNav>();

		int wherePos = 2;
		for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
			String columnName = entry.getValue();
			if (columnName.equals(condition[0])) {
				wherePos = entry.getKey();
			}
		}
		Set<Integer> oPos = colNames.keySet();
		for (Map.Entry<Integer, PageNav> entry : records.entrySet()) {
			PageNav PageNav = entry.getValue();
			PageNav payload = PageNav.getPayload();
			String[] data = payload.data;
			byte[] dataTypeCodes = payload.dataType;

			boolean result;
			if (wherePos == 1)
				result = checkData((byte) 0x06, entry.getKey().toString(), condition);
			else
				result = checkData(dataTypeCodes[wherePos - 2], data[wherePos - 2], condition);

			if (result)
				filteredRecords.put(entry.getKey(), entry.getValue());
		}

		return filteredRecords;

	}

	private static boolean checkData(byte code, String data, String[] condition) {

		if (code >= 0x04 && code <= 0x07) {
			Long dataLong = Long.parseLong(data);
			switch (condition[1]) {
			case "=":
				if (dataLong == Long.parseLong(condition[2]))
					return true;
				break;
			case ">":
				if (dataLong > Long.parseLong(condition[2]))
					return true;
				break;
			case "<":
				if (dataLong < Long.parseLong(condition[2]))
					return true;
				break;
			case "<=":
				if (dataLong <= Long.parseLong(condition[2]))
					return true;
				break;
			case ">=":
				if (dataLong >= Long.parseLong(condition[2]))
					return true;
				break;
			case "<>":
				if (dataLong != Long.parseLong(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code == 0x08 || code == 0x09) {
			Double doubleData = Double.parseDouble(data);
			switch (condition[1]) {
			case "=":
				if (doubleData == Double.parseDouble(condition[2]))
					return true;
				break;
			case ">":
				if (doubleData > Double.parseDouble(condition[2]))
					return true;
				break;
			case "<":
				if (doubleData < Double.parseDouble(condition[2]))
					return true;
				break;
			case "<=":
				if (doubleData <= Double.parseDouble(condition[2]))
					return true;
				break;
			case ">=":
				if (doubleData >= Double.parseDouble(condition[2]))
					return true;
				break;
			case "<>":
				if (doubleData != Double.parseDouble(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}

		} else if (code >= 0x0C) {

			condition[2] = condition[2].replaceAll("'", "");
			condition[2] = condition[2].replaceAll("\"", "");
			switch (condition[1]) {
			case "=":
				if (data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			case "<>":
				if (!data.equalsIgnoreCase(condition[2]))
					return true;
				break;
			default:
				System.out.println("undefined operator return false");
				return false;
			}
		}

		return false;

	}

	public static Map<Integer, String> getColumnNames(String tableName) {
		Map<Integer, String> columns = new LinkedHashMap<Integer, String>();
		try {
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			int noOfPages = (int) (table.length() / PAGESIZE);

			for (int i = 0; i < noOfPages; i++) {
				table.seek(PAGESIZE * i);
				byte pageType = table.readByte();
				if (pageType == 0x0D) {

					int noOfBlds = table.readByte();
					short[] BldLocn = new short[noOfBlds];
					table.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfBlds; location++) {
						BldLocn[location] = table.readShort();
					}
					Map<Integer, PageNav> recordBuilders = new LinkedHashMap<Integer, PageNav>();
					recordBuilders = retreivePayload(table, BldLocn, i);

					for (Map.Entry<Integer, PageNav> entry : recordBuilders.entrySet()) {

						PageNav PageNav = entry.getValue();

						PageNav payload = PageNav.getPayload();
						String[] data = payload.data;
						if (data[0].equalsIgnoreCase(tableName)) {
							columns.put(Integer.parseInt(data[3]), data[1]);
						}

					}

				}

			}
			table.close();
		} catch (Exception e) {

			e.printStackTrace();
		}

		return columns;

	}

	public static Map<Integer, PageNav> retreivePayload(RandomAccessFile table, short[] BuilderLocations, int pageNo) {

		Map<Integer, PageNav> PageNavs = new LinkedHashMap<Integer, PageNav>();
		for (int position = 0; position < BuilderLocations.length; position++) {
			try {
				PageNav PageNav = new PageNav();
				PageNav.pageNumber = pageNo;

				PageNav.location = BuilderLocations[position];

				table.seek(BuilderLocations[position]);

				short payLoadSize = table.readShort();
				PageNav.payLoadSize = payLoadSize;

				int rowId = table.readInt();
				PageNav.rowId = rowId;

				PageNav payload = new PageNav();
				byte num_cols = table.readByte();
				payload.noOfCols = num_cols;

				byte[] dataType = new byte[num_cols];
				int colsRead = table.read(dataType);
				payload.dataType = dataType;

				String data[] = new String[num_cols];
				payload.setData(data);

				for (int i = 0; i < num_cols; i++) {
					switch (dataType[i]) {
					case 0x00:
						data[i] = Integer.toString(table.readByte());
						data[i] = "null";
						break;

					case 0x01:
						data[i] = Integer.toString(table.readShort());
						data[i] = "null";
						break;

					case 0x02:
						data[i] = Integer.toString(table.readInt());
						data[i] = "null";
						break;

					case 0x03:
						data[i] = Long.toString(table.readLong());
						data[i] = "null";
						break;

					case 0x04:
						data[i] = Integer.toString(table.readByte());
						break;

					case 0x05:
						data[i] = Integer.toString(table.readShort());
						break;

					case 0x06:
						data[i] = Integer.toString(table.readInt());
						break;

					case 0x07:
						data[i] = Long.toString(table.readLong());
						break;

					case 0x08:
						data[i] = String.valueOf(table.readFloat());
						break;

					case 0x09:
						data[i] = String.valueOf(table.readDouble());
						break;

					case 0x0A:
						long tmp = table.readLong();
						Date dateTime = new Date(tmp);
						break;

					case 0x0B:
						long tmp1 = table.readLong();
						Date date = new Date(tmp1);
						break;

					default:
						int len = new Integer(dataType[i] - 0x0C);
						byte[] bytes = new byte[len];
						for (int j = 0; j < len; j++)
							bytes[j] = table.readByte();
						data[i] = new String(bytes);
						break;
					}

				}

				PageNav.setPayload(payload);
				PageNavs.put(rowId, PageNav);

			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		return PageNavs;
	}

	
	public static short dataLength(byte codes) {
		switch (codes) {
		case 0x00:
			return 1;
		case 0x01:
			return 2;
		case 0x02:
			return 4;
		case 0x03:
			return 8;
		case 0x04:
			return 1;
		case 0x05:
			return 2;
		case 0x06:
			return 4;
		case 0x07:
			return 8;
		case 0x08:
			return 4;
		case 0x09:
			return 8;
		case 0x0A:
			return 8;
		case 0x0B:
			return 8;
		default:
			return (short) (codes - 0x0C);
		}
	}

	public static String[] cmpCheck(String str) {

		String condition[] = new String[3];
		String values[] = new String[2];
		if (str.contains("=")) {
			values = str.split("=");
			condition[0] = values[0].trim();
			condition[1] = "=";
			condition[2] = values[1].trim();
		}

		if (str.contains(">")) {
			values = str.split(">");
			condition[0] = values[0].trim();
			condition[1] = ">";
			condition[2] = values[1].trim();
		}

		if (str.contains("<")) {
			values = str.split("<");
			condition[0] = values[0].trim();
			condition[1] = "<";
			condition[2] = values[1].trim();
		}

		if (str.contains(">=")) {
			values = str.split(">=");
			condition[0] = values[0].trim();
			condition[1] = ">=";
			condition[2] = values[1].trim();
		}

		if (str.contains("<=")) {
			values = str.split("<=");
			condition[0] = values[0].trim();
			condition[1] = "<=";
			condition[2] = values[1].trim();
		}

		if (str.contains("<>")) {
			values = str.split("<>");
			condition[0] = values[0].trim();
			condition[1] = "<>";
			condition[2] = values[1].trim();
		}

		return condition;
	}

	public static void upadateMetaTable(RandomAccessFile davisbaseTables, String metaTable, String tableName)
			throws IOException {
		int noOfPages = (int) (davisbaseTables.length() / PAGESIZE);

		Map<Integer, String> colNames = getColumnNames(metaTable);

		for (int i = 0; i < noOfPages; i++) {
			davisbaseTables.seek(PAGESIZE * i);
			byte pageType = davisbaseTables.readByte();
			if (pageType == 0x0D) {

				int noOfBlds = davisbaseTables.readByte();
				short[] BldLocn = new short[noOfBlds];
				davisbaseTables.seek((PAGESIZE * i) + 8);
				for (int location = 0; location < noOfBlds; location++) {
					BldLocn[location] = davisbaseTables.readShort();
				}
				Map<Integer, PageNav> recBlds = new LinkedHashMap<Integer, PageNav>();
				recBlds = retreivePayload(davisbaseTables, BldLocn, i);

				String[] condition = { "table_name", "<>", tableName };
				String[] columnNames = { "*" };

				Map<Integer, PageNav> filteredRecs = filterTuplesByData(colNames, recBlds, columnNames,
						condition);
				short[] offsets = new short[filteredRecs.size()];
				int l = 0;
				for (Map.Entry<Integer, PageNav> entry : filteredRecs.entrySet()) {
					PageNav PageNav = entry.getValue();
					offsets[l] = PageNav.location;
					davisbaseTables.seek(i * PAGESIZE + 8 + (2 * l));
					davisbaseTables.writeShort(offsets[l]);
					l++;
				}
				davisbaseTables.seek((PAGESIZE * i) + 1);
				davisbaseTables.writeByte(offsets.length);
				davisbaseTables.writeShort(offsets[offsets.length - 1]);
			}
		}

	}
}
