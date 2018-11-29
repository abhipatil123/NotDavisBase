package database;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ExecuteCommands {
	public static int PAGESIZE = 512;// page size
	
	public static void createTable(RandomAccessFile table, String tableName, String[] columnNames) {
		try {

			table.setLength(PAGESIZE);
			table.seek(0);
			table.writeByte(0x0D);
			table.seek(2);
			table.writeShort(PAGESIZE);
			table.writeInt(-1);
			table.close();

			RandomAccessFile davisbaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			int noOfPages = (int) (davisbaseTables.length() / PAGESIZE);
			int page = 0;

			Map<Integer, Builder> recordBuilders = new LinkedHashMap<Integer, Builder>();
			for (int i = 0; i < noOfPages; i++) {
				davisbaseTables.seek((i * PAGESIZE) + 4);
				int filePointer = davisbaseTables.readInt();
				if (filePointer == -1) {
					page = i;
					davisbaseTables.seek(i * PAGESIZE + 1);
					int noOfBuilders = davisbaseTables.readByte();
					short[] BuilderLocations = new short[noOfBuilders];
					davisbaseTables.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfBuilders; location++) {
						BuilderLocations[location] = davisbaseTables.readShort();
					}
					recordBuilders = Builder.getRecords(davisbaseTables, BuilderLocations, i);
				}
			}
			davisbaseTables.close();
			Set<Integer> rowIds = recordBuilders.keySet();
			Set<Integer> sortedRowIds = new TreeSet<Integer>(rowIds);
			Integer rows[] = sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			int key = rows[rows.length - 1] + 1;

			String[] values = { String.valueOf(key), tableName.trim(), "8", "10" };
			ExecuteCommands.insert("davisbase_tables", values);

			RandomAccessFile davisbaseColumns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			noOfPages = (int) (davisbaseColumns.length() / PAGESIZE);
			page = 0;

			recordBuilders = new LinkedHashMap<Integer, Builder>();
			for (int i = 0; i < noOfPages; i++) {
				davisbaseColumns.seek((i * PAGESIZE) + 4);
				int filePointer = davisbaseColumns.readInt();
				if (filePointer == -1) {
					page = i;
					davisbaseColumns.seek(i * PAGESIZE + 1);
					int noOfBuilders = davisbaseColumns.readByte();
					short[] BuilderLocations = new short[noOfBuilders];
					davisbaseColumns.seek((PAGESIZE * i) + 8);
					for (int location = 0; location < noOfBuilders; location++) {
						BuilderLocations[location] = davisbaseColumns.readShort();
					}
					recordBuilders = Builder.getRecords(davisbaseColumns, BuilderLocations, i);
				}
			}
			rowIds = recordBuilders.keySet();
			sortedRowIds = new TreeSet<Integer>(rowIds);
			rows = sortedRowIds.toArray(new Integer[sortedRowIds.size()]);
			key = rows[rows.length - 1];

			for (int i = 0; i < columnNames.length; i++) {
				key = key + 1;

				String[] coltemp = columnNames[i].split(" ");
				String isNullable = "YES";

				if (coltemp.length == 4) {
					if (coltemp[2].equalsIgnoreCase("NOT") && coltemp[3].equalsIgnoreCase("NULL")) {
						isNullable = "NO";
					}
					if (coltemp[2].equalsIgnoreCase("PRIMARY") && coltemp[3].equalsIgnoreCase("KEY")) {
						isNullable = "NO";
					}

				}
				String colName = coltemp[0];
				String dataType = coltemp[1].toUpperCase();
				String ordinalPosition = String.valueOf(i + 1);
				String[] val = { String.valueOf(key), tableName, colName, dataType, ordinalPosition, isNullable };
				ExecuteCommands.insert("davisbase_columns", val);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	
	// insert functionality
		public static void insert(String tableName, String[] values) {
			try {
				tableName = tableName.trim();// remove white spaces
				String path = "data/userdata/" + tableName + ".tbl";
				if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
					path = "data/catalog/" + tableName + ".tbl";

				RandomAccessFile table = new RandomAccessFile(path, "rw");

				String condition[] = { "table_name", "=", tableName };
				String columnNames[] = { "*" };
				Map<Integer, Builder> column = Builder.getcolumn(tableName, columnNames, condition);
				String[] dataType = Builder.getDataType(column);

				int count = 0;
				String[] nullable = new String[column.size()];
				for (Map.Entry<Integer, Builder> entry : column.entrySet()) {

					Builder Builder = entry.getValue();
					Builder payload = Builder.getPayload();
					String[] data = payload.data;
					nullable[count] = data[4];
					count++;
				}

				String[] isNullable = nullable;// check for null case

				for (int i = 0; i < values.length; i++) {
					if (values[i].equalsIgnoreCase("null") && isNullable[i].equals("NO")) {
						System.out.println("Cannot insert NULL values in NOT NULL field");
						table.close();
						return;
					}
				}
				condition = new String[0];

				int pageNo = Builder.getPage(tableName, Integer.parseInt(values[0]));

				Map<Integer, Builder> data = Builder.getData(tableName, columnNames, condition);
				if (data.containsKey(Integer.parseInt(values[0]))) {
					System.out.println("Duplicate value for primary key");
					table.close();
					return;
				}

				byte[] plDataType = new byte[dataType.length - 1];
				int payLoadSize = Builder.getPayloadSize(tableName, values, plDataType, dataType);
				payLoadSize = payLoadSize + 6;

				int address = TreeFunctions.checkOverFlow(table, pageNo, payLoadSize);

				if (address != -1) {
					Builder Builder1 = Builder.BuilderForm(pageNo, Integer.parseInt(values[0]), (short) payLoadSize, plDataType,
							values);
					Builder.payload(table, Builder1, address);
				} else {
					TreeFunctions.splitLeaf(table, pageNo);
					int pNo = Builder.getPage(tableName, Integer.parseInt(values[0]));
					int addr = TreeFunctions.checkOverFlow(table, pNo, payLoadSize);
					Builder Builder1 = Builder.BuilderForm(pNo, Integer.parseInt(values[0]), (short) payLoadSize, plDataType,
							values);
					Builder.payload(table, Builder1, addr);
				}
				table.close();
				
			} catch (Exception e) {
				
				e.printStackTrace();
			}
		}
		
		public static void delete(String tableName, String[] cond) throws IOException {

			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";

			try {
				RandomAccessFile table = new RandomAccessFile(path, "rw");

				String condition[] = { "table_name", "=", tableName };
				String columnNames[] = { "*" };
				Map<Integer, Builder> column = Builder.getcolumn(tableName, columnNames, condition);
				String[] dataType = Builder.getDataType(column);

				int count = 0;
				String[] nullable = new String[column.size()];
				for (Map.Entry<Integer, Builder> entry : column.entrySet()) {

					Builder Builder = entry.getValue();
					Builder payload = Builder.getPayload();
					String[] data = payload.data;
					nullable[count] = data[4];
					count++;
				}

				String[] isNullable = nullable;

				Map<Integer, String> colNames = Builder.getColumnNames(tableName);

				condition = new String[0];

				int pageNo = Builder.getPage(tableName, Integer.parseInt(cond[2]));

				Map<Integer, Builder> data = Builder.getData(tableName, columnNames, condition);
				if (data.containsKey(Integer.parseInt(cond[2]))) {
					table.seek((PAGESIZE * pageNo) + 1);
					int noOfBuilders = table.readByte();
					short[] BuilderLocations = new short[noOfBuilders];
					table.seek((PAGESIZE * pageNo) + 8);
					for (int location = 0; location < noOfBuilders; location++) {
						BuilderLocations[location] = table.readShort();
					}
					Map<Integer, Builder> recordBuilders = new LinkedHashMap<Integer, Builder>();
					recordBuilders = Builder.getRecords(table, BuilderLocations, pageNo);

					String[] condition1 = { cond[0], "<>", cond[2] };
					String[] columnNames1 = { "*" };

					Map<Integer, Builder> filteredRecs = Builder.filterRecordsByData(colNames, recordBuilders, columnNames,
							condition1);
					short[] offsets = new short[filteredRecs.size()];
					int l = 0;
					for (Map.Entry<Integer, Builder> entry : filteredRecs.entrySet()) {
						Builder Builder = entry.getValue();
						offsets[l] = Builder.location;
						table.seek(pageNo * PAGESIZE + 8 + (2 * l));
						table.writeShort(offsets[l]);
						l++;
					}

					table.seek((PAGESIZE * pageNo) + 1);
					table.writeByte(offsets.length);
					table.writeShort(offsets[offsets.length - 1]);
					table.close();

				}

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

		}
		
		public static void dropTable(String tableName) {

			try {
				
				RandomAccessFile davisbaseTables = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
				Builder.updateMetaOffset(davisbaseTables, "davisbase_tables", tableName);

				RandomAccessFile davisbaseColumns = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
				Builder.updateMetaOffset(davisbaseColumns, "davisbase_columns", tableName);

				
				File file = new File("data/userdata/" + tableName + ".tbl");
				if(file.delete()) {
					
				}else {
				FileOutputStream fp=new FileOutputStream(file);
				fp=null;
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		public static void Query(String tableName, String[] columnNames, String[] condition) {

			try {

				tableName = tableName.trim();
				String path = "data/userdata/" + tableName + ".tbl";
				if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
					path = "data/catalog/" + tableName + ".tbl";

				RandomAccessFile table = new RandomAccessFile(path, "rw");
				int noOfPages = (int) (table.length() / PAGESIZE);

				Map<Integer, String> colNames = Builder.getColumnNames(tableName);
				Map<Integer, Builder> records = new LinkedHashMap<Integer, Builder>();
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
						Map<Integer, Builder> recordBuilders = new LinkedHashMap<Integer, Builder>();
						recordBuilders = Builder.getRecords(table, BuilderLocations, i);
						records.putAll(recordBuilders);
					}
				}

				if (condition.length > 0) {
					Map<Integer, Builder> filteredRecords = Builder.filterRecords(colNames, records, columnNames, condition);
					Builder.printTable(colNames, filteredRecords);
				} else {
					if (records.isEmpty()) {
						System.out.println("Empty Set..");
					} else {
						Builder.printTable(colNames, records);
					}
				}
				table.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

		}
		
		public static void update(String tableName, String[] set, String[] cond) {

			String path = "data/userdata/" + tableName + ".tbl";
			if (tableName.equalsIgnoreCase("davisbase_tables") || tableName.equalsIgnoreCase("davisbase_columns"))
				path = "data/catalog/" + tableName + ".tbl";

			try {
				RandomAccessFile file = new RandomAccessFile(path, "rw");

				String condition[] = { "table_name", "=", tableName };
				String columnNames[] = { "*" };
				Map<Integer, Builder> column = Builder.getcolumn(tableName, columnNames, condition);
				String[] dataType = Builder.getDataType(column);

				int count = 0;
				String[] nullable = new String[column.size()];
				for (Map.Entry<Integer, Builder> entry : column.entrySet()) {

					Builder Builder = entry.getValue();
					Builder payload = Builder.getPayload();
					String[] data = payload.data;
					nullable[count] = data[4];
					count++;
				}
				String[] isNullable = nullable;

				Map<Integer, String> colNames = Builder.getColumnNames(tableName);

				int k = -1;
				for (Map.Entry<Integer, String> entry : colNames.entrySet()) {
					String columnName = entry.getValue();
					if (columnName.equals(set[0])) {
						k = entry.getKey();
					}
				}

				if (cond.length > 0) {
					int key = Integer.parseInt(cond[2]);
					condition = new String[0];

					int pageno = Builder.getPage(tableName, Integer.parseInt(cond[2]));

					Map<Integer, Builder> data = Builder.getData(tableName, columnNames, condition);
					if (data.containsKey(Integer.parseInt(cond[2]))) {

						try {
							file.seek((pageno) * PAGESIZE + 1);
							int records = file.read();
							short[] offsetLocations = new short[records];

							for (int j = 0; j < records; j++) {
								file.seek((pageno) * PAGESIZE + 8 + 2 * j);
								offsetLocations[j] = file.readShort();
								file.seek(offsetLocations[j] + 2);
								int ky = file.readInt();
								if (key == ky) {
									int no = file.read();
									byte[] sc = new byte[no];
									file.read(sc);
									int seek_positions = 0;
									for (int i = 0; i < k - 2; i++) {
										seek_positions += Builder.dataLength(sc[i]);
									}
									file.seek(offsetLocations[j] + 6 + no + 1 + seek_positions);

									byte sc_update = sc[k - 2];
									switch (sc_update) {

									case 0x00:
										file.write(Integer.parseInt(set[2]));
										sc[k - 2] = 0x04;
										break;
									case 0x01:
										file.writeShort(Integer.parseInt(set[2]));
										sc[k - 2] = 0x05;
										break;
									case 0x02:
										file.writeInt(Integer.parseInt(set[2]));
										sc[k - 2] = 0x06;
										break;
									case 0x03:
										file.writeDouble(Double.parseDouble(set[2]));
										sc[k - 2] = 0x09;
										break;
									case 0x04:
										file.write(Integer.parseInt(set[2]));
										break;
									case 0x05:
										file.writeShort(Integer.parseInt(set[2]));
										break;
									case 0x06:
										file.writeInt(Integer.parseInt(set[2]));
										break;
									case 0x07:
										file.writeLong(Long.parseLong(set[2]));
										break;

									case 0x08:
										file.writeFloat(Float.parseFloat(set[2]));
										break;

									case 0x09:
										file.writeDouble(Double.parseDouble(set[2]));
										break;

									}

									file.seek(offsetLocations[j] + 7);
									file.write(sc);
									file.close();

								}
							}

						} catch (Exception e) {
							e.printStackTrace(System.out);
						}
					}
				} else {

					try {
						int no_of_pages = (int) (file.length() / PAGESIZE);
						for (int l = 0; l < no_of_pages; l++) {
							file.seek(l * PAGESIZE);
							byte pageType = file.readByte();
							if (pageType == 0x0D) {

								file.seek((l) * PAGESIZE + 1);
								int records = file.read();
								short[] offsetLocations = new short[records];

								for (int j = 0; j < records; j++) {
									file.seek((l) * PAGESIZE + 8 + 2 * j);
									offsetLocations[j] = file.readShort();
									file.seek(offsetLocations[j] + 6);

									int no = file.read();
									byte[] sc = new byte[no];
									file.read(sc);
									int seek_positions = 0;
									for (int i = 0; i < k - 2; i++) {
										seek_positions += Builder.dataLength(sc[i]);
									}
									file.seek(offsetLocations[j] + 6 + no + 1 + seek_positions);

									byte sc_update = sc[k - 2];
									switch (sc_update) {

									case 0x00:
										file.write(Integer.parseInt(set[2]));
										sc[k - 2] = 0x04;
										break;
									case 0x01:
										file.writeShort(Integer.parseInt(set[2]));
										sc[k - 2] = 0x05;
										break;
									case 0x02:
										file.writeInt(Integer.parseInt(set[2]));
										sc[k - 2] = 0x06;
										break;
									case 0x03:
										file.writeDouble(Double.parseDouble(set[2]));
										sc[k - 2] = 0x09;
										break;
									case 0x04:
										file.write(Integer.parseInt(set[2]));
										break;
									case 0x05:
										file.writeShort(Integer.parseInt(set[2]));
										break;
									case 0x06:
										file.writeInt(Integer.parseInt(set[2]));
										break;
									case 0x07:
										file.writeLong(Long.parseLong(set[2]));
										break;

									case 0x08:
										file.writeFloat(Float.parseFloat(set[2]));
										break;

									case 0x09:
										file.writeDouble(Double.parseDouble(set[2]));
										break;

									}

									file.seek(offsetLocations[j] + 7);
									file.write(sc);

								}
							}
						}
					} catch (Exception e) {
						e.printStackTrace(System.out);
					}
				}
			} catch (Exception e) {
				e.printStackTrace(System.out);
			}
		}
}
