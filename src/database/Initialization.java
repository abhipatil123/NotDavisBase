package database;
import java.io.File;
import java.io.RandomAccessFile;

public class Initialization {
	
	public static int PAGESIZE = 512;// page size
	
	public static void initialize() {
		File file = new File("data/catalog");
		File userData = new File("data/userdata");
		file.mkdirs();
		userData.mkdirs();
		if (file.isDirectory()) {
			File davisBaseTables = new File("data/catalog/davisbase_tables.tbl");
			File davisBaseColumns = new File("data/catalog/davisbase_columns.tbl");

			if (!davisBaseTables.exists()) {
				initializeDatabase();
			}
			if (!davisBaseColumns.exists()) {
				initializeDatabase();
			}
		} else {
			initializeDatabase();
		}

	}
	
	
	public static void initializeDatabase() {

		File data = new File("data/catalog");
		File userData = new File("data/userdata");
		data.mkdir();
		userData.mkdir();
		createDavisBase_Tables();
		createDavisBase_Columns();
	}

	
	public static void createDavisBase_Tables() {

		try {
			@SuppressWarnings("resource")
			RandomAccessFile table = new RandomAccessFile("data/catalog/davisbase_tables.tbl", "rw");
			table.setLength(PAGESIZE * 1);
			table.seek(0);
			table.write(0x0D);
			table.write(0x02);
			table.writeShort(PAGESIZE - 32 - 33);
			table.writeInt(-1);
			table.writeShort(PAGESIZE - 32);
			table.writeShort(PAGESIZE - 32 - 33);

			table.seek(PAGESIZE - 32);
			table.writeShort(26);
			table.writeInt(1);
			table.writeByte(3);
			table.writeByte(28);
			table.write(0x06);
			table.write(0x05);
			table.writeBytes("davisbase_tables");
			table.writeInt(2);
			table.writeShort(34);

			table.seek(PAGESIZE - 32 - 33);
			table.writeShort(19);
			table.writeInt(2);
			table.writeByte(3);
			table.writeByte(29);
			table.write(0x06);
			table.write(0x05);
			table.writeBytes("davisbase_columns");
			table.writeInt(10);
			table.writeShort(34);

		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	
	public static void createDavisBase_Columns() {

		int BuilderHeader = 6;
		try {

			@SuppressWarnings("resource")
			RandomAccessFile column = new RandomAccessFile("data/catalog/davisbase_columns.tbl", "rw");
			column.setLength(PAGESIZE * 1);
			column.seek(0);
			column.write(0x0D);
			column.write(10);

			int recordSize[] = new int[] { 33, 39, 40, 43, 34, 40, 41, 39, 49, 41 };
			int offset[] = new int[10];

			offset[0] = PAGESIZE - recordSize[0] - BuilderHeader;

			column.seek(4);

			column.writeInt(-1);

			for (int i = 1; i < offset.length; i++) {
				offset[i] = offset[i - 1] - (recordSize[i] + BuilderHeader);

			}
			column.seek(2);
			column.writeShort(offset[9]);

			column.seek(8);
			for (int i = 0; i < offset.length; i++) {
				column.writeShort(offset[i]);
			}

			column.seek(offset[0]);
			column.writeShort(recordSize[0]);
			column.writeInt(1);
			column.writeByte(5);
			column.write(28);
			column.write(17);
			column.write(15);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("rowid");
			column.writeBytes("INT");
			column.write(1);
			column.writeBytes("NO");

			column.seek(offset[1]);
			column.writeShort(recordSize[1]);
			column.writeInt(2);
			column.writeByte(5);
			column.write(28);
			column.write(22);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("table_name");
			column.writeBytes("TEXT");
			column.write(2);
			column.writeBytes("NO");

			column.seek(offset[2]);
			column.writeShort(recordSize[2]);
			column.writeInt(3);
			column.writeByte(5);
			column.write(28);
			column.write(24);
			column.write(15);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("record_count");
			column.writeBytes("INT");
			column.write(3);
			column.writeBytes("NO");

			column.seek(offset[3]);
			column.writeShort(recordSize[3]);
			column.writeInt(4);
			column.writeByte(5);
			column.write(28);
			column.write(22);
			column.write(20);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_tables");
			column.writeBytes("avg_length");
			column.writeBytes("SMALLINT");
			column.write(4);
			column.writeBytes("NO");

			column.seek(offset[4]);
			column.writeShort(recordSize[4]);
			column.writeInt(5);
			column.writeByte(5);
			column.write(29);
			column.write(17);
			column.write(15);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("rowid");
			column.writeBytes("INT");
			column.write(1);
			column.writeBytes("NO");

			column.seek(offset[5]);
			column.writeShort(recordSize[5]);
			column.writeInt(6);
			column.writeByte(5);
			column.write(29);
			column.write(22);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("table_name");
			column.writeBytes("TEXT");
			column.write(2);
			column.writeBytes("NO");

			column.seek(offset[6]);
			column.writeShort(recordSize[6]);
			column.writeInt(7);
			column.writeByte(5);
			column.write(29);
			column.write(23);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("column_name");
			column.writeBytes("TEXT");
			column.write(3);
			column.writeBytes("NO");

			column.seek(offset[7]);
			column.writeShort(recordSize[7]);
			column.writeInt(8);
			column.writeByte(5);
			column.write(29);
			column.write(21);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("data_type");
			column.writeBytes("TEXT");
			column.write(4);
			column.writeBytes("NO");

			column.seek(offset[8]);
			column.writeShort(recordSize[8]);
			column.writeInt(9);
			column.writeByte(5);
			column.write(29);
			column.write(28);
			column.write(19);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("ordinal_position");
			column.writeBytes("TINYINT");
			column.write(5);
			column.writeBytes("NO");

			column.seek(offset[9]);
			column.writeShort(recordSize[9]);
			column.writeInt(10);
			column.writeByte(5);
			column.write(29);
			column.write(23);
			column.write(16);
			column.write(0x04);
			column.write(14);
			column.writeBytes("davisbase_columns");
			column.writeBytes("is_nullable");
			column.writeBytes("TEXT");
			column.write(6);
			column.writeBytes("NO");

		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
