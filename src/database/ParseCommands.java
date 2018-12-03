package database;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;

public class ParseCommands {

	public static boolean findTable(String tableName) {

		String filename = tableName + ".tbl";

		File catalog = new File("data/catalog/");
		String[] tablenames = catalog.list();
		for (String table : tablenames) {
			if (filename.equals(table))
				return true;

		}
		File userdata = new File("data/userdata/");
		String[] tables = userdata.list();
		for (String table : tables) {

			if (filename.equals(table))
				return true;
		}
		return false;
	}
	
	
	static void parseDeleteString(String userCommand) {
		String[] delete = userCommand.split("where");
		String[] table = delete[0].trim().split("from");
		String[] table1 = table[1].trim().split(" ");
		String tableName = table1[1].trim();
		String[] check = PageNav.cmpCheck(delete[1]);

		if (!findTable(tableName)) {
			System.out.println("Table not present");
			return;
		}
		try {
			ExecuteCommands.delete(tableName, check);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void parseDropTable(String dropTableString) throws FileNotFoundException {
		System.out.println("Parsing the string:\"" + dropTableString + "\"");
		String[] temp = dropTableString.split(" ");

		String tableName = temp[2].trim();

		if (!findTable(tableName)) {
			System.out.println("Table " + tableName + " is not present.");
			System.out.println();
		} else {
			ExecuteCommands.dropTable(tableName);
			
		}

	}

	public static void parseSearchString(String queryString) {
		System.out.println("Parsing the string:\"" + queryString + "\"");
		String tableName;
		String[] columnNames;
		String[] condition = new String[0];
		String temp[] = queryString.split("where");
		tableName = temp[0].split("from")[1].trim();
		columnNames = temp[0].split("from")[0].replaceAll("select", " ").split(",");

		if (!findTable(tableName)) {
			System.out.println("Table not present");
		}

		else {
			for (int i = 0; i < columnNames.length; i++)
				columnNames[i] = columnNames[i].trim();

			if (temp.length > 1)
				condition = PageNav.cmpCheck(temp[1]);

			ExecuteCommands.parseQuery(tableName, columnNames, condition);
		}
	}

	static void parseUpdateString(String userCommand) {
		String[] updates = userCommand.toLowerCase().split("set");
		String[] table = updates[0].trim().split(" ");
		String tablename = table[1].trim();
		String set_value;
		String where = null;
		if (!findTable(tablename)) {
			System.out.println("Table not present");
			return;
		}
		if (updates[1].contains("where")) {
			String[] findupdate = updates[1].split("where");
			set_value = findupdate[0].trim();
			where = findupdate[1].trim();
			ExecuteCommands.update(tablename, PageNav.cmpCheck(set_value), PageNav.cmpCheck((where)));
		} else {
			set_value = updates[1].trim();

			String[] no_where = new String[0];
			ExecuteCommands.update(tablename, PageNav.cmpCheck(set_value), no_where);
		}
	}

	public static void parseCreateString(String createTableString) {
		System.out.println("Parsing the string:\"" + createTableString + "\"");
		ArrayList<String> createTableTokens = new ArrayList<String>(Arrays.asList(createTableString.split(" ")));

		String tableName = createTableTokens.get(2);
		String[] temp = createTableString.replaceAll("\\(", "").replaceAll("\\)", "").split(tableName);
		String[] columnNames = temp[1].trim().split(",");

		for (int i = 0; i < columnNames.length; i++)
			columnNames[i] = columnNames[i].trim();

		if (findTable(tableName)) {
			System.out.println("Table " + tableName + " is already present.");
			System.out.println();
		} else {
			RandomAccessFile table;
			try {
				table = new RandomAccessFile("data/userdata/" + tableName + ".tbl", "rw");
				ExecuteCommands.createTable(table, tableName, columnNames);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public static void parseInsertString(String insertString) {

		String[] insert = insertString.split(" ");
		String tableName = insert[2].trim();
		String values = insertString.split("values")[1].replaceAll("\\(", "").replaceAll("\\)", "").trim();

		String[] insertValues = values.split(",");
		for (int i = 0; i < insertValues.length; i++)
			insertValues[i] = insertValues[i].trim();

		if (!findTable(tableName)) {
			System.out.println("Table " + tableName + " does not exist.");
			System.out.println();
			return;
		} else
			ExecuteCommands.insert(tableName, insertValues);

	}
}
