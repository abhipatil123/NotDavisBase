package database;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class DavisBase {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";
	static String version = "v1.0b";
	static String copyright = "Chris Irwin Davis";
	static boolean isExit = false;
	/*
	 * Page size for all files is 512 bytes.
	 */
	static long pageSize = 512;

	static Scanner scanner = new Scanner(System.in).useDelimiter(";");

	public static void main(String[] args) throws FileNotFoundException {

		/* Display the welcome screen */
		welcomScreen();

		/* Variable to collect user input from the prompt */
		String userCommand = "";

		Initialization.initialize(); //Initialize database with davisbase_tables and davisbase_columns

		while (!isExit) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			// userCommand = userCommand.replace("\n", "").replace("\r", "");
			parseUserCommand(userCommand);
		}
		System.out.println("Exiting...");
	}

	public static void welcomScreen() {
		System.out.println(line("-", 80));
		System.out.println("Welcome to DavisBaseLite"); 
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-", 80));
	}

	public static String line(String s, int num) {
		String a = "";
		for (int i = 0; i < num; i++) {
			a += s;
		}
		return a;
	}

	/**
	 * Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*", 80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tCREATE TABLE table_name(col1 data_type, col2 data_type) ; 		Create a new table.");
		System.out.println("\tINSERT INTO TABLE table_name(val1, val2) ; 	  			Insert into table.");
		System.out.println("\tSELECT * FROM table_name;                        			Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  			Display records whose rowid is <id>.");
		System.out.println("\tUPDATE table_name SET COL_NAME=VAL WHERE rowid = <value>; 		Update table column record.");
		System.out.println("\tDROP TABLE table_name;                           			Remove table data and its schema.");
		System.out.println("\tDELETE FROM TABLE table_name WHERE rowid = <value>;			Remove record from the table  whose rowid is <id>.");
		System.out.println("\tVERSION;                                         			Show the program version.");
		System.out.println("\tHELP;                                           			Show this help information");
		System.out.println("\tEXIT;                                            			Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*", 80));
	}

	/** return the DavisBase version */
	public static String getVersion() {
		return version;
	}

	public static String getCopyright() {
		return copyright;
	}

	public static void displayVersion() {
		System.out.println("DavisBaseLite Version " + getVersion());
		System.out.println(getCopyright());
	}

	public static void parseUserCommand(String userCommand) throws FileNotFoundException {

		/*
		 * splitCommand is an array of Strings that contains one string per array
		 * element The first string can be used to determine the type of command 
		 */
		ArrayList<String> splitCommand = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		switch (splitCommand.get(0)) {

		case "show":
			String[] condition = new String[0];
			String[] columnNames = { "*" };
			ExecuteCommands.parseQuery("davisbase_tables", columnNames, condition);
			break;
		case "select":
			System.out.println("CASE: SELECT");
			ParseCommands.parseSearchString(userCommand);
			break;
		case "drop":
			System.out.println("CASE: DROP");
			ParseCommands.parseDropTable(userCommand);
			break;
		case "create":
			System.out.println("CASE: CREATE");
			ParseCommands.parseCreateString(userCommand);
			break;
		case "update":
			System.out.println("CASE: UPDATE");
			ParseCommands.parseUpdateString(userCommand);
			break;
		case "help":
			help();
			break;
		case "version":
			displayVersion();
			break;
		case "exit":
			isExit = true;
			break;
		case "delete":
			ParseCommands.parseDeleteString(userCommand);
			break;
		case "insert":
			ParseCommands.parseInsertString(userCommand);
			break;
		case "quit":
			isExit = true;
		default:
			System.out.println("I didn't understand the command: \"" + userCommand + "\"");
			break;
		}
	}
}
