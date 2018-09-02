package databaseLite;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Scanner;

public class Main {
	
	static String prompt = "adb> ";
	static String copyright = "Adrita Dutta";
	static boolean isExit = false;
	static long pageSize = 512; 
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	
	public static void main(String[] args) throws IOException {
		splashScreen();
		String userCommand = ""; 
		Common.createInfoSchema();
		while(!isExit) {
			System.out.print(prompt);
			userCommand = scanner.next().replace("\n", "").replace("\r", "").trim().toLowerCase();
			parseCommand(userCommand);
		}
		System.out.println("Exiting...");

	}
	public static void splashScreen() {
		System.out.println(line("-",80));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		System.out.println(copyright);
		System.out.println("\nType \"help;\" to display supported commands.");
		System.out.println(line("-",80));
	}
	
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	public static void help() {
		System.out.println(line("*",80));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSELECT * FROM table_name;                       					Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;  					Display records whose rowid is <id>.");
		System.out.println("\tDROP TABLE table_name;                           					Remove table data and its schema.");
		System.out.println("\tSHOW TABLES;                                     					Shows tables in database");
		System.out.println("\tCREATE TABLE table_name(rowid int, columnName text....);   <-Example  Creates table in database");
		System.out.println("\tINSERT INTO table_name VALUES values list;       					Inserts row in table");
		System.out.println("\tDELETE FROM TABLE table_name WHERE condition;          			Deletes row from table");
		System.out.println("\tUPDATE table_name SET column_name=value WHERE condition;          Updates table");
		System.out.println("\tHELP;                                            					Show this help information");
		System.out.println("\tEXIT;                                            					Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",80));
	}
	public static void parseCommand (String userCommand) {
		ArrayList<String> commandTokens = new ArrayList<String>(Arrays.asList(userCommand.split(" ")));
		
		switch (commandTokens.get(0)) {
			case "show":
				showTables();
					break;
			case "insert":
				insertPre(userCommand);
				break;		
			case "select":
				selectPre(userCommand);
				break;
			case "drop":
				dropPre(userCommand);
				break;
			case "delete":
				deletePre(userCommand);
				break;	
			case "create": 
				Queries.createTable(userCommand);
				break;
			case "update":
				updatePre(userCommand);
				break;
			case "help":
				help();
				break;
			case "exit":
				isExit = true;
				break;
			case "quit":
				isExit = true;
			default:
				System.out.println("Command not understood: \"" + userCommand + "\"");
				break;
		}
	}
	public static void showTables() {
		System.out.println("---Calling the method to process the command");
		System.out.println("Parsing the string:\"show tables\"");
		String t = "davisbase_tables";
		String[] col = {"table_name"};
		String[] compare = new String[0];
		Queries.select(t, col, compare);
	}
	public static void insertPre(String str) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + str + "\"");
		
		String[] tokens=str.split(" ");
		String table = tokens[2];
		String[] temp = str.split("values");
		String temporary=temp[1].trim();
		String[] insert_vals = temporary.substring(1, temporary.length()-1).split(",");
		for(int i = 0; i < insert_vals.length; i++)
			insert_vals[i] = insert_vals[i].trim();
		if(!Common.tablePresent(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			try{
				RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
				Queries.insert(file, table, insert_vals);
				file.close();

			}catch(Exception e){
				System.out.println(e);
			}
		}
	}
	public static void selectPre(String str) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + str + "\"");
		String[] compare;
		String[] column;
		String[] temp = str.split("where");
		if(temp.length > 1){
			String tmp = temp[1].trim();
			compare = Common.parser_eq(tmp);
		}
		else{
			compare = new String[0];
		}
		String[] select = temp[0].split("from");
		String tName = select[1].trim();
		String cols = select[0].replace("select", "").trim();
		if(cols.contains("*")){
			column = new String[1];
			column[0] = "*";
		}
		else{
			column = cols.split(",");
			for(int i = 0; i < column.length; i++)
				column[i] = column[i].trim();
		}
		if(!Common.tablePresent(tName)){
			System.out.println("Table "+tName+" does not exist.");
		}
		else
		{
		    Queries.select(tName, column, compare);
		}
	}
	
	public static void deletePre(String str) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + str + "\"");
		
		String[] tokens=str.split(" ");
		String table = tokens[3];
		String[] temp = str.split("where");
		String cmpTemp = temp[1];
		String[] cmp = Common.parser_eq(cmpTemp);
		if(!Common.tablePresent(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Queries.delete(table, cmp);
		}
		
		
	}
	
	public static void updatePre(String str) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + str + "\"");
		String[] tokens=str.split(" ");
		String table = tokens[1];
		String[] temp1 = str.split("set");
		String[] temp2 = temp1[1].split("where");
		String cmpTemp = temp2[1];
		String setTemp = temp2[0];
		String[] cmp = Common.parser_eq(cmpTemp);
		String[] set = Common.parser_eq(setTemp);
		if(!Common.tablePresent(table)){
			System.out.println("Table "+table+" does not exist.");
		}
		else
		{
			Queries.update(table, cmp, set);
		}
		
	}

	public static void dropPre(String str) {
		System.out.println("STUB: Calling the method to process the command");
		System.out.println("Parsing the string:\"" + str + "\"");
		String[] tok=str.split(" ");
		String t_name = tok[2];
		if(!Common.tablePresent(t_name)){
			System.out.println("Table "+t_name+" does not exist.");
		}
		else
		{
			Queries.drop(t_name);
		}		

	}
}
