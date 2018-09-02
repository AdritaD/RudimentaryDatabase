package databaseLite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class Common {
	static int pSize=512;
	
	public static void createInfoSchema() throws IOException {
		File dataFolder= new File("data");
		if ((!dataFolder.exists()) && (!dataFolder.isDirectory())){
			dataFolder.mkdir();
		}
		try {
			File allTables = new File("data/davisbase_tables.tbl");
			if (!allTables.exists()){
				RandomAccessFile allTablesTbl = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
				allTablesTbl.setLength(pSize);
				allTablesTbl.seek(0);
				allTablesTbl.write(0x0D);
				allTablesTbl.writeByte(0x02); //
				allTablesTbl.writeShort(463);
				allTablesTbl.writeInt(0);
				allTablesTbl.writeInt(0);
				allTablesTbl.writeShort(488);
				allTablesTbl.writeShort(463);
				
				allTablesTbl.seek(488);
				allTablesTbl.writeShort(20);
				allTablesTbl.writeInt(1); 
				allTablesTbl.writeByte(1);
				allTablesTbl.writeByte(28);
				allTablesTbl.writeBytes("davisbase_tables");
				
				allTablesTbl.seek(463);
				allTablesTbl.writeShort(21);
				allTablesTbl.writeInt(2); 
				allTablesTbl.writeByte(1);
				allTablesTbl.writeByte(29);
				allTablesTbl.writeBytes("davisbase_columns");
				
				allTablesTbl.close();
			}
			
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			File allColumns = new File("data/davisbase_columns.tbl");
			if (!allColumns.exists()){
				RandomAccessFile allColumnsTbl = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
				
				allColumnsTbl.setLength(pSize);
				allColumnsTbl.seek(0);       
				allColumnsTbl.writeByte(0x0D); 
				allColumnsTbl.writeByte(0x08); 
								
				allColumnsTbl.writeShort(128); 	
				allColumnsTbl.writeInt(0); 
				allColumnsTbl.writeInt(0); 
				
				allColumnsTbl.writeShort(469);
				allColumnsTbl.writeShort(422);
				allColumnsTbl.writeShort(378);
				allColumnsTbl.writeShort(330);
				allColumnsTbl.writeShort(281);
				allColumnsTbl.writeShort(234);
				allColumnsTbl.writeShort(177);
				allColumnsTbl.writeShort(128);

				writeToAllColumns(allColumnsTbl,469, 33, 1, 5, 28, 17, 
						15, 4, 14, "davisbase_tables","rowid","INT", 1, "NO");	
				
				writeToAllColumns(allColumnsTbl,422, 39, 2, 5, 28, 22, 
						16, 4, 14, "davisbase_tables","table_name","TEXT", 2, "NO");
				
				writeToAllColumns(allColumnsTbl,378, 34, 3, 5, 29, 17, 
						15, 4, 14, "davisbase_columns","rowid","INT", 1, "NO");
				
				writeToAllColumns(allColumnsTbl,330, 40, 4, 5, 29, 22, 
						16, 4, 14, "davisbase_columns","table_name","TEXT", 2, "NO");
				
				writeToAllColumns(allColumnsTbl,281, 41, 5, 5, 29, 23, 
						16, 4, 14, "davisbase_columns","column_name","TEXT", 3, "NO");
				
				writeToAllColumns(allColumnsTbl,234, 39, 6, 5, 29, 21, 
						16, 4, 14, "davisbase_columns","data_type","TEXT", 4, "NO");
				
				writeToAllColumns(allColumnsTbl,177, 49, 7, 5, 29, 28, 
						19, 4, 14, "davisbase_columns","ordinal_position","TINYINT", 5, "NO");
				
				writeToAllColumns(allColumnsTbl,128, 41, 8, 5, 29, 23, 
						16, 4, 14, "davisbase_columns","is_nullable","TEXT", 6, "NO");
				
				allColumnsTbl.close();
			
			
			}
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void writeToAllColumns(RandomAccessFile file, int offset, int payloadSize, int rowIDnum,
			int numCol, int col_1DT, int col_2DT, int col_3DT, int col_4DT, int col_5DT,
			String col_1val, String col_2val, String col_3val, int col_4val, String col_5val) throws IOException {
		
		file.seek(offset);
		file.writeShort(payloadSize);
		file.writeInt(rowIDnum); 
		file.writeByte(numCol);
		file.writeByte(col_1DT);
		file.writeByte(col_2DT);
		file.writeByte(col_3DT);
		file.writeByte(col_4DT);
		file.writeByte(col_5DT);
		file.writeBytes(col_1val); 
		file.writeBytes(col_2val); 
		file.writeBytes(col_3val); 
		file.writeByte(col_4val); 
		file.writeBytes(col_5val); 
	}
	public static boolean tablePresent(String t_name){
		t_name = t_name+".tbl";
		boolean exists=false;
		try {
			File folder = new File("data");
			String[] oldT;
			oldT = folder.list();
			for (int i=0; i<oldT.length; i++) {
				if(oldT[i].equals(t_name))
					exists=true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Unable to access user_data directory");
		}
		return exists;
	}
	public static String[] parser_eq(String eq){
		String comp[] = new String[3];
		String tem[] = new String[2];
		if(eq.contains("=")) {
			tem = eq.split("=");
			comp[0] = tem[0].trim();
			comp[1] = "=";
			comp[2] = tem[1].trim();
		}
		if(eq.contains("<")) {
			tem = eq.split("<");
			comp[0] = tem[0].trim();
			comp[1] = "<";
			comp[2] = tem[1].trim();
		}
		if(eq.contains(">")) {
			tem = eq.split(">");
			comp[0] = tem[0].trim();
			comp[1] = ">";
			comp[2] = tem[1].trim();
		}
		if(eq.contains("<=")) {
			tem = eq.split("<=");
			comp[0] = tem[0].trim();
			comp[1] = "<=";
			comp[2] = tem[1].trim();
		}
		if(eq.contains(">=")) {
			tem = eq.split(">=");
			comp[0] = tem[0].trim();
			comp[1] = ">=";
			comp[2] = tem[1].trim();
		}
		return comp;
	}
}
