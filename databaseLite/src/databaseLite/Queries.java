package databaseLite;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

public class Queries {
	static int pSize = 512;
	
	public static void delete(String tab, String[] cmp){
		try{
		int key = new Integer(cmp[2]);

		RandomAccessFile file = new RandomAccessFile("data/"+tab+".tbl", "rw");
		int no_pages = (int)(file.length()/(new Long(pSize)));
		int page = 0;
		for(int p = 1; p <= no_pages; p++) {
			int[] keys = Tree.keys(file, p);
			boolean ch1=false;
			for(int i : keys) {
				if(key == i)
					ch1=true;
			}
			byte type1=0x05;
			file.seek((p-1)*pSize);
			type1 = file.readByte();
			if(ch1&type1==0x0D){
				page = p;
				break;
			}
		}
		if(page==0)
		{
			System.out.println("The given key value does not exist");
			return;
		}
		short[] cellAddress = Tree.getCellArr(file, page);
		int k = 0;
		for(int i = 0; i < cellAddress.length; i++)
		{
			file.seek((page-1)*pSize+12+i*2);
			short offset1 = file.readShort();
			long orig1 = (page-1)*pSize;
			long loc = orig1 + offset1;
			String[] vals = get_values(file, loc);
			int x = new Integer(vals[0]);
			if(x!=key)
			{
				file.seek((page-1)*pSize+12+k*2);
				file.writeShort(cellAddress[i]);
				k++;
			}
		}
		file.seek((page-1)*pSize+1);
		file.writeByte((byte)k);
		}catch(Exception e)
		{
			System.out.println(e);
		}
		
	}
	public static void select(String t, String[] cols, String[] cmp) {
		try {

			RandomAccessFile f = new RandomAccessFile("data/" + t + ".tbl", "rw");
			String[] columnName = getColN(t);
			String[] type = d_Type(t);
			TableContent tcon = new TableContent();
			filter2(f, cmp, columnName, type, tcon);
			tcon.display(cols);
			f.close();
		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void update(String table, String[] cmp, String[] set){
		try{
			int key = new Integer(cmp[2]);
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			int numPages = (int)(file.length()/(new Long(pSize)));
			int page = 0;
			for(int p = 1; p <= numPages; p++) {
				boolean ch1=false;
				int[] keys = Tree.keys(file, p);
				for(int i : keys) {
					if(key == i)
						ch1=true;
				}
				byte ch2=0x05;
				file.seek((p-1)*pSize);
				ch2 = file.readByte();
				if(ch1&&ch2==0x0D){
					page = p;
				}
			}
			if(page==0)
			{
				System.out.println("The given key value does not exist");
				return;
			}
			int[] keys = Tree.keys(file, page);
			int x = 0;
			for(int i = 0; i < keys.length; i++)
				if(keys[i] == key)
					x = i;
			file.seek((page-1)*pSize+12+x*2);
			int offset = file.readShort();			
			file.seek((page-1)*pSize+12+x*2);
			short offset2 = file.readShort();
			long orig2 = (page-1)*pSize;
			long loc = orig2 + offset2;
			String[] cols = getColN(table);
			String[] values = get_values(file, loc);
			String[] type = d_Type(table);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";
			for(int i = 0; i < cols.length; i++)
				if(cols[i].equals(set[0]))
					x = i;
			values[x] = set[2];
			String[] nullable = ifNull(table);
			for(int i = 0; i < nullable.length; i++){
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("NULL-value constraint violation");
					return;
				}
			}
			byte[] stc = new byte[cols.length-1];
			int plsize = payload_S(table, values, stc);
			Tree.update_LCell(file, page, offset, plsize, key, stc, values);
			file.close();

		}catch(Exception e){
			System.out.println(e);
		}
	}
	
	public static void createTable(String userCommand) {
		// ArrayList<String> tokens = new
		// ArrayList<String>(Arrays.asList(userCommand.split(" ")));

		// String[] tokens1=userCommand.split(" ");
		// String tableName1 = tokens1[2];
		String[] tableSplit = userCommand.split("[(]");
		String tableName = tableSplit[0].split(" ")[2];
		String[] columns = tableSplit[1].substring(0, tableSplit[1].length() - 1).split(",");

		for (int i = 0; i < columns.length; i++)
			columns[i] = columns[i].trim();

		if (Common.tablePresent(tableName)) {
			System.out.println("Table " + tableName + " already exists.");
		} else {
			createWrite(tableName, columns);
			System.out.println(tableName+ " table created");
		}
	}

	public static void createWrite(String tName, String[] columns) {
		try {

			RandomAccessFile file = new RandomAccessFile("data/" + tName + ".tbl", "rw");
			file.setLength(pSize);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();

			file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");

			int numOfPages = (int) (file.length() / (new Long(pSize)));
			int page = 1;
			for (int p = 1; p <= numOfPages; p++) {
				file.seek((page - 1) * pSize + 4);
				int rm = file.readInt();
				if (rm == 0)
					page = p;
			}

			int[] keys = Tree.keys(file, page);
			int l = keys[0];
			for (int i = 0; i < keys.length; i++)
				if (keys[i] > l)
					l = keys[i];
			file.close();

			String[] values = { Integer.toString(l + 1), tName };

			RandomAccessFile file1 = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			insert(file1, "davisbase_tables", values);
			file1.close();

			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");

			numOfPages = (int) (file.length() / (new Long(pSize)));
			page = 1;
			for (int p = 1; p <= numOfPages; p++) {
				file.seek((p - 1) * pSize + 4);
				int rm = file.readInt();
				if (rm == 0)
					page = p;
			}

			keys = Tree.keys(file, page);
			l = keys[0];
			for (int i = 0; i < keys.length; i++)
				if (keys[i] > l)
					l = keys[i];
			for (int i = 0; i < columns.length; i++) {
				l = l + 1;
				String[] token = columns[i].split(" ");
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String pos = Integer.toString(i + 1);
				String nullable;
				if (token.length > 2)
					nullable = "NO";
				else
					nullable = "YES";
				String[] value = { Integer.toString(l), tName, col_name, dt, pos, nullable };
				insert(file, "davisbase_columns", value);
			}

		} catch (Exception e) {
			System.out.println(e);
		}
	}

	public static void insert(RandomAccessFile f, String t, String[] val) throws IOException {
		String[] data_type = d_Type(t);
		String[] n = ifNull(t);

		for (int x = 0; x < n.length; x++) {
			if (val[x].equals("null") && n[x].equals("NO")) {
				System.out.println("null constraint violation");
				return;
			}
		}
		int k = new Integer(val[0]);
		int page = search_keyP(f, k);
		if (page != 0) {
			boolean check;
			int[] keys = Tree.keys(f, page);
			for (int i : keys)
				if (k == i)
					check = true;
			check = false;
			if (check) {
				System.out.println("Unique constraint violation");
				return;
			}
		}
		if (page == 0)
			page = 1;
		byte[] sc = new byte[data_type.length - 1];
		short p_size = (short) payload_S(t, val, sc);
		int c_size = p_size + 6;
		int offset = Tree.c_Leaf_s(f, page, c_size);
		if (offset != -1) {
			Tree.insertLeaf(f, page, offset, p_size, k, sc, val);
		} else {
			Tree.splitL(f, page);
			insert(f, t, val);
		}
	}

	public static String[] d_Type(String table) {
		String[] data_type = new String[0];
		try {
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			TableContent t_con = new TableContent();
			String[] col_n = { "rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable" };
			String[] cmp = { "table_name", "=", table };
			filter(file, cmp, col_n, t_con);
			HashMap<Integer, String[]> content = t_con.con;
			ArrayList<String> array = new ArrayList<String>();
			for (String[] x : content.values()) {
				array.add(x[3]);
			}
			int size = array.size();
			data_type = array.toArray(new String[size]);
			file.close();
			return data_type;
		} catch (Exception e) {
			System.out.println(e);
		}
		return data_type;
	}

	public static void filter(RandomAccessFile f, String[] compare, String[] c, TableContent t_con) {
		try {
			int pages = (int) (f.length() / (new Long(pSize)));
			for (int p = 1; p <= pages; p++) {
				f.seek((p - 1) * pSize);
				byte p_Type = f.readByte();
				if (p_Type == 0x0D) {
					f.seek((p - 1) * pSize + 1);
					byte cells = f.readByte();
					for (int ctr = 0; ctr < cells; ctr++) {
						f.seek((p - 1) * pSize + 12 + ctr * 2);
						short offset = f.readShort();
						long original = (p - 1) * pSize;
						long loc = original + offset;
						String[] vals = get_values(f, loc);
						int rid = Integer.parseInt(vals[0]);
						boolean check = compare(vals, rid, compare, c);
						if (check)
							t_con.add(rid, vals);
					}
				} else
					continue;
			}
			t_con.col_name = c;
			t_con.format = new int[c.length];
		} catch (Exception e) {
			System.out.println("Error at filter");
			e.printStackTrace();
		}
	}

	public static String[] get_values(RandomAccessFile f, long location) {

		String[] val = null;
		String date1 = "yyyy-MM-dd_HH:mm:ss";
		try {
			SimpleDateFormat d_format = new SimpleDateFormat(date1);
			f.seek(location + 2);
			int k = f.readInt();
			int columns = f.readByte();
			byte[] ss = new byte[columns];
			f.read(ss);
			val = new String[columns + 1];
			val[0] = Integer.toString(k);
			for (int i = 1; i <= columns; i++) {
				switch (ss[i - 1]) {
				case 0x00:
					f.readByte();
					val[i] = "null";
					break;
				case 0x01:
					f.readShort();
					val[i] = "null";
					break;
				case 0x02:
					f.readInt();
					val[i] = "null";
					break;
				case 0x03:
					f.readLong();
					val[i] = "null";
					break;
				case 0x04:
					val[i] = Integer.toString(f.readByte());
					break;
				case 0x05:
					val[i] = Integer.toString(f.readShort());
					break;
				case 0x06:
					val[i] = Integer.toString(f.readInt());
					break;
				case 0x07:
					val[i] = Long.toString(f.readLong());
					break;
				case 0x08:
					val[i] = String.valueOf(f.readFloat());
					break;
				case 0x09:
					val[i] = String.valueOf(f.readDouble());
					break;
				case 0x0A:
					Long read = f.readLong();
					Date d = new Date(read);
					val[i] = d_format.format(d);
					break;
				case 0x0B:
					read = f.readLong();
					Date d1 = new Date(read);
					val[i] = d_format.format(d1).substring(0, 10);
					break;
				default:
					int size = new Integer(ss[i - 1] - 0x0C);
					byte[] b = new byte[size];
					f.read(b);
					val[i] = new String(b);
					break;
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return val;
	}

	public static boolean compare(String[] v, int r, String[] compare, String[] col) {

		boolean yesno = false;
		if (compare.length == 0) {
			yesno = true;
		} else {
			int position = 1;
			for (int i = 0; i < col.length; i++) {
				if (col[i].equals(compare[0])) {
					position = i + 1;
					break;
				}
			}
			if (position == 1) {
				int num = Integer.parseInt(compare[2]);
				String toCompare = compare[1];
				switch (toCompare) {
				case "=":
					if (r == num)
						yesno = true;
					else
						yesno = false;
					break;
				case ">":
					if (r > num)
						yesno = true;
					else
						yesno = false;
					break;
				case ">=":
					if (r >= num)
						yesno = true;
					else
						yesno = false;
					break;
				case "<":
					if (r < num)
						yesno = true;
					else
						yesno = false;
					break;
				case "<=":
					if (r <= num)
						yesno = true;
					else
						yesno = false;
					break;
				case "!=":
					if (r != num)
						yesno = true;
					else
						yesno = false;
					break;
				}
			} else {
				if (compare[2].equals(v[position - 1]))
					yesno = true;
				else
					yesno = false;
			}
		}
		return yesno;
	}

	public static String[] ifNull(String tk) {
		String[] n = new String[0];
		try {
			TableContent tc = new TableContent();
			RandomAccessFile f = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			String[] compare = { "table_name", "=", tk };
			String[] c_name = { "rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable" };
			filter(f, compare, c_name, tc);
			ArrayList<String> arr = new ArrayList<String>();
			HashMap<Integer, String[]> content = tc.con;
			for (String[] j : content.values()) {
				arr.add(j[5]);
			}
			int size = arr.size();
			n = arr.toArray(new String[size]);
			f.close();
			return n;
		} catch (Exception e) {
			System.out.println(e);
		}
		return n;
	}

	public static int search_keyP(RandomAccessFile f, int k) {
		int val = 1;
		try {
			int noP = (int) (f.length() / (new Long(pSize)));
			for (int pg = 1; pg <= noP; pg++) {
				f.seek((pg - 1) * pSize);
				byte p_type = f.readByte();
				if (p_type == 0x0D) {
					int[] key = Tree.keys(f, pg);
					if (key.length == 0)
						return 0;
					f.seek((pg - 1) * pSize + 4);
					int rm = f.readInt();
					if (key[0] <= k && k <= key[key.length - 1]) {
						return pg;
					} else if (rm == 0 && key[key.length - 1] < k) {
						return pg;
					}
				}
			}
		} catch (Exception e) {
			System.out.println(e);
		}
		return val;
	}

	public static int payload_S(String tab, String[] v, byte[] sc) {
		String[] dataType = d_Type(tab);
		int siz = dataType.length;
		for (int x = 1; x < dataType.length; x++) {
			sc[x - 1] = getSc(v[x], dataType[x]);
			siz = siz + fLen(sc[x - 1]);
		}
		return siz;
	}

	public static byte getSc(String v, String dT) {
		if (v.equals("null")) {
			switch (dT) {
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
			switch (dT) {
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
				return (byte) (v.length() + 0x0C);
			default:
				return 0x00;
			}
		}
	}

	public static short fLen(byte sc) {
		switch (sc) {
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
			return (short) (sc - 0x0C);
		}
	}

	public static void filter2(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, TableContent tcon) {
		try {
			int numOfPages = (int) (file.length() / (new Long(pSize)));
			for (int page = 1; page <= numOfPages; page++) {
				file.seek((page - 1) * pSize);
				byte pageType = file.readByte();
				if (pageType == 0x0D) {
					file.seek((page-1)*pSize+1);
					byte numOfCells = file.readByte();
					for (int i = 0; i < numOfCells; i++) {
						
						file.seek((page-1)*pSize+12+i*2);
						short offset1 = file.readShort();
						long orig1 = (page-1)*pSize;
						long loc = orig1 + offset1;
						String[] vals = get_values(file, loc);
						int rowid = Integer.parseInt(vals[0]);
						for (int j = 0; j < type.length; j++)
							if (type[j].equals("DATE") || type[j].equals("DATETIME"))
								vals[j] = "'" + vals[j] + "'";
						boolean check = compare(vals, rowid, cmp, columnName);
						for (int j = 0; j < type.length; j++)
							if (type[j].equals("DATE") || type[j].equals("DATETIME"))
								vals[j] = vals[j].substring(1, vals[j].length() - 1);
						if (check)
							tcon.add(rowid, vals);
					}
				} else
					continue;
			}
			tcon.col_name = columnName;
			tcon.format = new int[columnName.length];
		} catch (Exception e) {
			System.out.println("Error at filter");
			e.printStackTrace();
		}
	}
	public static String[] getColN(String t){
		String[] col = new String[0];
		try{
			RandomAccessFile fi = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			TableContent tcon1 = new TableContent();
			String[] col_name = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",t};
			filter(fi, cmp, col_name, tcon1);
			HashMap<Integer, String[]> content = tcon1.con;
			ArrayList<String> arr = new ArrayList<String>();
			for(String[] i : content.values()){
				arr.add(i[2]);
			}
			int size=arr.size();
			col = arr.toArray(new String[size]);
			fi.close();
			return col;
		}catch(Exception e){
			System.out.println(e);
		}
		return col;
	}
	public static void drop(String table){
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numOfPages = (int)(file.length()/(new Long(pSize)));
			for(int page = 1; page <= numOfPages; page ++){
				file.seek((page-1)*pSize);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] cellsAddr = Tree.getCellArray(file, page);
					int k = 0;
					for(int i = 0; i < cellsAddr.length; i++)
					{
						file.seek((page-1)*pSize+12+i*2);
						short offset = file.readShort();
						long orig = (page-1)*pSize;
						long loc = orig + offset;
						String[] vals = get_values(file, loc);
						String tb = vals[1];
						if(!tb.equals(table))
						{
							file.seek((page-1)*pSize+12+k*2);
							file.writeShort(cellsAddr[i]);
							k++;
						}
					}
					file.seek((page-1)*pSize+1);
					file.writeByte((byte)k);
				}
				else
					continue;
			}
			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			numOfPages = (int)(file.length()/(new Long(pSize)));
			for(int page = 1; page <= numOfPages; page ++){
				file.seek((page-1)*pSize);
				byte fileType = file.readByte();
				if(fileType == 0x0D)
				{
					short[] cellsAddr = Tree.getCellArray(file, page);
					int k = 0;
					for(int i = 0; i < cellsAddr.length; i++)
					{
						file.seek((page-1)*pSize+12+i*2);
						short offset3 = file.readShort();
						long orig3 = (page-1)*pSize;
						long loc = orig3 + offset3;
						String[] vals = get_values(file, loc);
						String tb = vals[1];
						if(!tb.equals(table))
						{
							file.seek((page-1)*pSize+12+k*2);
							file.writeShort(cellsAddr[i]);
							k++;
						}
					}
					file.seek((page-1)*pSize+1);
					file.writeByte((byte)k);
				}
				else
					continue;
			}
			File anOldFile = new File("data", table+".tbl"); 
			anOldFile.delete();
		}catch(Exception e){
			System.out.println(e);
		}

	}
}
