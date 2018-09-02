package databaseLite;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.Date;

public class Tree {
	
	static int pSize=512;
	
	public static int[] keys(RandomAccessFile file, int page) throws IOException{
		file.seek((page-1)*pSize+1);
		byte val = file.readByte();
		int num = new Integer(val);
		int[] arr = new int[num];
		try{
			file.seek((page-1)*pSize);
			byte p_type = file.readByte();
			byte offset = 0;
			switch(p_type){
			    case 0x0d:
				    offset = 2;
				    break;
				case 0x05:
					offset = 4;
					break;
				default:
					offset = 2;
					break;
			}
			for(int i = 0; i < num; i++){
				file.seek((page-1)*pSize+12+i*2);
				short off = file.readShort();
				long original = (page-1)*pSize;
				long location = original + off;
				file.seek(location+offset);
				arr[i] = file.readInt();
			}
		}
		catch(Exception e){
			System.out.println(e);
		}
		return arr;
	}

	public static int c_Leaf_s(RandomAccessFile f, int pg, int size){
		int val = -1;
		try{
			f.seek((pg-1)*pSize+2);
			int cont = f.readShort();
			if(cont == 0)
				return pSize - size;
			f.seek((pg-1)*pSize+1);
			int n_cell = f.readByte();
			int sp = cont - 20 - 2*n_cell;
			if(size < sp)
				return cont - size;
		}
		catch(Exception e){
			System.out.println(e);
		}
		return val;
	}
	public static void insertLeaf(RandomAccessFile f, int pg, int off, short p_size, int k, byte[] sc, String[] val){
		try{
			String str;
			f.seek((pg-1)*pSize+off);
			f.writeShort(p_size);
			f.writeInt(k);
			int column = val.length - 1;
			f.writeByte(column);
			f.write(sc);
			for(int i = 1; i < val.length; i++){
				switch(sc[i-1]){
					case 0x00:
						f.writeByte(0);
						break;
					case 0x01:
						f.writeShort(0);
						break;
					case 0x02:
						f.writeInt(0);
						break;
					case 0x03:
						f.writeLong(0);
						break;
					case 0x04:
						f.writeByte(new Byte(val[i]));
						break;
					case 0x05:
						f.writeShort(new Short(val[i]));
						break;
					case 0x06:
						f.writeInt(new Integer(val[i]));
						break;
					case 0x07:
						f.writeLong(new Long(val[i]));
						break;
					case 0x08:
						f.writeFloat(new Float(val[i]));
						break;
					case 0x09:
						f.writeDouble(new Double(val[i]));
						break;
					case 0x0A:
						str = val[i];
						Date tem = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str.substring(1, str.length()-1));
						long tim = tem.getTime();
						f.writeLong(tim);
						break;
					case 0x0B:
						str = val[i];
						str = str.substring(1, str.length()-1);
						str = str+"_00:00:00";
						Date tem2 = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str);
						long tim2 = tem2.getTime();
						f.writeLong(tim2);
						break;
					default:
						f.writeBytes(val[i]);
						break;
				}
			}
			f.seek((pg-1)*pSize+1);
			int n = f.readByte();
			byte tmp = (byte) (n+1);
			f.seek((pg-1)*pSize+1);
			f.writeByte(tmp);
			f.seek((pg-1)*pSize+12+n*2);
			f.writeShort(off);
			f.seek((pg-1)*pSize+2);
			int content = f.readShort();
			if(content >= off || content == 0){
				f.seek((pg-1)*pSize+2);
				f.writeShort(off);
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}
	public static void splitL(RandomAccessFile f, int pg) throws IOException{
		int new_pg = (int)(f.length()/(new Long(pSize)));
		new_pg = new_pg + 1;
		f.setLength(pSize * new_pg);
		f.seek((new_pg-1)*pSize);
		f.writeByte(0x0D);
		
		int mid_key = f_midK(f, pg);
		splitL_Page(f, pg, new_pg);		
		f.seek((pg-1)*pSize+8);
		int par = f.readInt();
		if(par == 0){
			int root_pg = makeInteriorPage(f);
			f.seek((pg-1)*pSize+8);
			f.writeInt(root_pg);
			f.seek((new_pg-1)*pSize+8);
			f.writeInt(root_pg);
			f.seek((root_pg-1)*pSize+4);
			f.writeInt(new_pg);
			
			insert_interiorC(f, root_pg, pg, mid_key);
		}else{
			long p_loc = getPointerLoc(f, pg, par);
			if(p_loc == 0){
				f.seek((par-1)*pSize+4);
			}else{
				f.seek(p_loc);
			}
			f.writeInt(new_pg);
			insert_interiorC(f, par, pg, mid_key);
			sortCellArray(f, par);
			while(checkInteriorSpace(f, par)){
				par = splitInterior(f, par);
			}
		}
	}
	public static int f_midK(RandomAccessFile f, int pg){
		int val = 0;
		try{
			f.seek((pg-1)*pSize);
			byte p_type = f.readByte();
			f.seek((pg-1)*pSize+1);
			int no_cells = f.readByte();
			int m = (int) Math.ceil((double) no_cells / 2);
			
			f.seek((pg-1)*pSize+12+(m-1)*2);
			short offset = f.readShort();
			long orig = (pg-1)*pSize;
			long loc = orig + offset;
			f.seek(loc);
			switch(p_type){
				case 0x05:
					f.readInt(); 
					val = f.readInt();
					break;
				case 0x0D:
					f.readShort();
					val = f.readInt();
					break;
			}

		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}
	public static short[] getCellArr(RandomAccessFile f, int pg) throws IOException{
		f.seek((pg-1)*pSize+1);
		byte val1 = f.readByte();
		int num = new Integer(val1);
		short[] arr = new short[num];
		try{
			f.seek((pg-1)*pSize+12);
			for(int i = 0; i < num; i++){
				arr[i] = f.readShort();
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return arr;
	}
	public static void splitL_Page(RandomAccessFile f, int c_page, int n_page){
		try{
			f.seek((c_page-1)*pSize+1);
			int numCells = f.readByte();			
			int mid = (int) Math.ceil((double) numCells / 2);

			int numCellA = mid - 1;
			int numCellB = numCells - numCellA;
			int content = 512;

			for(int i = numCellA; i < numCells; i++){	
				f.seek((c_page-1)*pSize+12+i*2);
				short offset1 = f.readShort();
				long orig1 = (c_page-1)*pSize;
				long loc = orig1 + offset1;
				
				f.seek(loc);
				int cellSize = f.readShort()+6;
				content = content - cellSize;
				f.seek(loc);
				byte[] cell = new byte[cellSize];
				f.read(cell);
				f.seek((n_page-1)*pSize+content);
				f.write(cell);				
				f.seek((n_page-1)*pSize+12+(i - numCellA)*2);
				f.writeShort(content);
			}
			f.seek((n_page-1)*pSize+2);
			f.writeShort(content);
			f.seek((c_page-1)*pSize+12+(numCellA-1)*2);
			short offset = f.readShort();
			f.seek((c_page-1)*pSize+2);
			f.writeShort(offset);

			f.seek((c_page-1)*pSize+4);
			int rightMost = f.readInt();
			//setRightMost(RandomAccessFile file, int page, int rightLeaf)
			f.seek((n_page-1)*pSize+4);
			f.writeInt(rightMost);
			f.seek((c_page-1)*pSize+4);
			f.writeInt(n_page);
			
			f.seek((c_page-1)*pSize+8);
			int parent = f.readInt();
			f.seek((n_page-1)*pSize+8);
			f.writeInt(parent);
			
			byte num = (byte) numCellA;			
			f.seek((c_page-1)*pSize+1);
			f.writeByte(num);
			num = (byte) numCellB;
			f.seek((n_page-1)*pSize+1);
			f.writeByte(num);
			
		}catch(Exception e){
			System.out.println(e);
			
		}
	}

	public static int makeInteriorPage(RandomAccessFile f){
		int no_page = 0;
		try{
			no_page = (int)(f.length()/(new Long(pSize)));
			no_page = no_page + 1;
			f.setLength(pSize * no_page);
			f.seek((no_page-1)*pSize);
			f.writeByte(0x05); 
		}catch(Exception e){
			System.out.println(e);
		}
		return no_page;
	}
	public static void insert_interiorC(RandomAccessFile f, int pg, int child, int k){
		try{
			f.seek((pg-1)*pSize+2);
			short cont = f.readShort();
			if(cont == 0)
				cont = 512;
			cont = (short)(cont - 8);
			f.seek((pg-1)*pSize+cont);
			f.writeInt(child);
			f.writeInt(k);
			f.seek((pg-1)*pSize+2);
			f.writeShort(cont);
			f.seek((pg-1)*pSize+1);
			byte num = f.readByte();
			f.seek((pg-1)*pSize+12+num*2);
			f.writeShort(cont);
			num = (byte) (num + 1);			
			f.seek((pg-1)*pSize+1);
			f.writeByte(num);

		}catch(Exception e){
			System.out.println(e);
		}
	}
	public static long getPointerLoc(RandomAccessFile f, int pg, int par){
		long val = 0;
		try{
			byte val1 = 0;
			f.seek((pg-1)*pSize+1);
			val1 = f.readByte();
			int numCells = new Integer(val1);
			for(int i=0; i < numCells; i++){
				f.seek((par-1)*pSize+12+i*2);
				short offset = f.readShort();
				long orig = (par-1)*pSize;
				long loc = orig + offset;
				f.seek(loc);
				int child_pg = f.readInt();
				if(child_pg == pg){
					val = loc;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
		return val;
	}
	public static void sortCellArray(RandomAccessFile file, int page) throws IOException{
		 file.seek((page-1)*pSize+1);
		 byte num  = file.readByte();
		 int[] keyArray = keys(file, page);
		 short[] cellArray = getCellArray(file, page);
		 int ltmp;
		 short rtmp;

		 for (int i = 1; i < num; i++) {
           for(int j = i ; j > 0 ; j--){
               if(keyArray[j] < keyArray[j-1]){

                   ltmp = keyArray[j];
                   keyArray[j] = keyArray[j-1];
                   keyArray[j-1] = ltmp;

                   rtmp = cellArray[j];
                   cellArray[j] = cellArray[j-1];
                   cellArray[j-1] = rtmp;
               }
           }
        }

        try{
        	file.seek((page-1)*pSize+12);
        	for(int i = 0; i < num; i++){
				file.writeShort(cellArray[i]);
			}
        }catch(Exception e){
        	System.out.println("Error at sortCellArray");
        }
	}
	public static short[] getCellArray(RandomAccessFile f, int pg) throws IOException{
		f.seek((pg-1)*pSize+1);
		byte val1 = f.readByte();
		int number = new Integer(val1);
		short[] arr = new short[number];
		try{
			f.seek((pg-1)*pSize+12);
			for(int i = 0; i < number; i++){
				arr[i] = f.readShort();
			}
		}catch(Exception e){
			System.out.println(e);
		}
		return arr;
	}
	public static boolean checkInteriorSpace(RandomAccessFile f, int pg) throws IOException{
		f.seek((pg-1)*pSize+1);
		byte no_cell = f.readByte();
		if(no_cell > 30)
			return true;
		else
			return false;
	}
	public static int splitInterior(RandomAccessFile file, int page) throws IOException{
		int newPage = makeInteriorPage(file);
		int midKey = f_midK(file, page);
		split_interiorP(file, page, newPage);
		file.seek((page-1)*pSize+8);
		int parent = file.readInt();
		if(parent == 0){
			int rootPage = makeInteriorPage(file);
			file.seek((page-1)*pSize+8);
			file.writeInt(rootPage);
			file.seek((newPage-1)*pSize+8);
			file.writeInt(rootPage);
			file.seek((rootPage-1)*pSize+4);
			file.writeInt(newPage);
			insert_interiorC(file, rootPage, page, midKey);
			return rootPage;
		}else{
			long ploc = getPointerLoc(file, page, parent);
			try{
				if(ploc == 0){
					file.seek((parent-1)*pSize+4);
				}else{
					file.seek(ploc);
				}
				file.writeInt(newPage);
			}catch(Exception e){
				System.out.println(e);
			}
			insert_interiorC(file, parent, page, midKey);
			sortCellArray(file, parent);
			return parent;
		}
	}
	public static void split_interiorP(RandomAccessFile f, int cur_pg, int new_pg){
		try{
			f.seek((cur_pg-1)*pSize+1);
			int no_cell = f.readByte();			
			int mid = (int) Math.ceil((double) no_cell / 2);
			int noCellA = mid - 1;
			int noCellB = no_cell - noCellA - 1;
			short content = 512;
			for(int i = noCellA+1; i < no_cell; i++){
				f.seek((cur_pg-1)*pSize+12+i*2);
				short offset1 = f.readShort();
				long orig1 = (cur_pg-1)*pSize;
				long loc = orig1 + offset1;
				short cellSize = 8;
				content = (short)(content - cellSize);
				f.seek(loc);
				byte[] cell = new byte[cellSize];
				f.read(cell);
				f.seek((new_pg-1)*pSize+content);
				f.write(cell);
				f.seek(loc);
				int page = f.readInt();
				f.seek((page-1)*pSize+8);
				f.writeInt(new_pg);;				
				f.seek((new_pg-1)*pSize+12+(i - (noCellA + 1))*2);
				f.writeShort(content);
			}
			f.seek((cur_pg-1)*pSize+4);
			int tmp = f.readInt();
			f.seek((new_pg-1)*pSize+4);
			f.writeInt(tmp);
			
			
			f.seek((cur_pg-1)*pSize+12+(mid - 1)*2);
			short offset2 = f.readShort();
			long orig2 = (cur_pg-1)*pSize;
			long midLoc = orig2 + offset2;
			f.seek(midLoc);
			tmp = f.readInt();
			f.seek((cur_pg-1)*pSize+4);
			f.writeInt(tmp);
			
			f.seek((new_pg-1)*pSize+2);
			f.writeShort(content);
			f.seek((cur_pg-1)*pSize+12+(noCellA-1)*2);
			short offset = f.readShort();			
			f.seek((cur_pg-1)*pSize+2);
			f.writeShort(offset);

			f.seek((cur_pg-1)*pSize+8);
			int parent = f.readInt();
			f.seek((new_pg-1)*pSize+8);
			f.writeInt(parent);
			
			byte num = (byte) noCellA;
			f.seek((cur_pg-1)*pSize+1);
			f.writeByte(num);
			num = (byte) noCellB;
			f.seek((new_pg-1)*pSize+1);
			f.writeByte(num);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	public static void update_LCell(RandomAccessFile f, int pg, int off1, int p_size, int k, byte[] sc, String[] v){
		try{
			String str;
			f.seek((pg-1)*pSize+off1);
			f.writeShort(p_size);
			f.writeInt(k);
			int col = v.length - 1;
			f.writeByte(col);
			f.write(sc);
			for(int i = 1; i < v.length; i++){
				switch(sc[i-1]){
					case 0x00:
						f.writeByte(0);
						break;
					case 0x01:
						f.writeShort(0);
						break;
					case 0x02:
						f.writeInt(0);
						break;
					case 0x03:
						f.writeLong(0);
						break;
					case 0x04:
						f.writeByte(new Byte(v[i]));
						break;
					case 0x05:
						f.writeShort(new Short(v[i]));
						break;
					case 0x06:
						f.writeInt(new Integer(v[i]));
						break;
					case 0x07:
						f.writeLong(new Long(v[i]));
						break;
					case 0x08:
						f.writeFloat(new Float(v[i]));
						break;
					case 0x09:
						f.writeDouble(new Double(v[i]));
						break;
					case 0x0A:
						str = v[i];
						Date tem = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str.substring(1, str.length()-1));
						long tim = tem.getTime();
						f.writeLong(tim);
						break;
					case 0x0B:
						str = v[i];
						str = str.substring(1, str.length()-1);
						str = str+"_00:00:00";
						Date tem2 = new SimpleDateFormat("yyyy-MM-dd_HH:mm:ss").parse(str);
						long tim2 = tem2.getTime();
						f.writeLong(tim2);
						break;
					default:
						f.writeBytes(v[i]);
						break;
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
	}
}
