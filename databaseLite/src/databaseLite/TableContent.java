package databaseLite;
import java.util.HashMap;

class TableContent{
	
	public int nRow; 
	public HashMap<Integer, String[]> con;
	public String[] col_name; 
	public int[] format; 
	public TableContent(){
		nRow = 0;
		con = new HashMap<Integer, String[]>();
	}
	public void add(int rowid, String[] val){
		con.put(rowid, val);
		nRow = nRow + 1;
	}
	public void updateFormat(){
		for(int i = 0; i < format.length; i++)
			format[i] = col_name[i].length();
		for(String[] i : con.values())
			for(int j = 0; j < i.length; j++)
				if(format[j] < i[j].length())
					format[j] = i[j].length();
	}
	public String fix(int len, String s) {
		return String.format("%-"+(len+3)+"s", s);
	}
	public void display(String[] col){
		if(nRow == 0){
			System.out.println("Empty set.");
		}
		else{
			updateFormat();
			if(col[0].equals("*")){
				for(int l: format)
					System.out.print(Main.line("-", l+3));
				
				System.out.println();
				for(int i = 0; i< col_name.length; i++)
					System.out.print(fix(format[i], col_name[i])+"|");
				System.out.println();
				for(int l: format)
					System.out.print(Main.line("-", l+3));
				System.out.println();
				for(String[] i : con.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format[j], i[j])+"|");
					System.out.println();
				}
			}
			else{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < col_name.length; i++)
						if(col[j].equals(col_name[i]))
							control[j] = i;

				for(int j = 0; j < control.length; j++)
					System.out.print(Main.line("-", format[control[j]]+3));
				
				System.out.println();
				for(int j = 0; j < control.length; j++)
					System.out.print(fix(format[control[j]], col_name[control[j]])+"|");
				
				System.out.println();
				for(int j = 0; j < control.length; j++)
					System.out.print(Main.line("-", format[control[j]]+3));
				
				System.out.println();
				for(String[] i : con.values()){
					for(int j = 0; j < control.length; j++)
						System.out.print(fix(format[control[j]], i[control[j]])+"|");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}