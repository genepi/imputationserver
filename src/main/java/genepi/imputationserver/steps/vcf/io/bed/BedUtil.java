package genepi.minicloudmac.hadoop.validation.io.bed;

import java.io.File;
import java.io.IOException;

import genepi.io.FileUtil;
import genepi.io.text.LineReader;
import genepi.io.text.LineWriter;

public class BedUtil {
	
	public static boolean removeParents(String input){
		
		try {
			LineReader reader = new LineReader(input);
			LineWriter writer= new LineWriter(input+"-temp");
			
			while(reader.next()){
				String[] tiles = reader.get().split("\\s{1}(?!\\s)");
				String line = tiles[0] + " " + tiles[1] + " 0 0";
				for (int i = 4; i < tiles.length; i++){
					line += " " + tiles[i];
				}
				writer.write(line);
			}
			
			writer.close();
			reader.close();
			
			FileUtil.deleteFile(input);
			(new File(input+"-temp")).renameTo(new File(input)); 
			
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
						
		return true;
		
	}
	

}
