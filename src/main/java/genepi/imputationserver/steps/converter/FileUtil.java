package genepi.imputationserver.steps.converter;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public class FileUtil {
	
	public static void gunzip(String inFile, String outFile) {

		byte[] buffer = new byte[1024];

		try {

			System.out.println("Gunzip...");

			GZIPInputStream gzis = new GZIPInputStream(new FileInputStream(inFile));

			FileOutputStream out = new FileOutputStream(outFile);

			int len;
			while ((len = gzis.read(buffer)) > 0) {
				out.write(buffer, 0, len);
			}

			gzis.close();
			out.close();

			System.out.println("Done");

		} catch (IOException ex) {
			ex.printStackTrace();
		}
	}
	
	public static void unzip(String in, String out){

		System.out.println("Unzip...");
		
	    try {
	         ZipFile zipFile = new ZipFile(in);
	         zipFile.extractAll(out);
	    } catch (ZipException e) {
	        e.printStackTrace();
	    }
	}

}
