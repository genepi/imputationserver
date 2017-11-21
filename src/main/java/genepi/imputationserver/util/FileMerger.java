package genepi.imputationserver.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import genepi.hadoop.HdfsUtil;
import genepi.io.text.LineReader;
import htsjdk.samtools.util.BlockCompressedOutputStream;

public class FileMerger {

	public static void splitIntoHeaderAndData(String input, OutputStream outHeader, OutputStream outData)
			throws IOException {
		LineReader reader = new LineReader(input);
		while (reader.next()) {
			String line = reader.get();
			if (!line.startsWith("#")) {
				outData.write(line.getBytes());
				outData.write("\n".getBytes());
			} else {
				if(!line.startsWith("##minimac4_Command")){
				outHeader.write(line.getBytes());
				outHeader.write("\n".getBytes());
				}
			}
		}
		outData.close();
		outHeader.close();
		reader.close();
	}

	public static class BgzipSplitOutputStream extends BlockCompressedOutputStream {

		public BgzipSplitOutputStream(OutputStream os) {
			super(os, null);
		}

		@Override
		public void close() throws IOException {
			flush();
		}

	}

	public static int splitIntoHeaderAndData(String input, String outputPrefix) throws IOException {
		boolean firstHeader = true;
		int snps = 0;

		int chunk = 0;
		GzipCompressorOutputStream outHeader = new GzipCompressorOutputStream(
				new FileOutputStream(outputPrefix + ".header.vcf.gz"));
		GzipCompressorOutputStream outData = new GzipCompressorOutputStream(
				new FileOutputStream(outputPrefix + "_" + chunk + ".data.vcf.gz"));

		LineReader reader = new LineReader(input);
		while (reader.next()) {
			String line = reader.get();
			if (!line.startsWith("#")) {
				outData.write("\n".getBytes());
				outData.write(line.getBytes());
				snps++;

				if (snps % 10000 == 0) {
					outData.close();
					chunk++;
					outData = new GzipCompressorOutputStream(
							new FileOutputStream(outputPrefix + "_" + chunk + ".data.vcf.gz"));

				}

			} else {
				if (!firstHeader) {
					outHeader.write("\n".getBytes());
				}
				firstHeader = false;
				outHeader.write(line.getBytes());
			}
		}
		outData.close();
		outHeader.close();

		reader.close();
		return chunk;
	}

	public static int splitIntoHeaderAndDataBgZip(String input, String outputPrefix) throws IOException {
		boolean firstHeader = true;
		int snps = 0;

		int chunk = 0;
		BgzipSplitOutputStream outHeader = new BgzipSplitOutputStream(
				new FileOutputStream(outputPrefix + ".header.vcf.gz"));
		BgzipSplitOutputStream outData = new BgzipSplitOutputStream(
				new FileOutputStream(outputPrefix + "_" + chunk + ".data.vcf.gz"));

		LineReader reader = new LineReader(input);
		while (reader.next()) {
			String line = reader.get();
			if (!line.startsWith("#")) {
				outData.write("\n".getBytes());
				outData.write(line.getBytes());
				snps++;

				if (snps % 10000 == 0) {
					outData.close();
					chunk++;
					outData = new BgzipSplitOutputStream(
							new FileOutputStream(outputPrefix + "_" + chunk + ".data.vcf.gz"));

				}

			} else {
				if (!firstHeader) {
					outHeader.write("\n".getBytes());
				}
				firstHeader = false;
				outHeader.write(line.getBytes());
			}
		}
		outData.close();
		outHeader.close();

		reader.close();
		return chunk;
	}

	public static void mergeAndGzInfo(ArrayList<String> hdfs, String local) throws IOException {

		GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(local));

		Configuration conf = HdfsUtil.getConfiguration();

		FileSystem fileSystem = FileSystem.get(conf);

		for (String folder : hdfs) {
			
			Path pathFolder = new Path(folder);
			
			FileStatus[] files = fileSystem.listStatus(pathFolder);

			boolean firstFile = true;

			for (FileStatus file : files) {

				FSDataInputStream in = fileSystem.open(file.getPath());

				LineReader reader = new LineReader(in);

				boolean header = true;
				
				while (reader.next()) {

					String line = reader.get();

					if (header) {
						if (firstFile) {
							firstFile = false;
						} else {
							out.write('\n');
						}
						header = false;
					} else {
						out.write('\n');

					}
					out.write(line.toString().getBytes());
				}
				in.close();

			}

		}

		out.close();
	}

}
