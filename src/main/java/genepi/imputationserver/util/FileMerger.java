package genepi.imputationserver.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import genepi.hadoop.HdfsUtil;
import genepi.io.text.LineReader;
import htsjdk.samtools.util.BlockCompressedOutputStream;

public class FileMerger {

	public static final String R2_FLAG = "R2";

	public static void splitIntoHeaderAndData(String input, OutputStream outHeader, OutputStream outData, double rsq)
			throws IOException {
		LineReader reader = new LineReader(input);
		while (reader.next()) {
			String line = reader.get();
			if (!line.startsWith("#")) {
				if (rsq > 0) {
					// rsq set. parse line and check rsq
					String info = parseInfo(line);
					if (info != null) {
						boolean keep = keepVcfLineByInfo(info, R2_FLAG, rsq);
						if (keep) {
							outData.write(line.getBytes());
							outData.write("\n".getBytes());
						}
					} else {
						// no valid vcf line. keep line
						outData.write(line.getBytes());
						outData.write("\n".getBytes());
					}
				} else {
					// no rsq set. keep all lines without parsing
					outData.write(line.getBytes());
					outData.write("\n".getBytes());
				}
			} else {
				// remove minimac4 command
				if (!line.startsWith("##minimac4_Command")) {
					outHeader.write(line.getBytes());
					outHeader.write("\n".getBytes());
				}
			}
		}
		outHeader.write(("##imputationserver_filter R2>" + rsq + "\n").getBytes());
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

	public static boolean keepVcfLineByInfo(String info, String field, double value) {
		String[] tilesInfo = info.split(";");
		for (String tile : tilesInfo) {
			String[] tilesRsq = tile.split("=");
			if (tilesRsq.length == 2) {
				String id = tilesRsq[0];
				String stringValue = tilesRsq[1];
				if (id.equals(field)) {
					double doubleValue = Double.parseDouble(stringValue);
					return (doubleValue > value);
				}
			}
		}
		return false;
	}

	public static String parseInfo(String line) {
		// rsq set. parse line and check rsq in info
		String[] tiles = line.split("\t", 9);
		if (tiles.length == 9) {
			String info = tiles[7];
			return info;
		} else {
			return null;
		}
	}

}
