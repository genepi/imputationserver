package genepi.imputationserver.util;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Vector;

import genepi.io.text.GzipLineWriter;
import genepi.io.text.LineReader;
import htsjdk.samtools.liftover.LiftOver;
import htsjdk.samtools.util.Interval;

public class LegendFileLiftOver {

	public static class LegendFileLiftOverResults{
		
		public Vector<String> errors;
		
		public int snpsWritten;
		
	}
	
	public static LegendFileLiftOverResults liftOver(String input, String output, String chainFile, String tempDir,
			String chromosome) throws IOException {

		LineReader reader = new LineReader(input);
		GzipLineWriter writer = new GzipLineWriter(output);

		LiftOver liftOver = new LiftOver(new File(chainFile));

		Vector<String> errors = new Vector<String>();
		Vector<String> buffer = new Vector<String>();

		// read file and perform lift over
		while (reader.next()) {
			String line = reader.get();
			if (line.startsWith("id")) {
				writer.write(line);
			} else {

				String[] tiles = line.split(" ", 3);
				Interval source = new Interval("chr" + chromosome, Integer.parseInt(tiles[1]),
						Integer.parseInt(tiles[1]) + 1, false, chromosome + ":" + tiles[1]);
				Interval target = liftOver.liftOver(source);
				if (target != null) {
					if (source.getContig().equals(target.getContig())) {
						buffer.add(tiles[0] + " " + target.getStart() + " " + tiles[2]);
					} else {
						errors.add(target.getContig() + ":" + tiles[1] + "\t" + "LiftOver" + "\t"
								+ "On different chromosome after LiftOver. SNP removed.");
					}
				} else {
					errors.add(tiles[0] + ":" + tiles[1] + "\t" + "LiftOver" + "\t" + "LiftOver failed. SNP removed.");
				}
			}

		}

		// sort legend file by position
		Collections.sort(buffer, new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				String[] tiles1 = o1.split(" ", 3);
				String[] tiles2 = o2.split(" ", 3);
				int position1 = Integer.parseInt(tiles1[1]);
				int position2 = Integer.parseInt(tiles2[1]);
				return Integer.compare(position1, position2);
			}
		});

		// write file
		for (String line : buffer) {
			writer.write(line);
		}

		reader.close();
		writer.close();

		LegendFileLiftOverResults result = new LegendFileLiftOverResults();
		result.errors = errors;
		result.snpsWritten = buffer.size();
		
		return result;
	}

}
