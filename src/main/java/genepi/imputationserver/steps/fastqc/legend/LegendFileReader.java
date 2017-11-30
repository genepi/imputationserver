package genepi.imputationserver.steps.fastqc.legend;

import genepi.io.FileUtil;
import genepi.io.text.AbstractLineReader;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class LegendFileReader extends AbstractLineReader<String> {

	private Map<Integer, Integer> index = new HashMap<Integer, Integer>();

	private Set<Integer> duplicates = new HashSet<Integer>();

	private String line;

	private int oldOffset = 0;

	private BufferedReader myIn;

	private LegendEntry entry = new LegendEntry();

	private String population;

	private int idCol = -1;
	private int posCol = -1;
	private int a0Col = -1;
	private int a1Col = -1;
	private int popCol = -1;

	private int oldPosition = 0;

	private String oldLine = null;

	public LegendFileReader(DataInputStream inputStream) throws IOException {
		super(inputStream);
	}

	public LegendFileReader(String filename, String population) throws IOException {
		super(filename);
		this.population = population;
	}

	public void createIndex() throws IOException {

		int offset = 0;

		int oldPosition = -1;

		while (next()) {
			String line = get();
			if (!line.startsWith("id")) {
				String[] tiles = line.split(" ", 3);
				int position = Integer.parseInt(tiles[posCol]);

				// store duplicates
				if (index.containsKey(position)) {
					duplicates.add(position);
				}

				index.put(position, offset);

				if (oldPosition > position) {
					throw new IOException("Legend File is not sorted.");
				}

				oldPosition = position;
			} else {

				// parse header
				String[] tiles = line.split(" ");
				int i = 0;
				for (String tile : tiles) {
					if (tile.equals("id")) {
						idCol = i;
					}
					if (tile.equals("position")) {
						posCol = i;
					}
					if (tile.equals("a0")) {
						a0Col = i;
					}
					if (tile.equals("a1")) {
						a1Col = i;
					}
					if (tile.equals(population + ".aaf")) {
						popCol = i;
					}

					i++;
				}

			}
			offset += line.length() + 1;
		}
		close();
	}

	@Override
	public String get() {
		return line;
	}

	public void initSearch() throws IOException {
		FileInputStream inputStream = new FileInputStream(getFilename());
		InputStream in2 = FileUtil.decompressStream(inputStream);
		myIn = new BufferedReader(new InputStreamReader(in2));
	}

	public LegendEntry findByPosition(int position) throws IOException {

		String line = findLineByPosition(position);

		if (line != null) {

			String[] tiles = line.split(" ");

			entry.setRsId(tiles[idCol]);
			entry.setAlleleA(tiles[a0Col].charAt(0));
			entry.setAlleleB(tiles[a1Col].charAt(0));
			entry.setType("-");

			float aaf = 0;

			if (popCol != -1) {
				if (!tiles[popCol].equals(".")) {
					aaf = Float.parseFloat(tiles[popCol]);
					entry.setFrequencies(true);
				} else {
					entry.setFrequencies(false);
				}
			} else {
				entry.setFrequencies(false);
			}

			entry.setFrequencyA(1 - aaf);
			entry.setFrequencyB(aaf);

			return entry;

		} else {

			return null;

		}
	}

	public String findLineByPosition(int position) throws IOException {
		Integer offset = index.get(position);
		if (offset != null) {
			if (oldPosition != position) {
				try {
					myIn.skip(offset - oldOffset);
					String line = myIn.readLine();
					oldOffset = offset + line.length() + 1;
					oldLine = line;
					oldPosition = position;
					return line;
				} catch (Exception e) {
					throw new IOException("Problem reading position '" + position + "' [Old position: " + oldPosition
							+ ", OldOffset" + oldOffset + ", New Offset" + offset + "]");
				}
			}
			return oldLine;

		} else {

			return null;

		}
	}

	@Override
	public Iterator<String> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void parseLine(String arg0) throws Exception {
		line = arg0;
	}

}
