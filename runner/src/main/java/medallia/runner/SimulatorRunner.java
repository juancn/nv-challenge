package medallia.runner;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import medallia.sim.FieldLayoutSimulator;
import medallia.sim.RecordLayoutSimulator;
import medallia.sim.RecordProcessor;
import medallia.sim.SimulatorFactory;
import medallia.sim.data.CompanyLayout;
import medallia.sim.data.DatasetLayout;
import medallia.sim.data.Field;
import medallia.sim.data.Layout;
import medallia.util.SimulatorUtil;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import static medallia.util.SimulatorUtil.toSi;
import static medallia.util.SimulatorUtil.unused;

/**
 * Simulator Framework for alternative slug record loading strategies.
 * <p>
 * The purpose of this is to experiment with alternative record/segment/column
 * layouts, in order to maximize the efficiency of shared null vectors.
 * <p/>
 * A simulator is composed of three classes:
 * <ul>
 *     <li>A {@link medallia.sim.FieldLayoutSimulator} (optional) that returns a new field-column mapping</li>
 *     <li>A {@link medallia.sim.RecordLayoutSimulator} that packs records into segments</li>
 *     <li>A {@link medallia.sim.SimulatorFactory} that creates field and record layout simulators for a run on each dataset</li>
 * </ul>
 */
public class SimulatorRunner {


	/** Simulator analysis result */
	static class Analysis {
		private final long totalRows;
		private final int totalLayouts;
		private final int fields;
		private final int columns;
		private final int segments;
		private final double usedBitsPercent;
		private final double usedColumnsPercent;
		private final long usedBytes;

		public Analysis(long totalRows, int totalLayouts, int fields, int columns, int segments, double usedBitsPercent, double usedColumnsPercent, long usedBytes) {
			this.totalRows = totalRows;
			this.totalLayouts = totalLayouts;
			this.fields = fields;
			this.columns = columns;
			this.segments = segments;
			this.usedBitsPercent = usedBitsPercent;
			this.usedColumnsPercent = usedColumnsPercent;
			this.usedBytes = usedBytes;
		}

		@Override
		public String toString() {
			return String.format("%s rows (%s layouts), %s fields (%s columns in %s segments): %.1f%% used-data, %.1f%% used-columns, %s",
					totalRows, totalLayouts, fields, columns, segments, usedBitsPercent, usedColumnsPercent, toSi(usedBytes));
		}
	}


	/**
	 * Analyze the result of a simulator.
	 * <p>
	 * This calculates the raw data null-coverage, the amount of columns used,
	 * the amount of columns that would be shareable as null-vectors, and
	 * finally the total dataset size.
	 */
	private static Analysis analyze(List<int[]> segments, List<Field> fields, DatasetLayout stats) {
		// Non-null values in column across entire dataset
		long[] counts = new long[fields.size()];

		// Number of times layout id has been used
		long[] layoutCounts = new long[stats.layouts.length];

		int columns = SimulatorUtil.columnCount(fields);

		// Number of bits per column
		int[] bitsPerColumn = new int[columns];

		// Number of segments where this column has at least one
		// non-null value.
		int[] columnUsedInSegments = new int[columns];

		// Map from field id to column number
		int[] columnMap = new int[fields.size()];

		// Map from bit index in the layout to relocated field
		int[] bitFieldMap = new int[fields.size()];

		for (int i = 0; i < fields.size(); ++i) {
			final Field field = fields.get(i);
			columnMap[i] = field.column;
			bitsPerColumn[field.column] += field.size;
			bitFieldMap[field.getIndex()] = i;
		}

		// Check that fields are not over-allocated
		for (int i = 0; i < bitsPerColumn.length; i++) {
			Preconditions.checkArgument(bitsPerColumn[i] <= 32, "Column %s is using %s bits", i, bitsPerColumn[i]);
		}

		// Total rows in entire dataset
		long totalRows = 0;

		// Number of allocated column positions
		long allocatedValues = 0;

		for (int[] rows : segments) {
			boolean[] columnUsed = new boolean[columns];
			for (int id : rows) {
				++layoutCounts[id];
				BitSet bitSet = stats.layouts[id];
				for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i+1)) {
					++counts[bitFieldMap[i]];
					columnUsed[columnMap[bitFieldMap[i]]] = true;
				}
				++totalRows;
			}
			for (int i = 0; i < columns; ++i)
				if (columnUsed[i]) {
					++columnUsedInSegments[i];
					allocatedValues += rows.length;
				}
		}

		// Total bits that cover non-null values
		long usedBits = 0;
		// Total number of bits allocated
		long totalBits = 0;
		for (int i = 0; i < fields.size(); ++i) {
			totalBits += fields.get(i).size * totalRows;
			usedBits += fields.get(i).size * counts[i];
		}

		// Number of non-null column vectors in all segments
		long usedColumns = 0;
		// Total number of column vectors
		long totalColumns = 0;
		for (int i = 0; i < columns; ++i) {
			totalColumns += segments.size();
			usedColumns += columnUsedInSegments[i];
		}

		// Do a sanity check
		Preconditions.checkArgument(usedBits == computeUsedBits(stats), "Bit counts do not match");
		Preconditions.checkArgument(Arrays.equals(layoutCounts, computeLayoutCounts(stats)), "Layout counts do not match");

		final int perSegmentCost = 40 * columns + 4096;
		return new Analysis(
				totalRows,
				stats.layouts.length - 1,
				fields.size(),
				columns,
				segments.size(),
				(usedBits * 100.0) / totalBits,
				(usedColumns * 100.0) / totalColumns,
				allocatedValues * 4 + segments.size() * (perSegmentCost)
		);
	}

	private static long computeUsedBits(DatasetLayout stats) {
		long usedBits = 0;
		for (int[] segment : stats.segments) {
			for (int row : segment) {
				BitSet layout = stats.layouts[row];
				for (int i = layout.nextSetBit(0); i >= 0; i = layout.nextSetBit(i+1)) {
					usedBits += stats.fields[i].size;
				}
			}
		}
		return usedBits;
	}

	/**
	 * Fetch data from file and parse
	 */
	public static void main(SimulatorFactory fac, String[] args) throws IOException, ClassNotFoundException {
		final Iterable<Path> paths = Iterables.transform(Arrays.asList(args), new Function<String, Path>() {
			@Override public Path apply(String fileName) {
				return Paths.get(fileName);
			}
		});

		// Collect total bytes used
		long totalUsedBytes = 0;
		final String name = fac.getName();

		for (Path path : paths) {
			final Layout layout;

			try (InputStream in = Files.newInputStream(path); InputStream gzip = new GZIPInputStream(in); ObjectInputStream ois = new ObjectInputStream(gzip)) {
				layout = (Layout) ois.readObject();
			}

			for (Entry<String, CompanyLayout> company : layout.companies.entrySet()) {
				for (Entry<String, DatasetLayout> dataset : company.getValue().datasets.entrySet()) {
					final DatasetLayout stats = dataset.getValue();

					// Ignore empty datasets
					if (stats.segments.isEmpty())
						continue;

					final Analysis analysis = simulateCompany(fac, stats);

					System.out.printf("%s:%s:%s %s\n", company.getKey(), dataset.getKey(), name, analysis);

					// Update total used bytes
					totalUsedBytes += analysis.usedBytes;
				}
			}
		}

		System.out.printf("** %s - total bytes used: %s%n", name, toSi(totalUsedBytes));
	}

	private static long[] computeLayoutCounts(DatasetLayout stats) {
		long[] layoutCounts = new long[stats.layouts.length];
		for (int[] layouts : stats.segments)
			for (int idx : layouts)
					layoutCounts[idx]++;
		return layoutCounts;
	}

	private static Analysis simulateCompany(final SimulatorFactory fac, final DatasetLayout stats) {
		List<Field> fields = Arrays.asList(stats.fields);

		FieldLayoutSimulator fieldLayoutSimulator = fac.createFieldLayoutSimulator(stats.layouts, fields);
		if (fieldLayoutSimulator != null) {
			processRecords(stats, fieldLayoutSimulator, SURVIVAL_RATE);
			fields = fieldLayoutSimulator.getFields();
		}

		final RecordLayoutSimulator sim = fac.createRecordLayoutSimulator(stats.layouts, fields);

		// Allow GC of potentially massive object
		fieldLayoutSimulator = null;
		unused(fieldLayoutSimulator);

		processRecords(stats, sim, 1);
		sim.flush();

		return analyze(sim.getSegments(), fields, stats);
	}

	private static void processRecords(DatasetLayout stats, RecordProcessor processor, double survivalRate) {
		// Always use the same PRNG so results are comparable
		final Random prng = new Random(0);
		for (int[] layouts : stats.segments)
			for (int idx : layouts)
				if (survivalRate < 0 || survivalRate >= 1 || prng.nextDouble() < survivalRate)
					processor.processRecord(idx);
	}

	/** Size of a segment */
	public static final int SEGMENT_SIZE = 50000;

	/** Rate of records that are used for training vs total records */
	public static final double SURVIVAL_RATE = 0.1;
}
