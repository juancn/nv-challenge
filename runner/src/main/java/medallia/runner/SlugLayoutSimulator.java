package medallia.runner;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import express.web.test.DumpSlugCompleteLayout;
import express.web.test.DumpSlugCompleteLayout.CompanyLayout;
import express.web.test.DumpSlugCompleteLayout.DatasetLayout;
import express.web.test.DumpSlugCompleteLayout.Field;
import express.web.test.DumpSlugCompleteLayout.Layout;
import ibs.test.SimulatorUtil.RecordPacker;
import tiny.Empty;
import tiny.Format;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import static common.Common.unused;

/**
 * Simulator Framework for alternative slug record loading strategies.
 * <p>
 * The purpose of this is to experiment with alternative record/segment/column
 * layouts, in order to maximize the efficiency of shared null vectors.
 * <p/>
 * A simulator is composed of three classes:
 * <ul>
 *     <li>A {@link FieldLayoutSimulator} (optional) that returns a new field-column mapping</li>
 *     <li>A {@link RecordLayoutSimulator} that packs records into segments</li>
 *     <li>A {@link SimulatorFactory} that creates field and record layout simulators for a run on each dataset</li>
 * </ul>
 * For an example, check {@link SillySim} for a simple simulator or {@link CorrelationSim} for one a bit more sophisticated.
 */
public class SlugLayoutSimulator {

	/** Basic interface for classes that need to process records */
	public interface RecordProcessor {
		/**
		 * Process a single record.
		 * @param layoutIdx Layout index in 'layouts' this record represents.
		 */
		void processRecord(int layoutIdx);
	}


	/**
	 * Base class for a simulator. All Simulators should extend
	 * {@link RecordLayoutSimulator} or {@link FieldLayoutSimulator}.
	 * <p>
	 * This simulator uses the term layout id. A layout id is a placeholder for
	 * a unique record layout, and the specific layout can be looked up in the
	 * {@link #layouts} member. Layout #0 is special, and is used to represent
	 * an empty row.
	 * <p>
	 * The flow of simulation is as follows:
	 * <ul>
	 * <li>Simulator is initialized with a given list of fields and layouts.
	 * <li>{@link #processRecord(int)} is called once per record.
	 * <li>{@link #flush()} may be called multiple times during loading, and is
	 * guaranteed to be called at the end.
	 * <li>After all loading is done, {@link #getFields()} is called to
	 * determine any new field ordering needed, and {@link #getSegments()} is
	 * called to fetch the new segments.
	 * </ul>
	 * <p>
	 * As an example, assume we have two fields, A and B, and that we have three
	 * records R1, R2 and R3.
	 * 
	 * <pre>
	 * R1: A="Hello", B=3
	 * R2: A="World", B=4
	 * R3: B=5
	 * </pre>
	 * 
	 * The simulator code doesn't have (or care) what the values actually are,
	 * it only cares if it they are set, which indicates it has to reserve space
	 * to store them. In this case, we would end up with 2 fields (A and B), and
	 * 2 layouts. Layout 1 would be (A set, B set) and layout 2 would be (B
	 * set).
	 * <p>
	 * The flow of execution is therefore likely to be:
	 * 
	 * <pre>
	 * Constructor()
	 * processRecord(1);
	 * processRecord(1);
	 * processRecord(2);
	 * flush();
	 * getFields();
	 * getSegments();
	 * </pre>
	 * 
	 * @see DumpSlugCompleteLayout
	 */
	protected abstract static class SimulatorBase implements RecordProcessor {
		/**
		 * The mapping between a layout id and which fields contain values.
		 */
		protected final BitSet[] layouts;

		/**
		 * Field definitions on input
		 */
		protected final List<Field> fields;

		/**
		 * Initialize constructor with given layout and fields.
		 */
		public SimulatorBase(BitSet[] layouts, List<Field> fields) {
			this.layouts = layouts;
			this.fields = fields;
		}

		/**
		 * Load a single record into the dataset
		 * 
		 * @param layoutIdx Layout index in {@link #layouts} this record
		 *            represents.
		 */
		@Override
		public abstract void processRecord(int layoutIdx);

	}
	
	/**
	 * Base class for simulators. The meat of the logic is in
	 * {@link SimulatorBase}.
	 */
	public abstract static class RecordLayoutSimulator extends SimulatorBase {
		/**
		 * Initialize simulator with given layout and fields.
		 */
		public RecordLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			super(layouts, fields);
		}

		/**
		 * @return Completed layout of segments
		 */
		protected abstract List<int[]> getSegments();

		/**
		 * Flush layout. This is used to signify a publishing point during
		 * loading (all currently {@link #processRecord(int)} records must be
		 * visible in a call to {@link #getSegments()}) and at the end.
		 */
		public void flush() {
			// Optional
		}
	}

	/**
	 * Base class for field layout simulators. These can be used to pre-analyze the dataset,
	 * but their primary purpose is to change column allocation for fields.
	 * Field layout simulators. typically only see a subset of the actual data.
	 */
	public abstract static class FieldLayoutSimulator extends SimulatorBase {
		/**
		 * Initialize the simulator with given layout and fields.
		 */
		public FieldLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			super(layouts, fields);
		}

		/**
		 * This can be overridden to change the column of field definitions. Be
		 * aware that the order of fields should never change, only the column
		 * they are allocated in.
		 */
		protected List<Field> getFields() {
			return fields;
		}
	}

	/**
	 * Creation of field and record layout simulators.
	 */
	public interface SimulatorFactory {
		/**
		 * Create a {@link FieldLayoutSimulator}. If this returns non-null, the
		 * simulator will be given a subset of records to calculate optimal column
		 * allocation for the fields.
		 */
		FieldLayoutSimulator createFieldLayoutSimulator(BitSet[] layouts, List<Field> fields);

		/**
		 * Crate a simulator for the given layout and fields. If {@link #createFieldLayoutSimulator}
		 * returned non-null, the field layout built by the {@link FieldLayoutSimulator}
		 * will be given here.
		 */
		RecordLayoutSimulator createRecordLayoutSimulator(BitSet[] layouts, List<Field> fields);

		/** @return the name of this simulator (used for reporting) */
		String getName();
	}


	/** Just pack segments as they come in the order they come */
	public static class SimpleRecordLayoutSimulator extends RecordLayoutSimulator {
		private final RecordPacker packer = new RecordPacker(SEGMENT_SIZE);

		/**
		 * Initialize simulator with given layout and fields.
		 */
		public SimpleRecordLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			super(layouts, fields);
		}

		@Override
		protected List<int[]> getSegments() {
			return packer.getSegments();
		}

		@Override
		public void flush() {
			packer.flush();
		}

		@Override
		public void processRecord(int layoutIdx) {
			packer.processRecord(layoutIdx);
		}
	}

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
					totalRows, totalLayouts, fields, columns, segments, usedBitsPercent, usedColumnsPercent, Format.toSi(usedBytes));
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
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		final Iterable<Path> paths = (args.length == 0) ? Collections.singleton(DumpSlugCompleteLayout.DUMP_PATH) : Iterables.transform(Arrays.asList(args), new Function<String, Path>() {			@Override
			public Path apply(String fileName) {
				return Paths.get(fileName);
			}
		});

		final List<SimulatorFactory> sims = ImmutableList.of(SillySim.FACTORY, DoNothingSim.FACTORY, CorrelationSim.WAIT_AND_FIX_FACTORY, CorrelationSim.SPLIT_FACTORY );

		// Collect total bytes used by each simulator to report an overall winner
		final Map<SimulatorFactory, Long> totalUsedBytes = Empty.hashMap();
		for (SimulatorFactory sim : sims) {
			totalUsedBytes.put(sim, 0L);
		}

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

					long bytesUsed = Long.MAX_VALUE;
					String best = null;
					for (SimulatorFactory fac : sims) {
						final String simName = fac.getName();
						final Analysis analysis = simulateCompany(fac, stats);

						System.out.printf("%s:%s:%s %s\n", company.getKey(), dataset.getKey(), simName, analysis);

						if ( bytesUsed > analysis.usedBytes ) {
							bytesUsed = analysis.usedBytes;
							best = simName;
						}

						// Update total used bytes
						totalUsedBytes.put(fac, totalUsedBytes.get(fac) + analysis.usedBytes);
					}
					System.out.printf(" * * * Round Winner: %s * * * %n", best);
				}
			}
		}

		// Find which algorithm used the least amount of memory overall
		long bytesUsed = Long.MAX_VALUE;
		String winner = null;
		for (Entry<SimulatorFactory, Long> entry : totalUsedBytes.entrySet()) {
			final String name = entry.getKey().getName();
			final long usedBytes = entry.getValue();
			if ( bytesUsed > usedBytes) {
				bytesUsed = usedBytes;
				winner = name;
			}
			System.out.printf("** %s - total bytes used: %s%n", name, Format.toSi(usedBytes));
		}
		System.out.printf(" * * * And the winner is: %s * * * %n", winner);
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
