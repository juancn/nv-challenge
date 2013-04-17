package medallia.runner;
import com.google.common.collect.ImmutableList;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import medallia.runner.SimulatorUtil.LayoutStatProcessor.ColumnLayoutStats;
import medallia.runner.SlugLayoutSimulator.RecordProcessor;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.FieldPosition;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Collection of assorted utilities to assist in building {@link SlugLayoutSimulator.RecordLayoutSimulator}s and {@link SlugLayoutSimulator.FieldLayoutSimulator}s
 */
public class SimulatorUtil {

	/** @return the number of used columns in a particular field arrangement */
	public static int columnCount(Iterable<Field> fields) {
		int columns = 0;
		for (Field field : fields)
			columns = Math.max(columns, field.column);
		// Max column of 0 means we have 1 column
		++columns;
		return columns;
	}

	/** Transform used field layouts to clear column layouts */
	public static BitSet[] buildClearColumnLayouts(BitSet[] fieldLayouts, List<Field> fields) {
		final int[] bitToFieldMap = new int[fields.size()];
		for (int i = 0; i < fields.size(); i++) {
			final Field field = fields.get(i);
			bitToFieldMap[field.getIndex()] = i;
		}

		final BitSet[] columnLayouts = new BitSet[fieldLayouts.length];
		for (int i = 0; i < fieldLayouts.length; i++) {
			columnLayouts[i] = makeClearColumnLayout(fieldLayouts[i], fields, bitToFieldMap);
		}
		return columnLayouts;
	}

	/** Turns a used field layout into a clear column layout */
	private static BitSet makeClearColumnLayout(BitSet fieldLayout, List<Field> fields, int[] bitToFieldMap) {
		if (fieldLayout == null) return null;

		// Find all used columns
		final BitSet columnLayout = new BitSet();
		for (int i = fieldLayout.nextSetBit(0); i >= 0; i = fieldLayout.nextSetBit(i+1)) {
			columnLayout.set(fields.get(bitToFieldMap[i]).column);
		}

		// Flip it so it represents clear columns
		columnLayout.flip(0, columnCount(fields));
		return columnLayout;
	}

	/** @return the number of common null columns, given two {@link BitSet} with bits set for clear columns */
	public static int countCommonNullColumns(BitSet clearCols1, BitSet clearCols2) {
		final BitSet a = cloneBitSet(clearCols1);
		a.and(clearCols2);
		return a.cardinality();
	}

	/** @return how many records are used in a particular clustering */
	public static int totalRecordCount(ImmutableList<ColumnLayoutStats> clustering) {
		int total = 0;
		for (ColumnLayoutStats columnLayoutStats : clustering) {
			total += columnLayoutStats.recordCount;
		}
		return total;
	}

	/** Clones the passed BitSet (saves a cast)*/
	public static BitSet cloneBitSet(BitSet b) {
		return (BitSet) b.clone();
	}

	/** Useful to suppress unused warnings on some variables */
	@SuppressWarnings("UnusedParameters")
	public static void unused(Object o) {}

	/**
	 * A {@link RecordProcessor} that collects frequency statistics about processed record layouts.
	 * This class also provides methods to cluster layouts based on usage statistics.
	 */
	public static class LayoutStatProcessor implements SlugLayoutSimulator.RecordProcessor {
		private final int[] freq;

		/** Creates a new layout stat collector */
		public LayoutStatProcessor(int layoutCount) {
			this.freq = new int[layoutCount];
		}

		@Override
		public void processRecord(int layoutIdx) {
			freq[layoutIdx]++;
		}

		/** @return a list of {@link FieldLayoutStats}s*/
		public List<FieldLayoutStats> buildFieldLayoutStats(BitSet[] fieldLayouts) {
			final List<FieldLayoutStats> layoutStats = Lists.newArrayList();
			for (int i = 0; i < freq.length-1; i++) {
				layoutStats.add(new FieldLayoutStats(fieldLayouts[i], freq[i]));
			}
			return layoutStats;
		}

		/** @return a list of {@link ColumnLayoutStats}s sorted by number of records using it in ascending order */
		public List<ColumnLayoutStats> buildColumnLayoutStats(BitSet[] columnLayouts) {
			// There is a chance that when we reduce two distinct field layouts to a column layout that
			// both layouts will end up being the same column layout. Grouping them together will increase
			// the chances of having null vectors. We're currently not taking advantage of this.
			final List<ColumnLayoutStats> layoutStats = Lists.newArrayList();
			for (int i = 0; i < freq.length-1; i++) {
				layoutStats.add(new ColumnLayoutStats(columnLayouts[i], i, freq[i]));
			}
			Collections.sort(layoutStats);
			return layoutStats;
		}

		/** Field layout statistics */
		public static class FieldLayoutStats {
			public final BitSet usedFields;
			public final int recordCount;

			FieldLayoutStats(BitSet usedFields, int recordCount) {
				this.usedFields = usedFields;
				this.recordCount = recordCount;
			}
		}

		/** Column layout statistics  */
		public static class ColumnLayoutStats implements Comparable<ColumnLayoutStats>{
			final BitSet clearColumns;
			final int layoutIdx;
			final int recordCount;

			ColumnLayoutStats(BitSet clearColumns, int layoutIdx, int recordCount) {
				this.clearColumns = clearColumns;
				this.layoutIdx = layoutIdx;
				this.recordCount = recordCount;
			}

			@Override
			public int compareTo(ColumnLayoutStats o) {
				return Integer.compare(o.recordCount, recordCount); //Ascending order
			}

			@Override
			public String toString() {
				return String.format("%s:%s", layoutIdx, recordCount);
			}
		}
	}

	/**
	 * A {@link SlugLayoutSimulator.RecordProcessor} that multiplexes calls to {@link #processRecord(int)}
	 * on different instances of {@link SimulatorUtil.RecordPacker}, depending on the layoutIdx of the
	 * record being processed.
	 * <p/>
	 * This class receives an array of {@link RecordPacker}, the array should have one element per valid layout index.
	 * <p/>
	 * The {@link #processRecord(int)} method will delegate the actual record processing to the RecordPacker
	 * corresponding to the layoutIdx of the record being processed.
	 * On each call to {@link #flush()} all the underlying {@link RecordPacker}s will be flushed once per instance.
	 */
	public static class MultiplexingPacker implements SlugLayoutSimulator.RecordProcessor {
		private final RecordPacker[] packersByLayout;

		/**
		 * Creates a new multiplexing packer.
		 * @param packersByLayout array mapping layoutIdx to {@link SimulatorUtil.RecordPacker}
		 */
		public MultiplexingPacker(RecordPacker[] packersByLayout) {
			this.packersByLayout = packersByLayout;
		}

		@Override
		public void processRecord(int layoutIdx) {
			packersByLayout[layoutIdx].processRecord(layoutIdx);
		}

		/** Flushes the all the underlying {@link RecordPacker}s once.*/
		public void flush() {
			for (RecordPacker packer : ImmutableSet.copyOf(Arrays.asList(packersByLayout))) {
				packer.flush();
			}
		}

		/** @return list of segments (flush should have been called by now) */
		public List<int[]> getSegments() {
			final List<int[]> result = Lists.newArrayList();
			for (RecordPacker packer : ImmutableSet.copyOf(Arrays.asList(packersByLayout))) {
				result.addAll(packer.getSegments());
			}
			return result;
		}
	}

	/** Packs fields in columns in the order they're passed */
	public static class FieldPacker {
		private int column;
		private int bitsUsed;
		private final List<Field> fields = Lists.newArrayList();
		private final Set<Field> alreadyPacked = Sets.newHashSet();

		/** Packs the specified field, adding a column if needed. */
		public FieldPacker pack(Field field) {
			checkArgument(!packed(field), "Field '%s' already packed", field.name);
			if (!fitsInCurrentColumn(field)) {
				column++;
				bitsUsed = 0;
			}
			bitsUsed += field.size;
			fields.add(new Field(field, column));
			alreadyPacked.add(field);
			return this;
		}

		/** @return true if packing the field will not cause a new column to be allocated */
		public boolean fitsInCurrentColumn(Field field) {
			return field.size + bitsUsed <= 32;
		}

		/** @return true if the field has already been packed */
		public boolean packed(Field field) {
			return alreadyPacked.contains(field);
		}

		/** Allocates a new column if the current one has at least one bit occupied */
		public FieldPacker newColumn() {
			if (bitsUsed > 0) {
				++column;
				bitsUsed = 0;
			}
			return this;
		}

		/** @return a list of fields packed by this FieldPacker*/
		public List<Field> getFields() {
			return fields;
		}
	}

	/** Simple record packer that packs records snugly into segments until the segment is full */
	public static class RecordPacker implements RecordProcessor {
		protected List<int[]> segments = new ArrayList<>();
		protected int[] current;
		protected int currentIdx;
		protected final int segmentSize;

		/**
		 * Initialize a segment packer
		 * @param segmentSize number of rows per segment
		 */
		public RecordPacker(int segmentSize) {
			this.segmentSize = segmentSize;
		}

		@Override
		public void processRecord(int layoutIdx) {
			if (current == null || currentIdx >= current.length) {
				flush();
				current = new int[segmentSize];
			}
			current[currentIdx++ ] = layoutIdx;
		}

		/**
		 * Flush layout any remaining changes.
		 */
		public void flush() {
			if (current != null)
				segments.add(Arrays.copyOf(current, currentIdx));
			current = null;
			currentIdx = 0;
		}

		public List<int[]> getSegments() {
			return segments;
		}
	}

	private static String[] siPostfix = new String[] { "", "K", "M", "G", "T", "P", "E" };

	public static final double EPS = 1e-11;

	/** Epsilon number below the significant fraction digits we use to compensate rounding */
	protected static final double SUB_EPSILON = 1e-6;

	/**
	 * @return n in SI format, e.i. postfix with K, M, G etc. If the resulting number
	 * has only one digit it will be returned with one decimal.
	 */
	public static String toSi(long n) {
		return toSiWithBase(n, 1000);
	}

	/**
	 * @return same as {@link #toSi(long)}, but takes a double. Note that this method will not print
	 *         any decimals, and if the number does not fit in a long it will be printed in
	 *         scientific notation.
	 */
	public static String toSi(double n) {
		return n > Long.MAX_VALUE ?
				Double.toString(n) :
				toSiWithBase((long) n, 1000);
	}

	/** @return n in SI format with base 1024, e.g. "2.3MB". Forwards to {@link #toSiWithBase(double, int)} */
	public static String toSiBytes(double n) {
		return toSiWithBase(n, 1024) + "B";
	}

	private static final DecimalFormat TO_SI_DF = getDecimalFormatFrom(1);

	private static DecimalFormat getDecimalFormatFrom(int i) {
		final DecimalFormat df = new DecimalFormatEpsilon("0.0");
		df.setMinimumFractionDigits(1);
		df.setMaximumFractionDigits(1);
		return df;
	}

	/**
	 * @return n in SI format with the given base (typically 1000 or 1024), i.e. postfix with
	 * K, M, G etc. If the resulting number has only one digit, or it has postfix G or higher,
	 * it will be returned with one decimal.
	 */
	private static String toSiWithBase(double n, int siBase) {
		String prefix = ((n < 0.0) ? "-" : "");
		int i = 0;
		double v = Math.abs(n);
		while (v >= siBase) {
			v /= siBase;
			i++;
		}
		v += EPS;
		if (Math.round(v) >= siBase) {
			v /= siBase;
			i++;
		}
		if (i < 3 && (i == 0 || v >= 9.5)) return prefix + String.valueOf(Math.round(v)) + siPostfix[i];
		return prefix + TO_SI_DF.format(v + SUB_EPSILON) + siPostfix[i];
	}

	/**
	 * Add or subtract an epsilon value to the given double depending on the RoundingMode, in order to compensate for
	 * double math artifacts and obtain a consistent value when rounded.
	 * Examples for 0 maximum fractional digits:
	 * <ul>
	 * <li>1.49999999999998 => 2
	 * <li>2.49999999999998 => 2
	 * <li>1.50000000000001 => 2
	 * <li>2.50000000000001 => 2
	 * </ul>
	 * The epsilon used to compensate is 6 orders of magnitude below the last significant fractional digit, i.e. if
	 * fractionalDigits is 2, the epsilon will be 1e-8.
	 * <p>
	 * If d is NaN, Infinite or MAX_VALUE, we return d unaltered.
	 * <p>
	 * For now, we only support {@link RoundingMode#HALF_EVEN} and {@link RoundingMode#HALF_UP}
	 * <p>
	 * d values near {@link Double#MAX_VALUE} (around fractionalDigits orders of magnitude away) could make the method
	 * return Infinity.
	 *
	 * @param d initial value
	 * @param rm rounding method used when formatting
	 * @param fractionalDigits number of digits to show in the formatted representation
	 * @param multiplier a multiplier applied to the number for formatting purposes (as in {@link DecimalFormat#getMultiplier()}
	 * @return compensated value
	 */
	public static double applyEpsilon(double d, RoundingMode rm, int fractionalDigits, int multiplier) {
		if (Double.isNaN(d) || Double.isInfinite(d) || d == 0.0) return d;

		double fractFactor = Math.pow(10, fractionalDigits) * multiplier;
		double eps = Math.signum(d) * SUB_EPSILON / fractFactor;
		switch (rm) {
			case HALF_EVEN:
				// check if the last digit before rounding is odd to calculate rounding direction
				double finalD = d * fractFactor;
				boolean odd = (((int) (finalD % 2.0d)) & 1) != 0;
				return d + (odd ? eps : -eps);
			case HALF_UP:
				return d + eps;
			default: throw new UnsupportedOperationException("Rounding mode " + rm + " is not yet supported" );
		}
	}

	/**
	 * Extension of DecimalFormat, where double numbers are compensated for double math artifacts before formatting.
	 * We apply an Epsilon value depending on the {@link java.math.RoundingMode}.
	 */
	public static class DecimalFormatEpsilon extends DecimalFormat {

		/** see {@link DecimalFormat#DecimalFormat()} */
		protected DecimalFormatEpsilon() {
			super();
		}

		/** see {@link DecimalFormat#DecimalFormat(String)} */
		protected DecimalFormatEpsilon(String pattern) {
			super(pattern);
		}

		/** see {@link DecimalFormat#DecimalFormat(String, java.text.DecimalFormatSymbols)} */
		protected DecimalFormatEpsilon(String pattern, DecimalFormatSymbols symbols) {
			super(pattern, symbols);
		}

		@Override public StringBuffer format(double number, StringBuffer result,
											 FieldPosition fieldPosition) {
			return super.format(applyEpsilon(number, getRoundingMode(), getMaximumFractionDigits(), getMultiplier()), result, fieldPosition);
		}

	}
}
