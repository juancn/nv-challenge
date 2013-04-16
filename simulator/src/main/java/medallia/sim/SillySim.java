package medallia.sim;

import express.web.test.DumpSlugCompleteLayout.Field;
import ibs.test.SimulatorUtil.FieldPacker;
import ibs.test.SlugLayoutSimulator.FieldLayoutSimulator;
import ibs.test.SlugLayoutSimulator.RecordLayoutSimulator;
import ibs.test.SlugLayoutSimulator.SimpleRecordLayoutSimulator;
import ibs.test.SlugLayoutSimulator.SimulatorFactory;
import tiny.CollUtils;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

/**
 * An extremely stupid simulator.
 * <p>
 * This simulator simply stuffs records into a continuously growing list of
 * segments. It ends the segment on any flush.
 * <p>
 * For field layout, it sorts the fields by size, then does eager
 * allocation. Note that this simulator would use less space if it didn't
 * modify the field-order, as Slug's current bitpacking is quite good.
 */
public class SillySim {
	/**
	 * The field layout simulator, which does eager bit allocation for fields.
	 */
	public static class SillyFieldLayoutSimulator extends FieldLayoutSimulator {
		/**
		 * Construct a silly field layout simulator.
		 */
		public SillyFieldLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			super(layouts, fields);
		}

		@Override
		public void processRecord(int layoutIdx) {
		}

		@Override
		protected List<Field> getFields() {
			List<Field> sorted = CollUtils.sortedCopy(fields, new Comparator<Field>() {
				@Override
				public int compare(Field o1, Field o2) {
					return Integer.compare(o1.size, o2.size);
				}
			});

			FieldPacker packer = new FieldPacker();
			for (Field field : sorted) {
				packer.pack(field);
			}
			return packer.getFields();
		}
	}

	/**
	 * The actual record layout simulator. This assumes our field layout  has already done field
	 * allocation for us. It packs all records as they come until a segment is full
	 * or {@link #flush()} is called.
	 */
	public static class SillyRecordLayoutSimulator extends SimpleRecordLayoutSimulator {
		/**
		 * Construct a silly simulator.
		 */
		public SillyRecordLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			super(layouts, fields);
		}
	}

	/**
	 * Factory for initializing {@link ibs.test.SillySim.SillyRecordLayoutSimulator}s.
	 */
	public static final SimulatorFactory FACTORY = new SimulatorFactory() {
		@Override
		public RecordLayoutSimulator createRecordLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			return new SillyRecordLayoutSimulator(layouts, fields);
		}

		@Override
		public SillyFieldLayoutSimulator createFieldLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			return new SillyFieldLayoutSimulator(layouts, fields);
		}

		@Override
		public String getName() {
			return "Silly";
		}
	};
}
