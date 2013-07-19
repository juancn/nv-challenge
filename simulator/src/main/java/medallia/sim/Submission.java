package medallia.sim;

import com.google.common.collect.Ordering;
import medallia.runner.SimulatorRunner;
import medallia.sim.data.Field;
import medallia.util.FieldPacker;

import java.util.BitSet;
import java.util.Comparator;
import java.util.List;

/**
 * An extremely simple simulator.
 * <p>
 * This simulator simply stuffs records into a continuously growing list of
 * segments. It ends the segment on any flush.
 * <p>
 * For field layout, it sorts the fields by size, then does eager allocation.
 */
public class Submission implements SimulatorFactory {
	/**
	 * The field layout simulator, which does eager bit allocation for fields.
	 */
	public static class SimpleFieldLayoutSimulator extends FieldLayoutSimulator {
		/**
		 * Construct a silly field layout simulator.
		 */
		public SimpleFieldLayoutSimulator(BitSet[] layouts, List<Field> fields) {
			super(layouts, fields);
		}

		@Override
		public void processRecord(int layoutIdx) {
			// We just ignore this information
		}

		@Override
		public List<Field> getFields() {
			List<Field> sorted = Ordering.from(new Comparator<Field>() {
				@Override
				public int compare(Field o1, Field o2) {
					return Integer.compare(o1.size, o2.size);
				}
			}).sortedCopy(fields);

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
	public static class SimpleRecordLayoutSimulator extends RecordLayoutSimulator {
		/**
		 * Initialize simulator with given layout and fields.
		 */
		public SimpleRecordLayoutSimulator(BitSet[] layouts, List<Field> fields, DataSet dataSet) {
			super(layouts, fields, dataSet);
		}

		@Override
		public void processRecord(int layoutIdx) {
			// The record needs to be placed in a segment as it's processed
			dataSet.addToLast(layoutIdx);
		}
	}

	@Override
	public RecordLayoutSimulator createRecordLayoutSimulator(BitSet[] layouts, List<Field> fields, DataSet dataSet) {
		return new SimpleRecordLayoutSimulator(layouts, fields, dataSet);
	}

	@Override
	public SimpleFieldLayoutSimulator createFieldLayoutSimulator(BitSet[] layouts, List<Field> fields) {
		return new SimpleFieldLayoutSimulator(layouts, fields);
	}

	@Override
	public String getName() {
		return "Simple";
	}

	/** Not actually called by the submission runner, useful for debugging. */
	public static void main(String[] args) throws Exception {
		SimulatorRunner.run(new Submission());
	}
}
