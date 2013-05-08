package medallia.util;

import medallia.sim.data.Field;
import medallia.runner.SlugLayoutSimulator;
import medallia.sim.RecordLayoutSimulator;

import java.util.BitSet;
import java.util.List;

/** Just pack segments as they come in the order they come */
public class SimpleRecordLayoutSimulator extends RecordLayoutSimulator {
	private final RecordPacker packer = new RecordPacker(SlugLayoutSimulator.SEGMENT_SIZE);

	/**
	 * Initialize simulator with given layout and fields.
	 */
	public SimpleRecordLayoutSimulator(BitSet[] layouts, List<Field> fields) {
		super(layouts, fields);
	}

	@Override
	public List<int[]> getSegments() {
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
