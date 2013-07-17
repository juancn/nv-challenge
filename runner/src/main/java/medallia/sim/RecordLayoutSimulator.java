package medallia.sim;

import medallia.sim.data.Field;

import java.util.BitSet;
import java.util.List;

/**
 * Base class for simulators. The meat of the logic is in
 * {@link SimulatorBase}.
 */
public abstract class RecordLayoutSimulator extends SimulatorBase {
	protected final DataSet dataSet;

	/**
	 * Initialize simulator with given layout and fields.
	 */
	public RecordLayoutSimulator(BitSet[] layouts, List<Field> fields, DataSet dataSet) {
		super(layouts, fields);
		this.dataSet = dataSet;
	}
}
