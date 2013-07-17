package medallia.sim;

import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;

/** Represents a segment in the dataset */
public class Segment implements Iterable<Integer> {
	/** Maximum size of a segment */
	public static final int MAX_SEGMENT_SIZE = 65536;

	/** Row data (values are indexes into the layouts array) */
	private int[] data = new int[1024];

	/** Next empty row */
	private int next;

	public Segment() {
		Arrays.fill(data, -1);
	}

	/**
	 * Attempts to add a record with the specified layout to this segment
	 * @param layoutIdx record layout (index into layouts array)
	 * @return true if added, false otherwise
	 */
	public boolean tryAdd(int layoutIdx) {
		if (next >= data.length) {
			if (data.length < MAX_SEGMENT_SIZE) {
				data = Arrays.copyOf(data, data.length*2);
				Arrays.fill(data, next, data.length, -1);
			} else {
				// Segment reached maximum size
				return false;
			}
		}

		data[next++] = layoutIdx;
		return true;
	}

	/** @return an array containing all the records successfully added to this segment. */
	public int[] getAllocatedRecords() {
		return Arrays.copyOfRange(data, 0, next);
	}

	/** @return an array containing all the including empty ones. */
	public int[] getAllRecords() {
		return data.clone();
	}

	@Override
	public Iterator<Integer> iterator() {
		return new RecordIterator();
	}

	/** @return number of allocated records in this segment */
	public int getRecordCount() {
		return next;
	}

	/** Iterator over the records of a {@link Segment} */
	public final class RecordIterator implements Iterator<Integer> {
		private int nextRecord;

		@Override
		public Integer next() {
			return nextInt();
		}

		/**
		 * Returns the next element in the iteration.
		 *
		 * This method is the same than {@link #next()} but avoid boxing, so it should
		 * be faster to use.
		 *
		 * @return the next element in the iteration
		 * @throws NoSuchElementException if the iteration has no more elements
		 */
		public int nextInt() {
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			return data[nextRecord++];
		}

		@Override
		public boolean hasNext() {
			return nextRecord < next;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
