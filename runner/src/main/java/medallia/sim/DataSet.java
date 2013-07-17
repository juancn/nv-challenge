package medallia.sim;

import com.google.common.collect.Iterators;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A collection of Segments (initially empty).
 */
public final class DataSet implements Iterable<Segment> {
	/** List of segments */
	private final List<Segment> segments = new ArrayList<>();

	public long getTotalRecordCount() {
		long total = 0;
		for (Segment segment : segments) {
			total += segment.getRecordCount();
		}
		return total;
	}

	/**
	 * Adds the record with the specified layout in the first available segment.
	 * This method will add a new segment if necessary.
	 * @param layoutIdx the record layout
	 */
	public void addAnywhere(int layoutIdx) {
		for (Segment segment : segments) {
			if (segment.tryAdd(layoutIdx)) {
				return;
			}
		}
		addSegment().tryAdd(layoutIdx);
	}

	/**
	 * Adds the record with the specified layout in the last segment if possible.
	 * This method will add a new segment if necessary.
	 * @param layoutIdx the record layout
	 */
	public void addToLast(int layoutIdx) {
		if (segments.isEmpty() || !segments.get(segments.size()-1).tryAdd(layoutIdx)) {
			addSegment().tryAdd(layoutIdx);
		}
	}

	/**
	 * Inserts a new segment at the specified position.
	 * Shifts the segment currently at that position (if any)
	 * and any subsequent elements to the right (adds one to their indices).
	 *
	 * @param index index at which the new segment is to be inserted
	 */
	public Segment insertSegmentAt(int index) {
		final Segment newSegment = new Segment();
		segments.add(index, newSegment);
		return newSegment;
	}

	/** @return the segment at the specified index */
	public Segment getSegment(int index) {
		return segments.get(index);
	}

	/** @return the number of segments */
	public int getSegmentCount() {
		return segments.size();
	}

	@Override
	public Iterator<Segment> iterator() {
		return Iterators.unmodifiableIterator(segments.iterator());
	}

	/**
	 * Adds a segment to the end of the list
	 * @return the newly created segment
	 */
	public Segment addSegment() {
		return insertSegmentAt(segments.size());
	}
}
