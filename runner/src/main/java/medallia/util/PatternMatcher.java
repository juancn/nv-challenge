package medallia.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static medallia.util.SimulatorUtil.uncheckedCast;

/**
 * This is an implementation of an Aho-Corasick pattern matcher specialized on byte strings.
 * Regardless, this class could easily be generalized to other data types.
 *
 * <p> </p>Matchers created by this class simultaneously match all patterns trained in constant time
 * and space, independent of the number of patterns to be matched.
 *
 * This means that search times are O(N) on the input regardless of the number of patterns
 * to be matched.
 *
 * Construction time is linear on the sum of the number of elements in all input patterns.
 * <p/>
 * {@link PatternMatcher}s are first built and then {@link Matcher}s are used to check matching patterns.
 *
 * Example:
 * <pre>
 * PatternMatcher<String> p = new PatternMatcher<String>();
 *
 * //First add all the patterns
 * p.add("he".getBytes(), "he");
 * p.add("she".getBytes(), "she");
 * p.add("his".getBytes(), "his");
 * p.add("hers".getBytes(), "hers");
 *
 * p.build(); //Build it
 *
 * Matcher<String> m = p.createMatcher();
 *
 * System.out.println(Arrays.asList(m.match((byte)'s'))); //prints []
 * System.out.println(Arrays.asList(m.match((byte)'h'))); //prints []
 * System.out.println(Arrays.asList(m.match((byte)'e'))); //prints [he, she]
 * </pre>
 * Note that if a pattern is a suffix of another pattern, multiple matches might be reported by
 * the {@code match()} method.
 * <p/>
 * For details see:
 * <br/>
 * <strong>Alfred V. Aho and Margaret J. Corasick.</strong> <i>Efficient string matching: An aid to bibliographic search.</i> Communications of the ACM 18(6):333–340, June 1975
 * @param <T> type of the action object associated with each pattern.
 */
public class PatternMatcher<T>
{
	/** True if this {@link PatternMatcher} has been built */
	private boolean built;

	/** Initial state of the Aho-Corasick state machine */
	private final State<T> start;

	/** Creates a new un-initialized BinaryPattern */
	public PatternMatcher() {
		start = new State<>();
	}

	/**
	 * Adds a new binary pattern with is associated action. Actions are returned by the
	 * Matcher.match() method when a match is found
	 * @param pattern byte array containing the pattern to be sought
	 * @param action object returned when a match is found
	 * @throws IllegalStateException if this method is called after this BinaryPattern is built.
	 */
	public final void add(final byte[] pattern, final T action) {
		ensureNotBuilt();
		start.add(0, pattern, action);
	}

	private void ensureNotBuilt() {
		if (built) {
			throw new IllegalStateException("pattern already built");
		}
	}

	/**
	 * Builds this BinaryPattern.
	 * Building a pattern prepares the internal structures for matching.
	 * @throws IllegalStateException if this method is called when this BinaryPattern is already built.
	 */
	public void build() {
		ensureNotBuilt();

		built = true;

		/**
		 * In this phase, we fist group all states by level (so far we have a tree).
		 * Then we work from one level to the next, updating the failure function and output function.
		 * Finally we update the leaf nodes of the tree to behave like the start state.
		 */
		final List<Collection<State<T>>> levels = new ArrayList<>();
		start.collectLevels(levels, 0);

		//Init first two levels
		start.failure = start;

		for (State child : start.success) {
			child.failure = start;
		}

		//Init level d+1 from level d, starting from level 1
		for (int i = 1; i < levels.size(); i++) {
			for (State<T> r : levels.get(i)) {
				if (r.keys.length == 0) {
					//I'm the last one in the chain, link back to the beginning
					r.keys = start.keys;
					r.success = start.success;
					r.failure = start;
				}
				else {
					// Walk the failure chain for each key
					// set the failure function of that key to
					// the first state that matched
					for (int j = 0; j < r.keys.length; j++) {
						final byte     key = r.keys[j];
						final State<T> success = r.success[j];
						success.failure = walkFailureChain(key, r.failure);

						//There was a suffix match: we must append the outputs
						if (success.failure.output != null) {
							if (success.output != null) {
								success.output.addAll(success.failure.output);
							}
							else {
								success.output = success.failure.output;
							}
						}
					}
				}
			}
		}

		//Make the output lists read-only
		start.freezeOutput(new HashSet<State<T>>());
	}

	private State<T> walkFailureChain(byte key, State<T> failure) {
		// Wak the failure chain until we find the key
		State<T> state = failure;
		State<T> c = state.find(key);

		while (c == null) {
			state = state.failure;
			c = state.find(key);
		}
		return c;
	}

	/**
	 * Creates a new Matcher object.
	 * This method is thread safe once this pattern is built and properly exposed.
	 * @return a new Matcher object.
	 * @throws IllegalStateException if this pattern has not been built.
	 */
	public Matcher<T> createMatcher() {
		if (!built) {
			throw new IllegalStateException("pattern not built");
		}
		return new Matcher<>(start);
	}
	/** Provides methods to check a match. */
	public static class Matcher<T> {
		private State<T> current;

		private Matcher(State<T> current) {
			this.current = current;
		}

		/**
		 * Updates the state of this matcher with a new byte.
		 * @param b a new byte taken from the input to update the matcher state
		 * @return a list of 'actions' corresponding to all matches found. The list is empty if no match occured.
		 */
		public List<T> match(byte b) {
			current = current.find(b);
			return current.output;
		}

		/** @return the 'actions' matched by the last call to match or empty if none. */
		public List<T> current() {
			return current.output;
		}
	}

	/** A state of the state machine */
	private static final class State<T> {
		/** Keys for each match (indices are correlated to {@link #success} */
		private byte[] keys;

		/** Where to go on a successful match */
		private State<T>[] success;

		/** Failure function (i.e. where to go when matching fails) */
		private State<T> failure;

		/** Contains a list of actions for this state (i.e. matches)*/
		private List<T> output;

		private State() {
			keys = new byte[0];
			success = uncheckedCast(new State[0]);
			output = new ArrayList<>();
		}

		/** @return the next state given a specific byte */
		private State<T> find(byte a) {
			final State<T> result;
			final int index = Arrays.binarySearch(keys, a);
			if (index < 0) {
				result = failure;
			} else {
				result = success[index];
			}
			return result;
		}

		private void add(final int index, final byte[] pattern, final T action) {
			if (index == pattern.length) {
				// We're a terminal state for the pattern, add the action to the output list
				output.add(action);
			} else {
				final byte c = pattern[index];

				// Search existing states
				int where = Arrays.binarySearch(keys, c);
				if (where < 0) {
					// Insert a new one
					where = -where - 1;

					// Expand the arrays as needed
					final byte[]     nkeys = new byte[keys.length + 1];
					final State<T>[] nchildren = uncheckedCast(new State[success.length + 1]);

					System.arraycopy(keys, 0, nkeys, 0, where);
					System.arraycopy(keys, where, nkeys, where + 1, keys.length - where);
					System.arraycopy(success, 0, nchildren, 0, where);
					System.arraycopy(success, where, nchildren, where + 1, success.length - where);

					// Update the current state
					keys = nkeys;
					success = nchildren;
					keys[where] = c;
					success[where] = new State<>();
				}
				// Propagate
				success[where].add(index + 1, pattern, action);
			}
		}


		private void collectLevels(List<Collection<State<T>>> levels, int level) {
			if (levels.size() == level) {
				levels.add(new ArrayList<State<T>>());
			}

			final Collection<State<T>> c = levels.get(level);
			c.add(this);

			for (State<T> child : success) {
				child.collectLevels(levels, level + 1);
			}
		}

		private void freezeOutput(Set<State<T>> visited) {
			if (!visited.contains(this)) {
				visited.add(this);

				// Freeze output
				output = output.isEmpty() ? Collections.<T>emptyList() : Collections.unmodifiableList(output);

				for (State<T> child : success) {
					child.freezeOutput(visited);
				}
			}
		}
	}
}
