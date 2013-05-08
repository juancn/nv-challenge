package medallia.util;

import medallia.util.PatternMatcher.Matcher;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import static java.io.ObjectStreamConstants.TC_CLASSDESC;

/** Class that lets you do simple edits on an {@link InputStream} on the fly */
public class StreamEditor {

	/** A pattern matcher to find class descriptors in the stream */
	private final PatternMatcher<Substitution> pattern = new PatternMatcher<>();

	/** size of the circular buffer used for reading ahead */
	private int bufSize = 0;

	/** Creates a new {@link StreamEditor} */
	public StreamEditor() {}

	/**
	 * Adds a substitution of one class for another on a Java serialization stream.
	 * @param oldClassName the old class' name
	 * @param oldSerialUID the old class' serialVersionUID
	 * @param newClassName the new class' name
	 * @param newSerialUID the new class' serialVersionUID
	 */
	public void addSubstitution(final String oldClassName, final long oldSerialUID, final String newClassName, final long newSerialUID) {
		addSubstitution(makePattern(oldClassName, oldSerialUID), makePattern(newClassName, newSerialUID));
	}

	/**
	 * Adds a binary substitution
	 * @param oldPattern pattern to be matched against
	 * @param newPattern replacement
	 */
	public void addSubstitution(byte[] oldPattern, byte[] newPattern) {
		final Substitution substitution = new Substitution(oldPattern, newPattern);
		bufSize = Math.max(bufSize, substitution.oldPattern.length);
		bufSize = Math.max(bufSize, substitution.newPattern.length);
		pattern.add(substitution.oldPattern, substitution);
	}

	/** Finalizes construction of this object */
	public void build() {
		pattern.build();
	}

	/** @return wraps the specified input stream in one that does substitutions on-the-fly */
	public InputStream replace(final InputStream in) {
		final Matcher<Substitution> matcher = pattern.createMatcher();
		return new InputStream() {
			/** Circular buffer */
			final byte[] buffer = new byte[bufSize+1];
			int head;
			int tail;

			/** Current replacement */
			byte[] replacement;
			int idx;

			@Override
			public int read() throws IOException {
				// We always try to keep the buffer full so we can undo reads
				while ( ((head+1) % buffer.length) != tail ) {
					// Read from the current replacement or from the stream
					final boolean useReplacement = replacement != null && idx < replacement.length;
					final int b = useReplacement
									? replacement[idx++] & 0xFF
									: in.read();
					// EOF, bye!
					if (b == -1) break;

					// Fill the buffer if we're reading from the replacement or there is no match
					if (useReplacement || matcher.match((byte) b).isEmpty()) {
						buffer[head] = (byte) b;
						head = (head+1) % buffer.length;
					} else {
						// We have a match!
						final List<Substitution> substitutions = matcher.current();
						final Substitution substitution = substitutions.get(0);

						// Sanity checks...
						checkState(substitutions.size() == 1, "Unexpected number of matches");
						checkState((buffer.length + head - tail + 1) % buffer.length >= substitution.oldPattern.length, "Not enough data to unread??");
						checkState(replacement == null || idx == replacement.length, "Patterns are prefixes of one another");

						// Everything is kosher, un-read the old pattern
						head = (buffer.length + head - substitution.oldPattern.length + 1) % buffer.length;

						// Set a replacement
						replacement = substitution.newPattern;
						idx = 0;
					}
				}

				// Read from the circular buffer if not empty
				if (tail == head) return -1;
				final int b = buffer[tail];
				tail = (tail+1) % buffer.length;
				return b;
			}

			private void checkState(boolean cond, String msg) {
				if (!cond) {
					throw new IllegalStateException(msg);
				}
			}

		};
	}

	private byte[] makePattern(String className, long serialUID) {
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try (final DataOutputStream daos = new DataOutputStream(baos)) {
			daos.writeByte(TC_CLASSDESC);
			daos.writeUTF(className);
			daos.writeLong(serialUID);
		} catch (IOException e) {
			//Shouldn't happen
			throw new Error(e);
		}
		return baos.toByteArray();
	}

	private static class Substitution {
		final byte[] newPattern;
		final byte[] oldPattern;

		private Substitution(byte[] oldPattern, byte[] newPattern) {
			this.oldPattern = oldPattern;
			this.newPattern = newPattern;
		}
	}

	/** Used for testing */
	private static class A implements Serializable {
		final String text;
		static final long serialVersionUID = 1L;

		public A(String text) {
			this.text = text;
		}

		@Override
		public String toString() {
			return getClass() + ": " + text;
		}
	}


	/** Used for testing */
	private static class B implements Serializable {
		final String text;
		static final long serialVersionUID = 2L;

		public B(String text) {
			this.text = text;
		}

		@Override
		public String toString() {
			return getClass() + ": " + text;
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		final StreamEditor editor = new StreamEditor();
		editor.addSubstitution(A.class.getName(), A.serialVersionUID, B.class.getName(), B.serialVersionUID);
		editor.build();

		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		try ( final ObjectOutputStream oos = new ObjectOutputStream(baos) ) {
			oos.writeObject(new A("Hakuna matata"));
		}

		try ( final ObjectInputStream ois = new ObjectInputStream(editor.replace(new ByteArrayInputStream(baos.toByteArray()))) ) {
			B b = (B) ois.readObject();
			System.out.println("b = " + b);
		}
	}
}
