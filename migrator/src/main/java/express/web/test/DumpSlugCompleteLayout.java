package express.web.test;


import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Dump complete slug layout to file.
 */
public class DumpSlugCompleteLayout {

	public static class Field implements Serializable {
		private static final long serialVersionUID = 1L;
		public final String name;
		public final int size;
		public final int column;
		private transient int index;
		public Field(String name, int size, int column) {
			this.name = name;
			this.size = size;
			this.column = column;
		}
		public Field(Field other, int newColumn) {
			this(other.name, other.size, newColumn);
			this.index = other.index;
		}
		public int getIndex() {
			return index;
		}
	}

	/**
	 * Layout for a single dataset
	 */
	public static class DatasetLayout implements Serializable {
		private static final long serialVersionUID = 1L;

		/** List of fields */
		public Field[] fields;

		/**
		 * Field with value for given layout id
		 */
		public BitSet[] layouts;

		/**
		 * List of layout id per segment. Each value represents a row.
		 */
		public List<int[]> segments;

		private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
			in.defaultReadObject();
			// Initialize the field's index
			for (int i = 0; i < fields.length; i++) {
				fields[i].index = i;
			}
		}
	}

	/**
	 * Layout of Company
	 */
	public static class CompanyLayout implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * Mapping from dataset name to statistics
		 */
		public Map<String, DatasetLayout> datasets = new HashMap<>();
	}

	/**
	 * Complete Layout structure
	 */
	public static class Layout implements Serializable {
		private static final long serialVersionUID = 1L;

		/**
		 * Mapping from company name to dataset mapping.
		 */
		public Map<String, CompanyLayout> companies = new HashMap<>();
	}
}
