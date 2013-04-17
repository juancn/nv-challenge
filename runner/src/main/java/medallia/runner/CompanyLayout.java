package medallia.runner;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Layout of Company
 */
public class CompanyLayout implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Mapping from dataset name to statistics
	 */
	public Map<String, DatasetLayout> datasets = new HashMap<>();
}
