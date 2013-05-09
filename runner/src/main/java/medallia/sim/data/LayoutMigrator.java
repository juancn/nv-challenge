package medallia.sim.data;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import express.web.test.DumpSlugCompleteLayout;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

public class LayoutMigrator {

	public static void main(String[] args) throws IOException, ClassNotFoundException {
		final Iterable<Path> paths = Iterables.transform(Arrays.asList(args), new Function<String, Path>() {
			@Override
			public Path apply(String fileName) {
				return Paths.get(fileName);
			}
		});
		for (Path path : paths) {
			final DumpSlugCompleteLayout.Layout srcLayout;

			try (InputStream in = Files.newInputStream(path);
				 InputStream gzip = new GZIPInputStream(in);
				 ObjectInputStream ois = new ObjectInputStream(gzip)) {
				srcLayout = (DumpSlugCompleteLayout.Layout) ois.readObject();
			}

			Layout anonymous = anonymize(srcLayout);
			System.out.println("migrated = " + anonymous);
		}
	}

	private static Layout anonymize(DumpSlugCompleteLayout.Layout layout) {
		final Layout anonymized = new Layout();
		int nameCounter = 0;
		for (Entry<String, DumpSlugCompleteLayout.CompanyLayout> entry : layout.companies.entrySet()) {
			anonymized.companies.put("company" + ++nameCounter, anonymize(entry.getValue()));
		}
		return anonymized;
	}

	private static CompanyLayout anonymize(DumpSlugCompleteLayout.CompanyLayout layout) {
		final CompanyLayout anonymized = new CompanyLayout();
		int nameCounter = 0;
		for (Entry<String, DumpSlugCompleteLayout.DatasetLayout> entry : layout.datasets.entrySet()) {
			anonymized.datasets.put("dataset" + ++nameCounter, anonymize(entry.getValue()));
		}
		return anonymized;
	}

	private static DatasetLayout anonymize(DumpSlugCompleteLayout.DatasetLayout layout) {
		final DatasetLayout anonymized = new DatasetLayout();

		// These are safe
		anonymized.layouts = layout.layouts;
		anonymized.segments = layout.segments;

		// Anonymize fields
		anonymized.fields = new Field[layout.fields.length];
		for (int i = 0; i < layout.fields.length; i++) {
			anonymized.fields[i] = new Field("field" + i, layout.fields[i].size, layout.fields[i].column);
			anonymized.fields[i].index = i;
		}
		return anonymized;
	}
}
