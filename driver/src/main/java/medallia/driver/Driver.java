package medallia.driver;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import medallia.runner.Analysis;
import medallia.sim.SimulatorFactory;
import medallia.sim.data.CompanyLayout;
import medallia.sim.data.DatasetLayout;
import medallia.sim.data.Layout;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import static medallia.util.SimulatorUtil.toSi;

public class Driver {

	public static void main(String[] args) throws Exception {
		final String jarFilename = args[0];
		final String jsonFilename = args[1];
		final String progressFilename = args[2];
		final Iterable<String> dataFiles = Iterables.skip(Arrays.asList(args), 3);

		final SimulatorFactory fac = build(jarFilename);

		final Iterable<Path> paths = Iterables.transform(dataFiles, new Function<String, Path>() {
			@Override
			public Path apply(String fileName) {
				return Paths.get(fileName);
			}
		});

		// Collect total bytes used
		long totalUsedBytes = 0;

		// progress reporting
		int pathsProcessed = 0;
		final int totalPaths = args.length - 3;

		for (Path path : paths) {
			final Layout layout;

			try (InputStream in = Files.newInputStream(path); InputStream gzip = new GZIPInputStream(in); ObjectInputStream ois = new ObjectInputStream(gzip)) {
				layout = (Layout) ois.readObject();
			}
			final int totalCompanies = layout.companies.size();
			int companiesProcessed = 0;
			for (Entry<String, CompanyLayout> company : layout.companies.entrySet()) {
				for (Entry<String, DatasetLayout> dataset : company.getValue().datasets.entrySet()) {
					final DatasetLayout stats = dataset.getValue();

					// Ignore empty datasets
					if (stats.segments.isEmpty())
						continue;

					final Analysis analysis = Analysis.simulateCompany(fac, stats);


					reportProgress(progressFilename, (pathsProcessed/(double)totalPaths) + ((companiesProcessed/(double)totalCompanies)/(double)totalPaths) );

					//System.out.printf("%s:%s:%s %s\n", company.getKey(), dataset.getKey(), name, analysis);

					// Update total used bytes
					totalUsedBytes += analysis.usedBytes;
				}
				++companiesProcessed;
			}
			++pathsProcessed;
		}

		writeResult(jsonFilename, totalUsedBytes);
	}

	private static SimulatorFactory build(String jarFilename) throws ClassNotFoundException, MalformedURLException, InstantiationException, IllegalAccessException {
		Class<?> numberProviderClass = Class.forName("medallia.sim.Submission", true, buildClassLoader(jarFilename));
		return (SimulatorFactory) numberProviderClass.newInstance();
	}

	private static URLClassLoader buildClassLoader(String jarFilename) throws MalformedURLException {
		return new URLClassLoader(new URL[]{new File(jarFilename).toURI().toURL()});
	}

	private static void reportProgress(String progressFilename, double completionRatio) throws FileNotFoundException {
		try (PrintWriter p = new PrintWriter(progressFilename)) {
			p.println(completionRatio);

		}
	}

	private static void writeResult(String jsonFilename, long totalUsedBytes) throws FileNotFoundException {
		try (PrintWriter p = new PrintWriter(jsonFilename)) {
			p.printf("{ \"totalUsedBytes\": %s }%n", totalUsedBytes);
		}
	}
}
