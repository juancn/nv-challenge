package medallia.driver;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

public class Driver {

	private static Object build(String jarFilename) throws ClassNotFoundException, MalformedURLException, InstantiationException, IllegalAccessException {
		Class<?> numberProviderClass = Class.forName("biggestnumber.NumberProviderSubmission", true, buildClassLoader(jarFilename));
		return numberProviderClass.newInstance();
	}

	private static URLClassLoader buildClassLoader(String jarFilename) throws MalformedURLException {
		return new URLClassLoader(new URL[]{new File(jarFilename).toURI().toURL()});
	}

	private static void reportProgress(String progressFilename, double completionRatio) throws FileNotFoundException {
		try (PrintWriter p = new PrintWriter(progressFilename)) {
			p.write(Double.toString(completionRatio));
		}
	}

	private static void writeJson(String jsonFilename, int number) throws FileNotFoundException {
		try (PrintWriter p = new PrintWriter(jsonFilename)) {
			p.write(String.format("{ \"number\": %d }", number));
		}
	}
}
