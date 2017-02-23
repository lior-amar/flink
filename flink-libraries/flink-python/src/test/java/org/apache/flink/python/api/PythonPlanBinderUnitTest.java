/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.python.api;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.test.util.JavaProgramTestBase;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.python.api.PythonPlanBinder.ARGUMENT_PYTHON_2;
import static org.apache.flink.python.api.PythonPlanBinder.ARGUMENT_PYTHON_3;

public class PythonPlanBinderUnitTest extends JavaProgramTestBase {

	private static boolean verbose = false;

	@Override
	protected boolean skipCollectionExecution() {
		return true;
	}

	private static String findUtilsFiles() throws Exception {
		FileSystem fs = FileSystem.getLocalFileSystem();
		return fs.getWorkingDirectory().toString()
				+ "/src/test/python/org/apache/flink/python/api/utils";
	}

	private static List<String> findUnitTestFiles() throws Exception {
		List<String> files = new ArrayList<>();
		FileSystem fs = FileSystem.getLocalFileSystem();
		FileStatus[] status = fs.listStatus(
				new Path(fs.getWorkingDirectory().toString()
						+ "/src/test/python/org/apache/flink/python/api"));
		for (FileStatus f : status) {
			Path path = f.getPath();
			String file = path.hasWindowsDrive()
				? path.getPath().substring(1)
				: path.getPath();
			String file_name = path.getName();

			if ((file_name.startsWith("unittest_") || file_name.startsWith("ut_")) && file_name.endsWith(".py")) {
				files.add(file);
			}
		}
		return files;
	}

	private static List<String> getFileTestCasesAndMethods(String file) {
		String file_fixed = file;
		if (file_fixed.startsWith("file:")) {
			file_fixed = file_fixed.substring(5);
		}

		List<String> tm = new ArrayList<>();
		String tmp = PythonPlanBinder.FLINK_PYTHON_FILE_PATH + PythonPlanBinder.r.nextInt();
		ProcessBuilder pb = new ProcessBuilder("python", new File(tmp, PythonPlanBinder.FLINK_PYTHON_PLAN_NAME).getAbsolutePath(), "--list-tests");
		String line;
		try {
			pb.redirectErrorStream(true);

			FileSystem fs = FileSystem.getLocalFileSystem();
			String cwd = fs.getWorkingDirectory().toString();
			if (cwd.startsWith("file:")) {
				cwd = cwd.substring(5);
			}
			if (verbose) {
				System.out.println("cwd: " + cwd);
			}
			PythonPlanBinder.prepareFiles(
				cwd + "/src/main/python/org/apache/flink/python/api/",
				tmp,
				new String[]{file_fixed, findUtilsFiles()});

			Process p = pb.start();
			BufferedReader bri = new BufferedReader((new InputStreamReader(p.getInputStream())));
			while ((line = bri.readLine()) != null) {
				tm.add(line);
			}
			p.waitFor();
		} catch (Exception err) {
			System.err.println("Error running [" + file + "]" + err);
		}

		return tm;
	}

	private static List<Map.Entry<String, String>> findUnitTestMethods(List<String> unitTestsFiles) {
		List<Map.Entry<String, String>> pairList = new ArrayList<>();

		for (String f : unitTestsFiles) {
			if (verbose) {
				System.out.println("Checking: " + f);
			}
			List<String> test_methods = getFileTestCasesAndMethods(f);
			for (String tm : test_methods) {
				pairList.add(new AbstractMap.SimpleEntry<String, String>(f, tm));
			}
		}
		return pairList;
	}

	private static boolean isPython2Supported() {
		try {
			Process p = Runtime.getRuntime().exec("python");
			p.destroy();
			try {
				p.waitFor();
			} catch (Exception e) {

			}
			return true;
		} catch (IOException ex) {
			return false;
		}
	}

	private static boolean isPython3Supported() {
		try {
			Process p = Runtime.getRuntime().exec("python3");
			p.destroy();
			try {
				p.waitFor();
			} catch (Exception e) {
			}
			return true;
		} catch (IOException ex) {
			return false;
		}
	}

	private static void runTestMethods(String pythonArg, List<Map.Entry<String, String>> allTests) throws Exception {
		String utilsPy = findUtilsFiles();
		if (verbose) {
			System.out.println("utils: " + utilsPy);
		}

		int i = 0;
		for(Map.Entry<String, String> ent : allTests) {
			String[] args = new String[]{pythonArg, ent.getKey(), utilsPy, "-", "--run-mode", ent.getValue()};
			if (verbose) {
				System.out.println(i + " Running - " + ent.getKey() + "  " + ent.getValue());
				for (String s : args) {
					System.out.println("Args: " + s);
				}
			}
			PythonPlanBinder.main(args);
			i++;
		}
	}


	@Override
	protected void testProgram() throws Exception {

		List<String> unitTestFiles = findUnitTestFiles();

		List<Map.Entry<String, String>> allTestMethods = findUnitTestMethods(unitTestFiles);

		if (isPython2Supported()) {
			if (verbose) {
				System.out.println("Running tests using python2");
			}
			runTestMethods(ARGUMENT_PYTHON_2, allTestMethods);
		}

		if (isPython3Supported()) {
			if (verbose) {
				System.out.println("Running tests using python3");
			}
			runTestMethods(ARGUMENT_PYTHON_3, allTestMethods);
		}
	}
}
