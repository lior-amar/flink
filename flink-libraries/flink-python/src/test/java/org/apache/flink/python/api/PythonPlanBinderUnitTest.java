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

	private static String findUtilsFile() throws Exception {
		FileSystem fs = FileSystem.getLocalFileSystem();
		return fs.getWorkingDirectory().toString()
				+ "/src/test/python/org/apache/flink/python/api/utils/utils.py";
	}

	private static String findSubproessTestCaseFile() throws Exception {
		FileSystem fs = FileSystem.getLocalFileSystem();
		return fs.getWorkingDirectory().toString()
				+ "/src/test/python/org/apache/flink/python/api/utils/subprocess_test.py";
	}

	private static List<String> findUnitTestFiles() throws Exception {
		List<String> files = new ArrayList<>();
		FileSystem fs = FileSystem.getLocalFileSystem();
		FileStatus[] status = fs.listStatus(
				new Path(fs.getWorkingDirectory().toString()
						+ "/src/test/python/org/apache/flink/python/api"));
		for (FileStatus f : status) {
			String file = f.getPath().toString();
			String[] file_parts = file.split("/");
			String file_name = file_parts[file_parts.length - 1];

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
		ProcessBuilder pb = new ProcessBuilder("python", file_fixed, "--list-tests");
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

			String pythonPath = cwd + "/src/main/python/org/apache/flink/python/api/" + ":" +
				                cwd + "/src/test/python/org/apache/flink/python/api/";


			Map<String, String> env = pb.environment();
			env.put("PYTHONPATH", pythonPath);

			Process p = pb.start();
			BufferedReader bri = new BufferedReader((new InputStreamReader(p.getInputStream())));
			while ((line = bri.readLine()) != null) {
				tm.add(line);
			}
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
			Runtime.getRuntime().exec("python");
			return true;
		} catch (IOException ex) {
			return false;
		}
	}

	private static boolean isPython3Supported() {
		try {
			Runtime.getRuntime().exec("python3");
			return true;
		} catch (IOException ex) {
			return false;
		}
	}

	private static void runTestMethods(String pythonArg, List<Map.Entry<String, String>> allTests) throws Exception {
		String utilsPy = findUtilsFile();
		String subprocessPy = findSubproessTestCaseFile();
		if (verbose) {
			System.out.println("utils: " + utilsPy);
			System.out.println("subprocess:" + subprocessPy);
		}

		int i = 0;
		for(Map.Entry<String, String> ent : allTests) {
			String[] args = new String[]{pythonArg, ent.getKey(), utilsPy, subprocessPy, "-", "--run-mode", "--mvn-mode", ent.getValue()};
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
