# ###############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
################################################################################


import unittest
import subprocess
import inspect
import sys
import os


class SubprocessTestCase(unittest.TestCase):
	"""
	Run test cases in a separate process, keeping the unittest scheme of running method starting with test_.
	"""

	# Indicating if run mode is true. In run mode the test code is run, as opposed to non run mode when the
	# process for running the test is created
	running_mode = False

	# The path to the external runner to use to run the test. This can be a list where the runner should
	# be the first item followed by arguments.
	external_runner = []

	# Controlling the verbosity level of the SubprocessTestCase
	verbose = False

	class_file = None

	@staticmethod
	def is_run_mode():
		return SubprocessTestCase.running_mode

	@classmethod
	def set_run_mode(cls):
		SubprocessTestCase.running_mode = True

	@classmethod
	def set_class_file(cls, file_path):
		SubprocessTestCase.class_file = file_path

	@classmethod
	def set_external_runner(cls, external_runner_list):
		SubprocessTestCase.external_runner = external_runner_list

	@classmethod
	def set_verbose(cls):
		SubprocessTestCase.verbose = True

	def __init__(self, methodName='runTest'):
		super(SubprocessTestCase, self).__init__(methodName=methodName)

		if not self.is_run_mode():

			if SubprocessTestCase.verbose:
				print("Running in top mode - renaming methods")

			list_of_attr = dir(self)

			for attr in list_of_attr:
				if attr.startswith("test_"):
					new_attr = "__subprocess_method_" + attr
					setattr(self, new_attr, attr)

					# Setting the test name self.test_xx to be the wrapper function for this test
					setattr(self, attr, self._generate_test_wrapper(attr))

	def _generate_test_wrapper(self, test_name):
		"""
        Generating a function that will run the wrapper code and call the test function given as test_name
        """
		def test_wrapper(wrapped_test_name=test_name):
			"""
            Running the test wrapper - note that the name of the test to run is passed as the default argument value
            """
			self._run_method_externaly(wrapped_test_name)
		return test_wrapper

	def _run_method_externaly(self, method_name):
		cmd = []
		cmd.extend(self.external_runner)
		cmd.append("{}.{}".format(self.__class__.__name__, method_name))
		if SubprocessTestCase.verbose:
			print("running: {}".format(cmd))

		p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
		(out, err) = p.communicate()

		retcode = p.wait()
		if SubprocessTestCase.verbose:
			print("retcode = {}".format(retcode))
		if retcode == 0:
			pass
		else:
			self.assertTrue(False, "external process failed\noutput = {}\nstderr = {}".format(out.decode("utf-8"), err.decode("utf-8")))

class MyTest(SubprocessTestCase):
	"""
	A basic tester to the SubprocessTestCase
	"""

	def test_feature_1(self):
		print("Running test_feature_1 pid = {}".format(os.getpid()))

	def test_feature_2(self):
		print("Running test_feature_2 pid = {}".format(os.getpid()))
		self.assertTrue(False, "must fail test")

	def test_feature_3(self):
		print("Running test_feature_3 pid = {}".format(os.getpid()))


import argparse

if __name__ == '__main__':

	parser = argparse.ArgumentParser()
	parser.add_argument('--run-mode', dest="run_mode", action="store_true", default=False,
						help="internal flag of this test - do not use unless you know what you are doing." +
							 "This flag indicate that the script is called the second time to actually run the tests")
	parser.add_argument('-v', dest="verbose", action="store_true", default=False,
						help="run tests with increased verbosity")
	# Rest of the arguments are kept to be passed to the unit tests framework
	parser.add_argument('unittest_args', nargs=argparse.REMAINDER)

	options = parser.parse_args()

	# The arguments are parsed at the beginning of the file since the sys.path is to be fixed
	if options.verbose:
		print("Testing SubprocessTestCase using MyTest class")
		print("unittests args: {}".format(options.unittest_args))

	if options.run_mode:
		if options.verbose:
			print("== run mode ==")
		SubprocessTestCase.set_run_mode()
	else:
		MyTest.set_class_file(__file__)
		test_py_path = __file__
		test_py_path = os.path.join(os.getcwd(), test_py_path)
		MyTest.set_external_runner(["python3", test_py_path, "--run-mode"])

	sys.argv[1:] = options.unittest_args

	if options.verbose and not options.run_mode:
		print("Arguments [{}]".format(sys.argv))

	vv = 1
	if options.verbose:
		MyTest.set_verbose()
		vv = 2

	unittest.main(verbosity=vv)
