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
import argparse
import sys
import os
import struct
from uuid import uuid4


options = None
def parse_args():
	parser = argparse.ArgumentParser()


	parser.add_argument('--pyfl', dest="pyflink_path", metavar="PATH-TO-PYFLINK", default=None,
						help="path to the pyflink2.sh or pyflink3.sh to use for running the test")

	parser.add_argument('--python-path', dest="python_path", metavar="PATH", default=None,
						help="python search path to use")

	parser.add_argument('--run-mode', dest="run_mode", action="store_true", default=False,
						help="Intended for internal use only (DO NOT USE UNLESS YOU KNOW WAHT YOU ARE DOING) " +
							 "This flag indicate that the script is called the second time to actually run the tests")
	parser.add_argument("-ls", "--list-tests", dest="list_tests", action="store_true", default=False,
	                    help="List the tests in the file.")

	parser.add_argument('-v', dest="verbose", action="store_true", default=False,
						help="run tests with increased verbosity")
	# Rest of the arguments are kept to be passed to the unit tests framework
	parser.add_argument('unittest_args', nargs=argparse.REMAINDER)

	local_options = parser.parse_args()
	return local_options


# This code should run before any other piece of code using the flink python stuff
options = parse_args()
if options.verbose:
	print("In first main section")


from utils.subprocess_test import SubprocessTestCase
from utils.utils import Id, Verify

# Flink python imports
from flink.plan.Environment import get_environment
from flink.functions.MapFunction import MapFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.FilterFunction import FilterFunction
from flink.functions.MapPartitionFunction import MapPartitionFunction
from flink.functions.ReduceFunction import ReduceFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.plan.Constants import Order, WriteMode


class FlinkPythonBatchTests(SubprocessTestCase):
	"""
	Testing the Flink python batch API
	"""
	def test_from_list(self):
		env = get_environment()


		l = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
		d1 = env.from_list(l)
		d1.map(Verify(l, "FromList")).output()

		test_str_prefix = "prefix-string-for-test-"
		l2 = []
		for i in range(1, 10000):
			l2.append(test_str_prefix + ":" + str(i))

		class Mapper(MapFunction):
			idx = 0
			def map(self, value):
				Mapper.idx += 1
				if value.startswith(test_str_prefix):
					parts = value.split(":")
					if len(parts) != 2:
						raise Exception("the string should be makde of 2 parts: prefix and number")
					if (int(parts[1]) != Mapper.idx):
						raise Exception("The idx of string is not as expected got[{}] expected [{}]".format(parts[1], Mapper.idx))
					return value

		class FlatMap(FlatMapFunction):
			def flat_map(self, value, collector):
				pass

		d2 = env.from_list(l2)
		d2.map(Mapper()).flat_map(FlatMap()).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_sequence(self):
		env = get_environment()
		d1 = env.generate_sequence(0, 999)
		d1.map(Id()).map_partition(Verify(range(1000), "Sequence")).output()
		env.set_parallelism(1)
		env.execute(local=True)

	def test_zip_with_index(self):

		env = get_environment()
		ds = env.generate_sequence(0, 999)


		# Generate Sequence Source
		ds.map(Id()).map_partition(Verify(range(1000), "Sequence")).output()

		# Zip with Index
		# check that IDs (first field of each element) are consecutive
		ds.zip_with_index() \
			.map(lambda x: x[0]) \
			.map_partition(Verify(range(1000), "ZipWithIndex")).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_types(self):
		env = get_environment()

		# Types
		env.from_elements(bytearray(b"hello"), bytearray(b"world")) \
			.map(Id()).map_partition(Verify([bytearray(b"hello"), bytearray(b"world")], "Byte")).output()

		env.from_elements(1, 2, 3, 4, 5) \
			.map(Id()).map_partition(Verify([1, 2, 3, 4, 5], "Int")).output()

		env.from_elements(True, True, False) \
			.map(Id()).map_partition(Verify([True, True, False], "Bool")).output()

		env.from_elements(1.4, 1.7, 12312.23) \
			.map(Id()).map_partition(Verify([1.4, 1.7, 12312.23], "Float")).output()

		env.from_elements("hello", "world") \
			.map(Id()).map_partition(Verify(["hello", "world"], "String")).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_custom_serialization(self):

		# Custom Serialization
		class Ext(MapPartitionFunction):
			def map_partition(self, iterator, collector):
				for value in iterator:
					collector.collect(value.value)

		class MyObj(object):
			def __init__(self, i):
				self.value = i

		class MySerializer(object):
			def serialize(self, value):
				return struct.pack(">i", value.value)

		class MyDeserializer(object):
			def deserialize(self, read):
				i = struct.unpack(">i", read(4))[0]
				return MyObj(i)

		env = get_environment()

		env.register_type(MyObj, MySerializer(), MyDeserializer())

		env.from_elements(MyObj(2), MyObj(4)) \
			.map(Id()).map_partition(Ext()) \
			.map_partition(Verify([2, 4], "CustomTypeSerialization")).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_map(self):

		class Mapper(MapFunction):
			def map(self, value):
				return value * value

		env = get_environment()

		d1 = env.from_elements(1, 6, 12)

		d1 \
			.map((lambda x: x * x)).map(Mapper()) \
			.map_partition(Verify([1, 1296, 20736], "Map")).output()


		env.set_parallelism(1)
		env.execute(local=True)

	def test_flat_map(self):

		class FlatMap(FlatMapFunction):
			def flat_map(self, value, collector):
				collector.collect(value)
				collector.collect(value * 2)

		env = get_environment()
		d1 = env.from_elements(1, 6, 12)

		d1 \
			.flat_map(FlatMap()).flat_map(FlatMap()) \
			.map_partition(Verify([1, 2, 2, 4, 6, 12, 12, 24, 12, 24, 24, 48], "FlatMap")).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_map_partition(self):
		class MapPartition(MapPartitionFunction):
			def map_partition(self, iterator, collector):
				for value in iterator:
					collector.collect(value * 2)

		env = get_environment()
		d1 = env.from_elements(1, 6, 12)

		d1 \
			.map_partition(MapPartition()) \
			.map_partition(Verify([2, 12, 24], "MapPartition")).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_filter(self):

		class Filter(FilterFunction):
			def __init__(self, limit):
				super(Filter, self).__init__()
				self.limit = limit

			def filter(self, value):
				return value > self.limit

		env = get_environment()
		d1 = env.from_elements(1, 6, 12)
		d1 \
			.filter(Filter(5)).filter(Filter(8)) \
			.map_partition(Verify([12], "Filter")).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_reduce(self):
		class Reduce(ReduceFunction):
			def reduce(self, value1, value2):
				return value1 + value2

		class Reduce2(ReduceFunction):
			def reduce(self, value1, value2):
				return (value1[0] + value2[0], value1[1] + value2[1], value1[2], value1[3] or value2[3])

		env = get_environment()
		d1 = env.from_elements(1, 6, 12)

		d4 = env.from_elements((1, 0.5, "hello", True),
							   (1, 0.4, "hello", False),
							   (1, 0.5, "hello", True),
							   (2, 0.4, "world", False))

		d1 \
			.reduce(Reduce()) \
			.map_partition(Verify([19], "AllReduce")).output()

		d4 \
			.group_by(2).reduce(Reduce2()) \
			.map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "GroupedReduce")).output()

		env.set_parallelism(1)
		env.execute(local=True)

	def test_group_reduce(self):
		class GroupReduce(GroupReduceFunction):
			def reduce(self, iterator, collector):
				if iterator.has_next():
					i, f, s, b = iterator.next()
					for value in iterator:
						i += value[0]
						f += value[1]
						b |= value[3]
					collector.collect((i, f, s, b))

		class GroupReduce2(GroupReduceFunction):
			def reduce(self, iterator, collector):
				for value in iterator:
					collector.collect(value)

		env = get_environment()

		d4 = env.from_elements((1, 0.5, "hello", True),
							   (1, 0.4, "hello", False),
							   (1, 0.5, "hello", True),
							   (2, 0.4, "world", False))

		d5 = env.from_elements((1, 2.4), (1, 3.7), (1, 0.4), (1, 5.4))

		d4 \
			.reduce_group(GroupReduce2()) \
			.map_partition(Verify(
			[(1, 0.5, "hello", True), (1, 0.4, "hello", False), (1, 0.5, "hello", True), (2, 0.4, "world", False)],
			"AllGroupReduce")).output()
		d4 \
			.group_by(lambda x: x[2]).reduce_group(GroupReduce(), combinable=False) \
			.map_partition(
			Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "GroupReduceWithKeySelector")).output()
		d4 \
			.group_by(2).reduce_group(GroupReduce()) \
			.map_partition(Verify([(3, 1.4, "hello", True), (2, 0.4, "world", False)], "GroupReduce")).output()
		d5 \
			.group_by(0).sort_group(1, Order.ASCENDING).reduce_group(GroupReduce2(), combinable=True) \
			.map_partition(Verify([(1, 0.4), (1, 2.4), (1, 3.7), (1, 5.4)], "SortedGroupReduceAsc")).output()
		d5 \
			.group_by(0).sort_group(1, Order.DESCENDING).reduce_group(GroupReduce2(), combinable=True) \
			.map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceDes")).output()
		d5 \
			.group_by(lambda x: x[0]).sort_group(1, Order.DESCENDING).reduce_group(GroupReduce2(), combinable=True) \
			.map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceKeySelG")).output()
		d5 \
			.group_by(0).sort_group(lambda x: x[1], Order.DESCENDING).reduce_group(GroupReduce2(), combinable=True) \
			.map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceKeySelS")).output()
		d5 \
			.group_by(lambda x: x[0]).sort_group(lambda x: x[1], Order.DESCENDING).reduce_group(GroupReduce2(),
																								combinable=True) \
			.map_partition(Verify([(1, 5.4), (1, 3.7), (1, 2.4), (1, 0.4)], "SortedGroupReduceKeySelGS")).output()

		# Execution
		env.set_parallelism(1)

		env.execute(local=True)

	def test_write_csv(self):
		env = get_environment()

		d1 = env.from_elements((1, 2, 3), (4, 5, 6))
		out = "/tmp/flink_python_" + str(uuid4())
		d1.write_csv(out, line_delimiter="\n", field_delimiter="|", write_mode=WriteMode.OVERWRITE)

		env.set_parallelism(1)
		env.execute(local=True)

import unittest

def get_test_cases(klass, from_file=None):
	subclasses = set()
	work = [klass]
	while work:
		parent = work.pop()
		for child in parent.__subclasses__():
			take_class = False
			if child not in subclasses:
				if from_file:
					child_file = inspect.getfile(child)
					if child_file == from_file:
						take_class = True
				else:
					take_class = True

				if take_class:
					subclasses.add(child)
					work.append(child)


	return subclasses


import inspect
def get_test_methods(klass):
	members = inspect.getmembers(klass, predicate=inspect.ismethod)
	test_methods = []
	for m in members:
		if m[0].startswith("test_"):
			test_methods.append(m[0])

	return test_methods


# The second part of main that will drive the unittests
if __name__ == "__main__":
	"""
	Running Flink Python Batch tests
	"""

	if options.list_tests:
		test_cases = get_test_cases(SubprocessTestCase, from_file=__file__)

		for tc in test_cases:
			test_methods = get_test_methods(tc)
			for tm in test_methods:
				print("{}.{}".format(tc.__name__, tm))

		sys.exit(0)

	# The arguments are parsed at the beginning of the file since the sys.path is to be fixed
	if options.verbose:
		print("Testing Flink-Python batch API")
		print("unittests args: {}".format(options.unittest_args))

	if options.run_mode:
		if options.verbose:
			print("== run mode ==")
		SubprocessTestCase.set_run_mode()
	else:
		FlinkPythonBatchTests.set_class_file(__file__)
		test_py_path = __file__
		test_py_path = os.path.join(os.getcwd(), test_py_path)

		python_path_str = ":".join(sys.path)
		FlinkPythonBatchTests.set_external_runner([options.pyflink_path, test_py_path, "-", "--run-mode", "--python-path", python_path_str])

	sys.argv[1:] = options.unittest_args

	if options.verbose and not options.run_mode:
		print("Using the [{}] to run python flink jobs".format(options.pyflink_path))
		# Setting the remaining arguments so the unittest module can use
		print("Arguments [{}]".format(sys.argv))

	vv = 1
	if options.verbose:
		FlinkPythonBatchTests.set_verbose()
		vv = 2

	unittest.main(verbosity=vv)

