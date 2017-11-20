#!/usr/bin/env python
"""
	@about: Loads a file and performs rollup operation
	@author: Hashem.Raslan@gmail.com
"""

import pandas as pd
import csv
import sys
import copy

class DataAggregator:
	"""DataAggregator class that will contain multiple aggregation functionalities

    Class only supports RollUp functionality

    Args:
        input_file (:obj:'file'): Input file with data to parse. Last column must be 'value' (unless overridden).
        value_key (:obj:`str`, optional): An override for the value column name.
        delimiter (:obj:`str`, optional): An overide for the input file delimiter
        lineterminator (:obj:`str`, optional): An overide for the input file lineterminator
    """
	def __init__(self, input_file, value_key = 'value', \
				 delimiter = '\t', lineterminator = '\n'):
		
		self.__input_file = input_file
		self.__value_key = value_key 				
		self.__input_file_delimiter = delimiter
		self.__input_file_lineterminator = lineterminator
		self.__result = []

	def __load_file(self):
		"""Loads input_file

		Args:
			None
		"""
		try:
			self.__data = pd.read_csv(self.__input_file, delimiter=self.__input_file_delimiter,\
				lineterminator=self.__input_file_lineterminator, header='infer')
			return

		except NameError:
			print ("File Not Found. ({})".format(self.__input_file)) 
		except Exception, e:
			print ("Error in reading ({})".format(self.__input_file))
			print (e)
		
		exit(1)

	def __remove_cols(self, target_cols):
		"""Removes specific columns from loaded data
			Returns a shallow copy of the modified data

		Args:
			target_cols (list): Columns to be deleted.
		"""
		buff_data = copy.copy(self.__data)

		for col in target_cols:
			buff_data = buff_data.drop(col, 1)
		return buff_data


	def __create_aggregated_groups(self, data, aggregation_groups, aggregation_type):
		"""Aggregates data based on aggregation groups and aggregation type
			Returns a list of dictionary of aggregated values

		Args:
			data (data_frame object): Data to be aggregated.
			aggregation_groups (list): Group indexes
			aggregation_type (str): Data frame aggregation types ('min', 'max', 'sum', 'mean')
		"""

		results = []
		#Create groups to iterate over
		for idx in range(len(aggregation_groups) + 1):
			target_grp_cols = aggregation_groups[:len(aggregation_groups)-idx]

			rol = {}
			if len(target_grp_cols) == 0:
				rol[self.__value_key] = data.agg(aggregation_type)[self.__value_key]
				results.append(rol)
			else:
				# Group by target_grp_cols and calculate sum
				grouped_df = data.groupby(target_grp_cols).agg(aggregation_type)
				for grp_idx, row in grouped_df.iterrows():
					if len(target_grp_cols) == 1: #Special case when only grouped by a single column. grp_idx is not a tuple
						rol = {target_grp_cols[0]: grp_idx} 
					else:
						rol = {target_grp_cols[i]: grp_idx[i] for i in range(len(target_grp_cols))}

					rol[self.__value_key] = row[self.__value_key]
					results.append(rol)
		return results

	def rollup(self, rollup_target = []):
		""" Rolls up input data based on rollup_target

		Args:
			rollup_target(list): Columns to be rolled on

		"""
		self.__load_file()

		#If no rollup target identified, use all columns
		if rollup_target == []:
			rollup_target = list(self.__data.columns)
			rollup_target.remove(self.__value_key)


		# Identify columns that we don't need
		extra_cols = list(set(list(self.__data.columns))^set(rollup_target)) 
		
		# If the value key is not present. We may have parsed incorrectly. We can't go any further, so quit
		if self.__value_key not in extra_cols: 
			print ('ERROR: Unable to find "value" column in provided data') 
			exit(1)

		# Delete extra columns
		extra_cols.remove(self.__value_key) # Don't delete the value column
		clean_data = self.__remove_cols(extra_cols)

		# Run
		rolled_data = self.__create_aggregated_groups(clean_data, rollup_target, 'sum')
		keys = rollup_target
		keys.append(self.__value_key)

		#Return RolledData object
		return(RolledData(rolled_data, keys))



class RolledData:
	""" Class to contain rolled data

	Args:
		data (list): List of dictionary items
		keys(list): Columns names
	"""
	def __init__(self, data, keys):
		self.__data = data
		self.__keys = keys


	def save(self, file_name, delimiter = '\t', lineterminator = '\n'):
		"""Saves rolled data to file

		Args
			file_name (str): Output file
			delimiter (:obj:`str`, optional): An overide for the input file delimiter
        	lineterminator (:obj:`str`, optional): An overide for the input file lineterminator
		"""
		with open(file_name, 'wb') as output_file:
			dict_writer = csv.DictWriter(output_file, self.__keys, \
				delimiter= delimiter, \
				lineterminator= lineterminator)

			dict_writer.writeheader()
			dict_writer.writerows(self.__data)


def main(input_file,  rollup_target):
	data_aggregator = DataAggregator(input_file)
	rolled_data = data_aggregator.rollup(rollup_target)
	rolled_data.save('out.txt')

if __name__ == '__main__':
	input_file = sys.stdin
	rollup_target = sys.argv[1:]

	main(input_file,  rollup_target)
