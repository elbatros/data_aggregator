import pandas as pd

VALUE_KEY = 'value\r' #TODO remove \r
rollup_target = None#['y','m','d']


class DataRoller:
	def __init__(self, input_file, rollup_target, value_key = VALUE_KEY, delimiter = '\t', lineterminator = '\n'):
		self.__input_file = input_file

		self.__rollup_target = rollup_target 		#TODO handle no rollup_target passed

		self.__value_key = value_key 				#TODO: Can the value key not be value
		self.__input_file_delimiter = delimiter
		self.__input_file_lineterminator = lineterminator
		self.__data = None
		self.__result = []

	def __load_file(self):
		self.__data = pd.read_csv(self.__input_file, delimiter=self.__input_file_delimiter,\
			lineterminator=self.__input_file_lineterminator, header='infer')
		#TODO: Handle empty file

	def __cleanup(self):

		# Identify columns that we don't need
		extra_cols = list(set(list(self.__data.columns))^set(self.__rollup_target)) 
		
		# If the value key is not present. We may have parsed incorrectly. We can't go any further, so quit
		if self.__value_key not in extra_cols: 
			print ('ERROR: Unable to find "value" column in provided data') 
			exit(1)

		# Delete extra columns
		extra_cols.remove(self.__value_key) # Don't delete the value column
		for col in extra_cols:
			self.__data = self.__data.drop(col, 1)

	def run(self):
		self.__load_file()

		#If no rollup target identified, use all columns
		if self.__rollup_target == None:
			self.__rollup_target = list(self.__data.columns)
			self.__rollup_target.remove(self.__value_key)
			#TODO: Do we need to worry about order?

		self.__cleanup()


		#Create groups to iterate over
		for idx in range(len(self.__rollup_target) + 1):
			target_grp_cols = self.__rollup_target[:len(self.__rollup_target)-idx]

			rol = {}
			if len(target_grp_cols) == 0:
				rol[self.__value_key] = self.__data.sum()[self.__value_key]
				print (rol)
				self.__result.append(rol)
			else:
				# Group by target_grp_cols and calculate sum
				grouped_df = self.__data.groupby(target_grp_cols).sum()
				for grp_idx, row in grouped_df.iterrows():
					if len(target_grp_cols) == 1: #Special case when only grouped by a single column. grp_idx is not a tuple
						rol = {target_grp_cols[0]: grp_idx} 
					else:
						rol = {target_grp_cols[i]: grp_idx[i] for i in range(len(target_grp_cols))}

					rol[self.__value_key] = row[self.__value_key]
					print (rol)
					self.__result.append(rol)


def main():
	data_roller = DataRoller('input01.txt', rollup_target)
	data_roller.run()
if __name__ == '__main__':
	main()
