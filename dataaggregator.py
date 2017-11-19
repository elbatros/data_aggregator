import pandas as pd

VALUE_KEY = 'value\r'
rollup_target = ['y','m','d']


class DataRoller:
	def __init__(self, input_file, rollup_target, value_key = VALUE_KEY, delimiter = 't', lineterminator = '\n'):
		self.__input_file = input_file
		self.__rollup_target = rollup_target
		self.__value_key = value_key
		self.__input_file_delimiter = delimiter
		self.__input_file_lineterminator = lineterminator
		self.__data = None
		self.__result = None

	def __load_file(self):
		self.__data = pd.read_csv(self.__input_file, delimiter=self.__input_file_delimiter,\
			lineterminator=self.__input_file_lineterminator, header='infer')

	def run(self):
		self.__load_file()
		#TODO
		None


def main():
	data_roller = DataRoller('input01.txt', rollup_target)
if __name__ == '__main__':
	main()
