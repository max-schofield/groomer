'''
A silly example of defining a dataset mainly for checking code at least runs
The `extract` subdir contains some empty file with the right names and headers.
'''
import sys
sys.path.append('../..')

from dataset import Dataset

dataset = Dataset(
	name = 'bbf9',
	client = 'New Zealand Ministry of Primary Industries',
	project = 'BFF2015-01 : Characterization of big bluefish in BBF 9',

	begin = '01 Oct 1989',
	end = '01 Oct 2014',
	species = ['BFF'],
	fishstocks = {
		'BFF':['BFF9'],
	},
	statareas = ['091','092','099'],
	methods = ['BT','BLL'],
	targets = ['BBF','BNS','ALB']
)

dataset.request()
dataset.load()
dataset.groom()
dataset.allocate()
dataset.augment()
dataset.simplify()
