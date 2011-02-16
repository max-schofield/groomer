import sys; sys.path.append('/Trophia/Tanga/Groomer/')

from tangagroomer.dataset import Dataset

dataset = Dataset(
	client = 'New Zealand Ministry of Fisheries',
	project = 'INS2009/03 : Characterization of FMA 2 fisheries',

	begin = '01 Oct 1989',
	end = '01 Oct 2009',
	species = ['BNS'],
	fishstocks = {
		'BNS':['BNS2'],
	},
	statareas = ['%03d'%stat for stat in range(11,20)+range(201,207)],
	methods = [],
	targets = []
)
#dataset.prerequest()
#dataset.request()
dataset.load()
dataset.groom()
dataset.allocate()
dataset.augment()
dataset.simplify()
dataset.summarize()