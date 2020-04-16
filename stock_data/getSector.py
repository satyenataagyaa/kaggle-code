from datetime import datetime
from concurrent import futures

import pandas as pd
from pandas import DataFrame
import pandas_datareader.data as web

def download_sector(sector):
	""" try to query the iex for a sector, if failed note with print """
	try:
		print(sector)
		stock_df = web.DataReader(sector,'yahoo', start_time, now_time)
		stock_df['Name'] = sector
		output_name = sector + '_data.csv'
		stock_df.to_csv(output_name)
	except:
		bad_names.append(sector)
		print('bad: %s' % (sector))

if __name__ == '__main__':

	""" set the download window """
	now_time = datetime(2019, 12, 31)
	start_time = datetime(2015, 1, 1)

	""" list of s_anp_p sectors """
	sector = ['XLK','XLV','XLI','XLU','XLF','XLP','XLB','XLY','XLE']

	bad_names =[] #to keep track of failed queries

	"""here we use the concurrent.futures module's ThreadPoolExecutor
		to speed up the downloads buy doing them in parallel
		as opposed to sequentially """

	#set the maximum thread number
	max_workers = 50

	workers = min(max_workers, len(sector)) #in case a smaller number of stocks than threads was passed in
	with futures.ThreadPoolExecutor(workers) as executor:
		res = executor.map(download_sector, sector)


	""" Save failed queries to a text file to retry """
	if len(bad_names) > 0:
		with open('failed_queries.txt','w') as outfile:
			for name in bad_names:
				outfile.write(name+'\n')

	#timing:
	finish_time = datetime.now()
	duration = finish_time - now_time
	minutes, seconds = divmod(duration.seconds, 60)
	print('getSector_threaded.py')
	print(f'The threaded script took {minutes} minutes and {seconds} seconds to run.')
	#The threaded script took 0 minutes and 31 seconds to run.
