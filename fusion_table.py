import json
import sys
import requests
import httplib2
import ratelimit

from urllib.parse import quote

import secrets
import praw
from datetime import datetime 
import time

from ratelimit import limits, sleep_and_retry


# gets require API key access
# posts require service account

# get a service account key
# share your table to the email associated with it

from google.oauth2 import service_account
import googleapiclient.discovery

SCOPES = ['https://www.googleapis.com/auth/fusiontables']
SERVICE_ACCOUNT_FILE = 'your_json_file.json'

credentials = service_account.Credentials.from_service_account_file(
		SERVICE_ACCOUNT_FILE, scopes=SCOPES)
fusiontables = googleapiclient.discovery.build('fusiontables', 'v2', credentials=credentials)


def is_an_image(url):
	if url[-3:] in ['jpg', 'png'] or url[-4:] in ['jpeg']:
		return True
	return False

# oauth
# Create a google api project for fusion table api
# Go to the credentials table and click create credentials for API key
TABLE_ID = "your_secret_code"
API_KEY = "your_secret_code"

ONE_MINUTE = 60

API = 'API'
SERVICE_ACCOUNT = 'SERVICE_ACCOUNT'

@sleep_and_retry
@limits(calls=25, period=ONE_MINUTE)
def call_api(type, query):
	if type == API:
		return try_request(query)
	if type == SERVICE_ACCOUNT:
		return fusiontables.query().sql(sql=query).execute()

                           
def try_request(url):
	#tries URL and returns JSON dict
	print(url)
	try:
		r = requests.post(url)
		#ensure a 200 is returned, 400 is error
		if r.status_code != 200:
			print('Recieved a {}'.format(r.status_code))
			print(r.reason)
			print(r.text)
	except:
		print('Some error from `requests`')

	return json.loads(r.text)

 
def is_id_in_db(sub):
	query = "SELECT id FROM {} where id='{}'".format(TABLE_ID, sub.id) # do i need to escape that lol
	url = "https://www.googleapis.com/fusiontables/v2/query?sql={}&key={}".format(query , API_KEY)

	j = call_api(api, url)

	if "rows" in j:
		print('is_id_in_db: found the request')
		return True
	else:
		print('is_id_in_db: did not find the request')
		return False




def add_sub_to_db(sub):
	query = "INSERT INTO {} (id, title, url, subreddit, subreddit_id, created_utc, discovered) VALUES ('{}', '{}', '{}', '{}', '{}', {}, {})".format(
			TABLE_ID,
			sub.id,
			'disabled', #quote(sub.title),
			sub.url,
			str(sub.subreddit),
			sub.subreddit_id,
			int(sub.created_utc),
			int(time.time())
			)
		 # do i need to escape that lol

	print(query)
	response = call_api(SERVICE_ACCOUNT, query)
	print(response)


def update_lastseen(sub):
	query = "UPDATE {} SET lastseen={} WHERE id='{}'".format(TABLE_ID, int(time.time()), sub.id)
	print(query)

	response = call_api(SERVICE_ACCOUNT, query)
	print(response)

	if response['rows'] != [['0']]:
		print('update_lastseen: found the request')
		return True
	else:
		print('update_lastseen: did not find the request')
		return False

def human_time(time):
	hours = int(time)
	minutes = (time*60) % 60
	seconds = (time*3600) % 60
	return "%d hr %02d mins %02d sec" % (hours, minutes, seconds)

def print_exception():
	exc_type, exc_obj, tb = sys.exc_info()
	f = tb.tb_frame
	lineno = tb.tb_lineno
	filename = f.f_code.co_filename
	print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))


while True:
 
	try:
		r = praw.Reddit(
				client_id = secrets.client_id,
				client_secret = secrets.client_secret,
				password = secrets.password,
				user_agent = 'PotableWater front page scrapper',
				username = secrets.username
				)

		submissions = r.subreddit('all').hot(limit=100);
		
		total_sub_count = 0
		new_sub_count = 0

		print('')

		for submission in submissions:
			total_sub_count += 1
			print(total_sub_count)

			if submission.is_self or not is_an_image(submission.url):
				continue

			if not update_lastseen(submission): #if id is not in db then add data
				add_sub_to_db(submission)
				new_sub_count += 1


		# print('Avg time to get to front page: {}'.format(human_time(get_avg_created_to_front())))
		# print('Avg life span on front page: {}'.format(human_time(get_avg_front_life())))
		
	except Exception as e:
		print(e)
		pass
		#time.sleep(5)
		#time.sleep(60*60) #sleep an hour
	
	print('{}: Found {} of {} new posts, sleeping'.format(str(datetime.now()), new_sub_count, total_sub_count))
	time.sleep(600)