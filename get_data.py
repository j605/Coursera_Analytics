import urllib.request, urllib.error, urllib.parse
import json
import pandas as pd


SHAREDCOUNT_API_KEY = 'XXXXXXXXXXXX'

def map_ids_names(ids_array, df, object_name):
	names_array = []
	for object_id in ids_array:
		try:
			names_array.append(df.loc[object_id][object_name])
		except:
			continue
	return names_array

def get_social_metrics(url, api_key):
	sharedcount_response = urllib.request.urlopen('https://free.sharedcount.com/?url=' + url + '&apikey=' + api_key).read()
	return json.loads(sharedcount_response.decode('utf-8'))

def main():

	print('Getting Courses Data')

	courses_response = urllib.request.urlopen('https://api.coursera.org/api/catalog.v1/courses?fields=shortName,name,language&includes=universities,categories').read()
	courses_data = json.loads(courses_response.decode('utf-8'))
	courses_data = courses_data['elements']

	universities_response = urllib.request.urlopen('https://api.coursera.org/api/catalog.v1/universities?fields=name,locationCountry').read()
	universities_data = json.loads(universities_response.decode('utf-8'))
	universities_data = universities_data['elements']

	categories_response = urllib.request.urlopen('https://api.coursera.org/api/catalog.v1/categories').read()
	categories_data = json.loads(categories_response.decode('utf-8'))
	categories_data = categories_data['elements']

	#2. Structuring the Data
	print('Structuring the Data')

	courses_df = pd.DataFrame()

	courses_df['course_name'] = [course_data['name'] for course_data in courses_data]
	courses_df['course_language'] = [course_data['language'] for course_data in courses_data]
	courses_df['course_short_name'] = [course_data['shortName'] for course_data in courses_data]
	courses_df['categories'] = [course_data['links']['categories'] if 'categories' in course_data['links'] else [] for course_data in courses_data]
	courses_df['universities'] = [course_data['links']['universities'] if 'universities' in course_data['links'] else [] for course_data in courses_data]

	universities_df = pd.DataFrame()
	universities_df['university_id'] = [university_data['id'] for university_data in universities_data]
	universities_df['university_name'] = [university_data['name'] for university_data in universities_data]
	universities_df['university_location_country'] = [university_data['locationCountry'] for university_data in universities_data]
	universities_df = universities_df.set_index('university_id')

	categories_df = pd.DataFrame()
	categories_df['category_id'] = [category_data['id'] for category_data in categories_data]
	categories_df['category_name'] = [category_data['name'] for category_data in categories_data]
	categories_df = categories_df.set_index('category_id')

	courses_df['categories_name'] = courses_df.apply(lambda row: map_ids_names(row['categories'], categories_df, 'category_name'), axis=1)
	courses_df['universities_name'] = courses_df.apply(lambda row: map_ids_names(row['universities'], universities_df, 'university_name'), axis=1)

	#Getting Social Shares Data
	print('Getting Social Shares Data')
	courses_df['course_url'] = 'https://www.coursera.org/course/' + courses_df['course_short_name']
	courses_df['sharedcount_metrics'] = [get_social_metrics(course_url, SHAREDCOUNT_API_KEY) for course_url in courses_df['course_url']]

	courses_df['twitter_count'] = [sharedcount['Twitter'] for sharedcount in courses_df['sharedcount_metrics']]
	courses_df['linkedin_count'] = [sharedcount['LinkedIn'] for sharedcount in courses_df['sharedcount_metrics']]
	courses_df['facebook_count'] = [sharedcount['Facebook']['total_count'] for sharedcount in courses_df['sharedcount_metrics']]

	#Saving the Data to a tsv File
	print('Saving the Data')
	courses_df.to_csv('coursera_with_sharedcount.tsv', sep='\t', encoding='utf-8')

if __name__ == "__main__":
	main()

