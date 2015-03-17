#!/usr/bin/env python
from nltk.stem.lancaster import LancasterStemmer
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
import sys
import nltk
import string
#from sentiment_analysis import get_sentiment
import re
import glob

#Python has a very good libraries for NLP Processing
porterstemmer = PorterStemmer()
stemming = LancasterStemmer()

# On Local /home/saiyan/myworks/dsba/BigData/project/data/
# Make a list of Producers
producer = open('film_production_companies.txt','r')
producers_list =[]
for lines in producer.readlines():
	producers_list.append(lines.decode('ascii','ignore').strip().lower())
producer.close()

# Make a list of Producers
directors = open('notable_directors.txt','r')
directors_list =[]
for lines in directors.readlines():
	directors_list.append(lines.decode('ascii','ignore').strip().lower())

directors.close()

# Make a list of Producers
actors = open('notable_actors.txt','r')
actors_list =[]
for lines in actors.readlines():
	actors_list.append(lines.decode('ascii','ignore').strip().lower())

actors.close()

# Doing Sentiment Analysis
# Make list for positive Words
positive=open('positive_words.txt','r')
positiveWords_list =[]
for lines in positive.readlines():
	positiveWords_list.append(lines.decode('ascii','ignore').strip().lower())
	
positive.close()	
# Make a list of Negative Words

negative=open('negative_words.txt','r')
negativeWords_list=[]
for lines in negative.readlines():
	negativeWords_list.append(lines.decode('ascii','ignore').strip().lower())
negative.close()



def list_onexists(name_list,baseline):
	itemset_in_wiki = []
	try:
		for names in name_list:
			name = str(names).split()
			fname = name[0]
			lname =""
			if len(name) >1:
				lname = porterstemmer.stem(name[1])	
			if porterstemmer.stem(fname) in baseline :
				indexofbaseline = baseline.index(porterstemmer.stem(fname))
				
				item_lastname = baseline[indexofbaseline+1]
				if lname == "":
					itemset_in_wiki.append(names)
				if lname == item_lastname:
					itemset_in_wiki.append(names)

	except Exception,e:
		
		pass	
	return itemset_in_wiki


# Find Movie Genre
genre=open('moviegenre.txt','r')
genre_list = {}
action_list =[]
comedy_list=[]
drama_list=[]
horror_list=[]
scifi_list=[]
for lines in genre.readlines():
	line = lines.split()

	if line[0].lower() == 'action':
		action_list.append(line[1].lower())
	if line[0].lower() == 'comedy':
		comedy_list.append(line[1].lower())
	if line[0].lower() == 'drama':
		drama_list.append(line[1].lower())
	if line[0].lower() == 'horror':
		horror_list.append(line[1].lower())
	if line[0].lower() == 'scifi':
		scifi_list.append(line[1].lower())


genre.close()
# List 

def find_genre(baseline):
	genre_type=['action', 'comedy','drama','horror','scifi'] 	
	genre_list=[0,0,0,0,0]
	movie_type = []
	#global action_list
	#global comedy_list
	#global drama_list
	#global horror_list
	#global scifi_list
	
	for word in baseline:
		if word in action_list:
			genre_list[0]= genre_list[0] +1
		if word in comedy_list:
			genre_list[1]= genre_list[1] +1 	 
		
		if word in drama_list:
			genre_list[2]= genre_list[2] +1 	 
		
		if word in horror_list:
			genre_list[3]= genre_list[3] +1 	 
		
		if word in scifi_list:
			genre_list[4]= genre_list[4] +1	 
		
	if max(genre_list) >1:
		indices =[i for i ,x in enumerate(genre_list)if x== max(genre_list)]
		
		for index in indices:
			movie_type.append(genre_type[index])
		return ','.join(movie_type)
	else:
		return ''
		


def movie_cost(baseline):
	extraction_list = ['budget','cost','costs']
	filtered_baseline=[]
	value=[0]
	units=''
	for word in extraction_list:
		if word in baseline:
			
			index = baseline.index(word)
			if index >5 and len(baseline) > index+5:
			 	filtered_baseline=baseline[index-5:index+5]
			elif len(baseline) < index +5:
				filtered_baseline=baseline[index-5:len(baseline)]
			else:
				filtered_baseline=baseline[0:index+5]	
				
	for item in filtered_baseline:
		if '$' in item:
			value.append(item.replace('$','').replace(',',''))
		if 'million' in item:
			units ='million'
		if 'billion' in item:
			units = 'billion'
		
	#revenue =[]
	#revenue.append(str(max(value)))
	#revenue.append(units)
	
	#return ' '.join(revenue)
	made=0
	try:
		if units == 'million':
			made =  float (max(value)) *1000000
		if units == 'billion':
			made = float (max(value)) *1000000000	
		if units == '':
			made =  max(value)
	except ValueError:
		pass	
	#print made
	return str(made)		

def extract_dollar(baseline):
	extraction_list = ['domest','intern','gross','total','sales','earned','earn','grossed','sale','earning','worldwide','worldwid','worldwide.','worldwide,']
	filtered_baseline=[]
	value=[0]
	units=''
	domest=''
	intern1=''
	for word in extraction_list:
		if word in baseline:
			index = baseline.index(word)
			if word == 'domest':
				domest = baseline[index-1]
				
			if word == 'intern':
				intern1 =  baseline[index-1]
						
			if index < 6 and len(baseline) > 10:
					
				if word == 'worldwid' or word == 'worldwide' or word=='worldwide.' or  word=='worldwide,':
					filtered_baseline = baseline[0:index]
				else:
					filtered_baseline = baseline[0:9]
			elif index +5 < len(baseline):
				if word == 'worldwid' or word == 'worldwide'  or word=='worldwide.' or  word=='worldwide,':
					filtered_baseline = baseline[index-5:index]
				else:
					filtered_baseline = baseline[index:index+5]
			else:
				if word == 'worldwid' or word == 'worldwide'  or word=='worldwide.' or  word=='worldwide,':
					filtered_baseline = baseline[index-5:index]
				else:
					filtered_baseline = baseline[index: len(baseline)]

	if not domest :
		if not intern1 :
			
			try:
				return (int(domest.replace('$','').replace(',','')) + int(intern1.replace('$','').replace(',','')))
			except ValueError:
				pass
	
	for item in filtered_baseline:
		if '$' in item:
			value.append(item.replace('$','').replace(',',''))
		if 'million' in item:
			units ='million'
		if 'billion' in item:
			units = 'billion'
	#revenue =[]
	#revenue.append(str(max(value)))
	#revenue.append(units)
	#print float (max(value)) *1000000
	##print ' '.join(revenue)
	#return ' '.join(revenue)
	made=0
	try:
		if units == 'million':
			made =  float (max(value)) *1000000
		if units == 'billion':
			made = float (max(value)) *1000000000	
		if units == '':
			made =  max(value)
		#print made
	except ValueError:
		pass	
	return str(made)



def opening_weekend(baseline):
	extraction_list = ['open','weekend','weekend,','weekend.']
	filtered_baseline=[]
	value=[0]
	units=''
	for word in extraction_list:
		if word in baseline:
			
			index = baseline.index(word)
						
			if index < 6 and len(baseline) > 10:
				filtered_baseline = baseline[0:index+3]
			elif index +3 < len(baseline):
				filtered_baseline = baseline[index-3:index+3]
			else:
				filtered_baseline = baseline[index-3: len(baseline)]
	
			
	for item in filtered_baseline:
		if '$' in item:
			value.append(item.replace('$','').replace(',',''))
		if 'million' in item:
			units ='million'
		if 'billion' in item:
			units = 'billion'
		
	#revenue =[]
	#revenue.append(str(max(value)))
	#revenue.append(units)
	#print ' '.join(revenue)
	#return ' '.join(revenue)	
	made=0
	try:
		if units == 'million':
			made =  float (max(value)) *1000000
		if units == 'billion':
			made = float (max(value)) *1000000000	
		if units == '':
			made =  max(value)
		#print made
	except ValueError:
		pass	
	return str(made)
				


def extract_year(text):
	
	if len(text) >4:
		year = re.sub(r'\D',' ',text)
		
		released_year = year.strip().split(" ")
		for eachyear in released_year:
			if len(eachyear) == 4:
				return  eachyear
	else:
		return ''


def theatre_count(baseline):
	extraction_list = ['theatres','theatr','theatres.','theatres,']
	filtered_baseline=[]
	value=[0]
	units=''
	for word in extraction_list:
		if word in baseline:
			
			index = baseline.index(word)
			if index >3 :
			 	filtered_baseline=baseline[index-3:index]
			
			else:
				filtered_baseline=baseline[0:index]

	
				
	text=' '.join(filtered_baseline)
	number = re.sub(r'\D',' ',text)
	released_theatres = number.strip().split(" ")[0]
	return released_theatres
# Finds the positive and negative words in description of Movies and determines if posive sentiment or negative

def search_sentiment(baseline):
	sentiment_type=['positive', 'negative'] 	
	sentiment_list=[0,0]
	sentiment = [' ','-']
	movieRating_type = []
	#global positiveWords_list
	#global negativeWords_list
	
	
	for word in baseline:
		if word in positiveWords_list:
			sentiment_list[0]= sentiment_list[0] +1
		if word in negativeWords_list:
			sentiment_list[1]= sentiment_list[1] +1 	 
		
		
		
	if max(sentiment_list) >1:
                if(sentiment_list[0] == sentiment_list[1]):
			#return '0'+'\001'+'0'+'\001'+'neutral'
					return '0'+'|'+'0'+'|'+'neutral'
               		#return 'neutral'
      		else :
			indices =[i for i ,x in enumerate(sentiment_list)if x== max(sentiment_list)]
		
			#for index in indices:
				#movieRating_type.append(sentiment_type[index])
			#return sentiment_type[indices[0]]
			if (indices[0] ==0):
				return str.strip(str(sentiment[indices[0]])+str(sentiment_list[indices[0]]))+'|'+str.strip(str(sentiment[1])+str(sentiment_list[1]))+'|'+sentiment_type[indices[0]]
				#return str.strip(str(sentiment[indices[0]])+str(sentiment_list[indices[0]]))+'\001'+str.strip(str(sentiment[1])+str(sentiment_list[1]))+'\001'+sentiment_type[indices[0]]
			else:
				#return str(sentiment_list[0])+'\001'+str.strip(str(sentiment[indices[0]])+str(sentiment_list[indices[0]]))+'\001'+sentiment_type[indices[0]]
				return str(sentiment_list[0])+'|'+str.strip(str(sentiment[indices[0]])+str(sentiment_list[indices[0]]))+'|'+sentiment_type[indices[0]]
	else:
		#return '0'+'\001'+'0'+'\001'+'undefined'
		return '0'+'|'+'0'+'|'+'undefined'
		#return 'undefined'
		
		
# Program starts executing from below
# Above code is wriiten in functions	

#file_list = glob.glob('/users/asadhu/project/filmsdata/*')
file_list = glob.glob('./sentiment_input/*')
#file_list = glob.glob('/users/agaur2/filmsdata/part-m-04009')
#print file_list
for eachfile in file_list:
	data = open(eachfile,'r')
	#content = data.read()
	filename = eachfile.split('/')[-1]
	wfile = open('./films_sentiment/'+filename,'a')
	#wfile = open('/users/agaur2/films_output/'+filename,'a')

	#print filename
	for readline in data.readlines():
		#print readline
		if len(readline) > 10:
			actor_in_wiki =[]
			director_in_wiki=[]
			production_in_wiki=[]
			line1 = readline.decode('ascii','ignore').strip().lower() # Ignore all the non-ascii values in a string
			words=line1.split()	
			baseline = []
			year= ' '
			for word in words:
				if word not in stopwords.words('english'):
					baseword = porterstemmer.stem(word)				
					baseline.append(baseword.strip())
			if len(baseline) <10:	
				year = ' '.join(baseline[0:len(baseline)])	
			else:
				year = ' '.join(baseline[0:15])
			#release_year = extract_year(year)
			#actor_in_wiki = list_onexists(actors_list,baseline)
			#director_in_wiki = list_onexists(directors_list,baseline)
			#production_in_wiki = list_onexists(producers_list,baseline)
			#genre = find_genre(baseline)
			#cost = movie_cost(baseline)
			#theatres = theatre_count(baseline)
			#comma=','
			#revenue= extract_dollar(baseline)
			#opening= opening_weekend(baseline)	
			sentiment = search_sentiment(baseline)
			

			print sentiment
			#wfile.write(readline.decode('ascii','ignore').strip().lower()+'\001'+sentiment)
			wfile.write(readline.decode('ascii','ignore').strip().lower()+'|'+sentiment)
			#wfile.write(readline.decode('ascii','ignore').strip().lower()+'\001'+comma.join(director_in_wiki)+'\001'+comma.join(production_in_wiki)+'\001'+genre+'\001'+sentiment+'\001'+comma.join(actor_in_wiki)+'\001'+str(opening)+'\001'+str(revenue)+'\001'+str(cost)+'\001'+str(theatres)+'\001'+str(release_year)+'\001'+sentiment+'\n')			
			#print  readline.decode('ascii','ignore').strip().lower()+'\001'+comma.join(director_in_wiki)+'\001'+comma.join(production_in_wiki)+'\001'+genre+'\001'+comma.join(actor_in_wiki)+'\001'+str(opening)+'\001'+str(revenue)+'\001'+str(cost)+'\001'+str(theatres)+'\001'+str(release_year)
	wfile.close()
	data.close()
		

