#!/bin/env python
#-*- coding: utf-8 -*-
import psycopg2
import ConfigParser
import sys, getopt
import hashlib
import unicodedata
from StringIO import StringIO
from time import strptime
import logging
import random
import time

from rrslib.db.model import *
from rrslib.db.dbal import PostgreSQLDatabase, FluentSQLQuery
from rrslib.db.xmlimport import RRSXMLImporter, LOOKUP_FAST, LOOKUP_PRECISE
from rrslib.db.dbal import PostgreSQLDatabase, RRSDatabase, RRSDB_MISSING, EXEC_LOG
from rrslib.extractors.normalize import Normalize
from rrslib.xml.xmlconverter import Model2XMLConverter

from researchr import *

class RPublication:
	def __init__(self):
		self.abstract = None
		self.address = None
		self.authors = []
		self.person = []
		self.booktitle = None
		self.conference = None
		self.conferenceYear = None
		self.doi = None
		self.editors = []
		self.firstpage = None
		self.key = None
		self.issuenumber = None
		self.journal = None
		self.key = None
		self.lastpage = None
		self.month = None
		self.note = None
		self.number = None
		self.organization = None
		self.publisher = None
		self.series = None
		self.title = None
		self.publication_type = None
		self.url = None
		self.volume = None
		self.volumenumber = None
		self.year = None

class ResearchrPublicationFeeder:
	def __init__(self, config, importer_kwargs):
		#data ziskana z api
		self.rPublication = None
		
		#objekt typu RRSPublication, ktery po naplneni budeme importovat do db
		self.publication = None

		#informace o feederu
		self.added = 0
		self.deleted = 0

		#nastaveni pro importer
		self.importer_kwargs = importer_kwargs

		self.LimitMin = 0.5
		self.LimitMax = 2

		#objekt pro vytvareni sql dotazu
		self.q = FluentSQLQuery()

		#API
		self.researchrClass = ResearchrClass()

		#vazby N:N
		self.publicationPerson = RRSRelationshipPersonPublication()

		#nejvyssi vrstva, pro nacteni objektu podle id
		self.rrsdb = RRSDatabase()

		#normalizator
		self.norm = Normalize()

	def checkIfImport(self, name):
		self.q.select("id").from_table("publication")
		self.q.where("researchr_key=", name)
		self.q()
		data = self.q.fetch_one()
		self.q.cleanup()
		return data

	def __FillType(self):
		self.q.select("id").from_table("publication_type")
		self.q.where("type=", self.rPublication.publication_type)
		self.q()
		data = self.q.fetch_one()
		self.q.cleanup()
		if (data != None):
			self.publication.set("type", self.rrsdb.load("publication_type", data[0]))
	

	def __FillSeries(self):
		if (self.rPublication.series != None and self.rPublication.series != ""):
			data = None
			while (data == None):
				#ziskame series_id
				self.q.select("id").from_table("publication_series")
				self.q.where("title=", self.rPublication.series)
				self.q()
				data = self.q.fetch_one()
				self.q.cleanup()
				if (data == None):
					#pridame zaznam do tabulky series
					series = RRSPublication_series()
					series.set("title", self.rPublication.series)
					importer = RRSXMLImporter(self.importer_kwargs)
					importer.import_model(series)
					continue
			self.publication.set("series", self.rrsdb.load("publication_series", data[0]))
			

	def __FillPublisher(self):
	 	if (self.rPublication.publisher != None and self.rPublication.publisher != ""):
			data = None
			normalized_title = self.norm.organization(self.rPublication.publisher)
			while (data == None):
				self.q.select("id").from_table("organization")
				self.q.where("title_normalized=", normalized_title)
				self.q()
				data = self.q.fetch_one()
				self.q.cleanup()
				if (data == None):
					organization = RRSOrganization(title=self.rPublication.publisher, title_normalized=normalized_title)
					importer = RRSXMLImporter(self.importer_kwargs)
					importer.import_model(organization)
					continue
				self.publication["publisher"] = self.rrsdb.load("organization", data[0])
	"""
	FillAuthor Add (if there are not) person to db and 
	contain them with actual publication. Foreach
	 rPublication.authors, take only person's url and fullname.
	"""
	def __FillAuthors(self):
		if (len(self.rPublication.authors) != 0):
			for author in self.rPublication.authors:
				data = None
				personUrls = RRSRelationshipPersonUrl()
				if "person" not in author:
					continue
				rFullname = author["person"]["fullname"]
				rUrl = author["person"]["url"]
				while (data == None):
					#pokusime se ziskat url id
					self.q.select("id").from_table("url")
					self.q.where("link=", rUrl)
					self.q()
					data = self.q.fetch_one()
					self.q.cleanup()
					if (data == None):
						#pokud url v db jeste neni, pridame ji
						url = RRSUrl(link=rUrl)
						urlType = self.rrsdb.load("url_type", "1")
						url.set("type", urlType)
						importer = RRSXMLImporter(self.importer_kwargs)
						importer.import_model(url)
						continue
					url = self.rrsdb.load("url", data[0])
					personUrls.set_entity(url)
				data = None
				while (data == None):
					#ziskame person_id
					self.q.select("id").from_table("person")
		       			self.q.where("full_name=", rFullname)
			       		self.q()
			       		data = self.q.fetch_one()
			       		self.q.cleanup()
					if (data == None):
						#pridame zaznam do tabulky person
						person = RRSPerson()
						person.full_name = rFullname
						splitName = rFullname.split()
						if (len(splitName) == 3):
							person.first_name = splitName[0]
							person.middle_name = splitName[1]
							person.last_name = splitName[2]
						elif (len(splitName) == 2):
							person.first_name = splitName[0]
							person.last_name = splitName[1]
						person.full_name_ascii = unicodedata.normalize('NFKD', rFullname).encode('ascii', 'ignore')
						importer = RRSXMLImporter(self.importer_kwargs)
						importer.import_model(person)
						continue
					person = self.rrsdb.load("person", data[0])
					self.publicationPerson.set_entity(person)

	def FillPublication(self, name):
		"""
		This function call all private function with prefix Fill, 
		this function assign data from rPublication to publication(RRSPublication).
		"""
		self.__FillRPublication(name)
		self.publication = RRSPublication()
		self.__FillAuthors()
		self.__FillPublisher()
		self.__FillType()
		self.__FillSeries()
		self.publication.set('title', self.rPublication.title)
		self.publication.set('title_normalized', self.norm.publication(self.rPublication.title))

		if (self.rPublication.year != None and self.rPublication.year != ''):
			self.publication.set('year', int(self.rPublication.year)) # 2000 -> 2000

		if (self.rPublication.month != None and self.rPublication.month != ''):
			print(strptime(self.rPublication.month[:3],'%b').tm_mon)# "January" -> "Jan" -> 1
			self.publication.set('month', int(strptime(self.rPublication.month[:3],'%b').tm_mon))
		if (self.rPublication.volume != None and self.rPublication.volume != '' and self.rPublication.volume.isdigit()):
			self.publication.set('volume', int(self.rPublication.volume))

		if (self.rPublication.number != None and self.rPublication.number != ''):
			self.publication.set('number', self.rPublication.number)

		if (self.rPublication.abstract != None and self.rPublication.abstract != ''):
			self.publication.set('abstract', self.rPublication.abstract)

		if (self.rPublication.doi != None and self.rPublication.doi != '' and "http://dx.doi.org/" in self.rPublication.doi):
			self.publication.set('doi', self.rPublication.doi.strip('http://dx.doi.org/'))

		if (self.rPublication.firstpage != None and self.rPublication.lastpage != None and 
			self.rPublication.firstpage != '' and self.rPublication.lastpage != ''):
			self.publication.set('pages', str(self.rPublication.firstpage) + " - " + str(self.rPublication.lastpage))
		self.publication.set('researchr_key', self.rPublication.key, strict=False)
		self.publication['person'] = self.publicationPerson
		print(self.publication)
		importer = RRSXMLImporter(self.importer_kwargs)
		try:
			importer.import_model(self.publication)
		except RRSDatabaseEntityError as e:
			logging.warning('RRSDatabaseEntityError - %s' % self.rPublication.key)
		except:
			logging.warning('Unexpected error - %s' % self.rPublication.key)
		

	def __FillRPublication(self, name):
		"""
		Fill rPublication object.

		@type  key: string
		@param key: Name od publication.	
		"""
		self.rPublication = RPublication()
		#get data via api
		publicationData = self.researchrClass.getPublication(name)
		time.sleep(random.uniform(self.LimitMin, self.LimitMax))
		print(publicationData)
		for key, value in publicationData.items():
			if key == 'abstract':
				self.rPublication.abstract = value
			elif key == 'address':
				self.rPublication.address = value
			elif key == 'authors':
				self.rPublication.authors = value
			elif key == 'booktitle':
	     			self.rPublication.booktitle = value
			elif key == 'conference':
	    			self.rPublication.conference = value
			elif key == 'conferenceYear':
	     	       		self.rPublication.conferenceYear = value
			elif key == 'doi':
	     	       		self.rPublication.doi = value
			elif key == 'editors':
				self.rPublication.editors = value
			elif key == 'firstpage':
	     	       		self.rPublication.firstpage = value
			elif key == 'key':
				self.rPublication.key = value
			elif key == 'issuenumber':
				self.rPublication.issuenumber = value
			elif key == 'journal':
				self.rPublication.journal = value
			elif key == 'key':
				self.rPublication.key = value
			elif key == 'lastpage':
	     	       		self.rPublication.lastpage = value
			elif key == 'month':
	     	       		self.rPublication.month = value
			elif key == 'note':
				self.rPublication.note = value
	     		elif key == 'number':
	     	       		self.rPublication.number = value
	     		elif key == 'organization':
	  	   		self.rPublication.organization = value
	  	   	elif key == 'publisher':
	     			self.rPublication.publisher = value
	     		elif key == 'series':
	     			self.rPublication.series = value
	  	   	elif key == 'title':
	  	   		self.rPublication.title = value
	  	   	elif key == 'type':
	 			self.rPublication.publication_type = value
	     		elif key == 'url':
	     			self.rPublication.url = value
	     		elif key == 'volume':
	   			self.rPublication.volume = value
	    		elif key == 'volumenumber':
				self.rPublication.volumenumber = value
	     		elif key == 'year':
		    		self.rPublication.year = value

def main(argv):
	#load config file
	config = ConfigParser.RawConfigParser()
	config.read('app.ini')

	#logging setting
	logging.basicConfig(filename='error.log',level=logging.DEBUG)

	#importer setting
	importer_kwargs = {
			'update_rule':  RRSDB_MISSING,      # jak se bude chovat updatovani radku pokud se vkladaji data do jiz existujiciho radku
			'lookup_level': LOOKUP_PRECISE,    # uroven zanoreni pri vyhledavani shodnych entit na zaklade topologie
			'logs':	 EXEC_LOG,	        # uroven logovani: informacni (status msg) a exekutivni log (update, insert)
			'logfile':      'logfile.log',      # cesta a jmeno logovaciho souboru
			'module':       'import rrslib.db.xmlimport',  # jmeno modulu, ktery s daty pracuje
			'schema':       'data_researchr_test'  # databazove schema, do ktereho hodlame data nahrat
			}

	db = PostgreSQLDatabase(importer_kwargs['logfile'])
        db.connect(host=config.get("Database","host"),
                dbname=config.get("Database","db"),
                user=config.get("Database","user"),
                password=config.get("Database","pass"))
        db.set_schema(config.get("Database","schema"))

	# load names from file
	names = loadFile(getParam(argv))
	# foreach names
	for name in names.split('\n'):
		print(name)
		if (checkIfImport(name) == None):
			feeder = ResearchrPublicationFeeder(config, importer_kwargs)
			feeder.FillPublication(name)

def checkIfImport(name):
	q = FluentSQLQuery()
	q.select("id").from_table("publication")
	q.where("researchr_key=", name)
	q()
	data = q.fetch_one()
	return data

def loadFile(filename):
	"""
	Open file with publications names
	"""
	try:
		f = open(filename, 'r')
   	except IOError:
		print 'cannot open %s' % filename
		exit(2)
	data = f.read()
	return data

def getParam(argv):
	 try:
		opts, args = getopt.getopt(argv,"hi:",["ifile="])
	 except getopt.GetoptError:
	 	print 'researchrFeed.py -i <inputfile>'
	 	sys.exit(2)
	 for opt, arg in opts:
		if opt == '-h':
		      print 'researchrFeed.py -i <inputfile>'
		      sys.exit(2)
	 	elif opt in("-i", "--ifile"):
		      return arg

if __name__ == "__main__":
	main(sys.argv[1:])

