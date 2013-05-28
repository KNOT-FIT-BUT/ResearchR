#!/bin/env python
#-*- coding: utf-8 -*-
# Demo pouziti modulu psycopg2 pro pripojeni k databazi rrs.

import psycopg2

connection = psycopg2.connect("host=nlpmosaic.fit.vutbr.cz dbname=reresearch user=reresearch password=lPPb5Wat4rPSMhtc")
cursor = connection.cursor()

cursor.execute("SELECT type FROM data_researchr_test.publication_type;")
for (tag, ) in cursor.fetchall():
	print tag
