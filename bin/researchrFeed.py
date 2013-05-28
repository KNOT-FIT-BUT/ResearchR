#!/bin/env python
#-*- coding: utf-8 -*-
# Demo pouziti modulu psycopg2 pro pripojeni k databazi rrs.

import psycopg2

connection = psycopg2.connect("host=localhost dbname=reresearch user=reresearch password=lPPb5Wat4rPSMhtc")
cursor = connection.cursor()

cursor.execute("SELECT tag FROM public.tag")
for (tag, ) in cursor.fetchall():
	print tag
