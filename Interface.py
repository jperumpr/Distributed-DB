#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur=openconnection.cursor()
    query = "drop table if exists " + ratingstablename + "; CREATE TABLE " + ratingstablename+ " (UserID int, Movieid int, Rating float)"
    cur.execute(query)
    print("create query executed "+ query)
    ratings_list = []
    for each_line in open(ratingsfilepath, 'r'):
        row = each_line.rstrip()
        ratings_list.append(row.split('::'))
    count=0
    for record in ratings_list:
        record=record[:3]
        cur.execute("Insert into " + ratingstablename + " VALUES(%s,%s,%s)", record)
        count+=1
        if(count==10000):
            openconnection.commit()
            count=0


pass


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur=openconnection.cursor();
    query = "select * from " + ratingstablename
    cur.execute(query)
    rows = cur.fetchall()
    first_limit=5.0/numberofpartitions
    threshold = []
    for i in range(0,numberofpartitions):
        threshold.append(first_limit)
        first_limit+=5.0/numberofpartitions
        qstatement = 'drop table if exists range_part' + str(i) +';' 'create table if not exists range_part'+str(i)+' (UserID int, Movieid int, Rating float)'
        cur.execute(qstatement)
    for row in rows:
        for i in range(0,numberofpartitions):
            if row[2]<=threshold[i]:
                cur.execute("Insert into range_part" + str(i) + " VALUES(%s,%s,%s)", row)
                break
    metadataquery = 'drop table if exists range_num ; create table range_num (partitionno int) ; insert into range_num values(' + str(numberofpartitions) + ')'
    cur.execute(metadataquery)
    cur.close()
    pass


def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    query = "select * from " + ratingstablename
    cur.execute(query)
    rows = cur.fetchall()
    for i in range(0,numberofpartitions):
        qstatement = 'drop table if exists rrobin_part' + str(i) + ';' 'create table if not exists rrobin_part' + str(i) + ' (UserID int, Movieid int, Rating float)'
        cur.execute(qstatement)
    part_no=0
    for row in rows:
        cur.execute("Insert into rrobin_part" + str(part_no) + " VALUES(%s,%s,%s)", row)
        part_no=(part_no+1)%numberofpartitions
    metadataquery1='drop table if exists rrobin_num ; create table rrobin_num (partitionno int) ; insert into rrobin_num values(' + str(numberofpartitions) +')'
    metadataquery2='drop table if exists roundrobin_num ; create table roundrobin_num (partitionno int) ; insert into roundrobin_num values(' + str(part_no) +')'
    metadataquery2 = 'drop table if exists roundrobin_num ; create table roundrobin_num (partitionno int) ; insert into roundrobin_num values(0)'
    cur.execute(metadataquery1)
    cur.execute(metadataquery2)
    cur.close()
    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    metadataquery = "select * from rrobin_num"
    cur.execute(metadataquery)
    num = cur.fetchone()
    n = num[0]
    metadataquery = "select * from roundrobin_num"
    cur.execute(metadataquery)
    num = cur.fetchone()
    rpart = num[0]
    rating_query = 'Insert into ' + ratingstablename + ' values(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ')'
    cur.execute(rating_query)
    range_query = 'Insert into rrobin_part' + str(rpart) + ' values(' + str(userid) + ',' + str(itemid) + ',' + str(rating) + ' )'
    cur.execute(range_query)
    rpart = (rpart + 1) % n
    metadataquery = 'drop table if exists roundrobin_num ; create table roundrobin_num (partitionno int) ; insert into roundrobin_num values(' + str(rpart) + ')'
    cur.execute(metadataquery)
    cur.close()
    pass


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    openconnection.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = openconnection.cursor()
    metadataquery = "select * from range_num"
    cur.execute(metadataquery)
    num=cur.fetchone()
    n=num[0]
    rating_query= 'Insert into ' + ratingstablename + ' values(' + str(userid)+ ',' + str(itemid) + ',' + str(rating) + ')'
    print rating_query
    cur.execute(rating_query)
    first_limit = 5.0/n
    threshold = []
    for i in range(0, n):
        threshold.append(first_limit)
        first_limit += 5.0/n
    for i in range(0, n):
        if rating <= threshold[i]:
            range_query ='Insert into range_part' + str(i) + ' values(' + str(userid)+ ',' + str(itemid) + ',' + str(rating) + ' )'
            cur.execute(range_query)
            break
    cur.close()
    pass


def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)
    #cur.execute("commit")
    # Clean up
    cur.close()
    con.close()

#create_db("postgres")
#print "starting"
#con = getopenconnection(dbname='dds_assgn1')
#con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
#query = "drop table if exists test1 ; CREATE TABLE test1 (UserID int, Movieid int, Rating float)"
#cur = con.cursor()
#cur.execute(query)
#loadratings("test", "/home/user/Documents/ratings.dat",con)
#rangepartition("ratings", 5,con)
#roundrobinpartition("test1",5,con)
#roundrobininsert("test1",1, 444, 2.5, con)
#rangeinsert("ratings",1, 444, 2.5, con)
#con.close()