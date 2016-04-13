#Names: Sachin Badgujar, Siva Krishna
#Course Project: Cloud Computing for Data Analysis.
#Project Name: Crime Forecasting


import sys
import numpy as np
import datetime
from pyspark import SparkContext
from datetime import date
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from compiler.syntax import check
from numpy.oldnumeric.random_array import seed
import operator
import math
import pylab as plt



if __name__ == "__main__":
    if len(sys.argv) !=2:
        print >> sys.stderr, "Usage: linreg <datafile>"
        exit(-1)
        
        
def barplot(d):
    plt.bar(range(1,len(d)+1), list(d.values()), align='center')
    plt.xticks(range(1,len(d)+1),list(d))
    plt.show()     

#setting up spark Context.
sc = SparkContext(appName="NaiveBayes")
sqlContext=SQLContext(sc)
inputCrimeCSV = sc.textFile(sys.argv[1])

header = inputCrimeCSV.first()
inputCrimeCSV = inputCrimeCSV.filter(lambda x:x !=header)
#inputCSV,excludecsv=inputCrimeCSV.randomSplit([0.999,0.001])

#Splitting the input file
inputCSV = inputCrimeCSV.take(5000)

#crimeData = inputCrimeCSV.map(lambda line: (line.split(',')))
crimeData = (sc.parallelize(inputCSV)).map(lambda line: (line.split(',')))


def date2dayofweek(f):
    f=f.split('/')
    g=f[0]+" "+f[1]+" "+f[2]
    day=datetime.datetime.strptime(g,'%m %d %Y').strftime('%A')
    return day

def timeslot(f):
    time24=datetime.datetime.strptime(f,'%I:%M:%S %p').strftime('%X')
    timesl=int(time24[0:2])/3
    timesl=timesl+1#divided time into 8 slots 
    return timesl
   
schemaString="day timeslot block crimetype latitude longitude" 


reformattedCrime=crimeData.map(lambda line: [date2dayofweek(line[1].split(' ',1)[0]),timeslot(line[1].split(' ',1)[1]),line[2].split(' ',1)[1],line[3],line[4],line[5]])


schemaCrime = sqlContext.createDataFrame(reformattedCrime, ['day','timeslot','block','crimetype','latitude','longitude'])
schemaCrime.registerTempTable("chicagocrimedata")
sqlContext.cacheTable("chicagocrimedata")

timeMatrix=sqlContext.sql("SELECT crimetype,timeslot,count(*) AS countPerTime FROM chicagocrimedata group by crimetype,timeslot order by crimetype")


#Extract all classes. Here, distinct crime types 
CrimeTypes = sqlContext.sql("SELECT distinct(crimetype) AS crimetypes FROM chicagocrimedata order by crimetypes").collect()
allCrimeTypes = list()
for index in range(len(CrimeTypes)):
    allCrimeTypes.append(CrimeTypes[index][0])
    
  
    
#Extracting statistics of crimes top 10 
crimeCounts=sqlContext.sql("SELECT crimetype,count(*) as crimeCount FROM chicagocrimedata GROUP BY crimetype order by crimeCount desc LIMIT 10").collect()
countByCrimeType = {}
for index in range(len(crimeCounts)):
    countByCrimeType[crimeCounts[index].crimetype] = crimeCounts[index].crimeCount

#print countByCrimeType.items()
sqlContext.uncacheTable("chicagocrimedata")         
timeMatrix.registerTempTable("TimeMatrix")


test = dict.fromkeys(list(countByCrimeType),0)
for crime in countByCrimeType:
    test[crime] = sqlContext.sql("SELECT timeslot,countPerTime FROM TimeMatrix WHERE crimetype = '"+crime+"' order by timeslot").collect()


#Displaying graph 
print dict(test)
notes = list()
for crime in countByCrimeType:
    if (len(test[crime])==8):
        plt.plot(range(1,9),[test[crime][x].countPerTime for x in range(len(test[crime]))])
        notes.append(crime)


plt.legend(np.asanyarray(notes), loc='upper left')
plt.show()

plt.pie(countByCrimeType.values(),  labels=list(countByCrimeType),  autopct='%1.1f%%', shadow=True, startangle=140)
plt.axis('equal')
plt.show()


sc.stop()    
