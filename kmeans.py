import sys
import numpy as np
import datetime
from pyspark import SparkContext
from datetime import date
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from compiler.syntax import check
from numpy.oldnumeric.random_array import seed
from math import sqrt



if __name__ == "__main__":
    if len(sys.argv) !=4:
        print >> sys.stderr, "Usage: KMEANS <datafile> latitutde longitude"
        exit(-1)
        

sc = SparkContext(appName="kmeans")
sqlContext=SQLContext(sc)
inputCrimeCSV = sc.textFile(sys.argv[1])
inputCrimeCSV.persist()

header = inputCrimeCSV.first()
inputCrimeCSV = inputCrimeCSV.filter(lambda x:x !=header)
inputCrimeCSV.persist()
inputCSV,excludecsv=inputCrimeCSV.randomSplit([0.01,0.99])
inputCSV.persist()

schemaString="day timeslot block crimetype latitude longitude"


#inputCrimeCSV=inputCrimeCSV.takeSample(False, 50000)
crimeData = (inputCSV).map(lambda line: (line.split(',')))
crimeData.persist()

def date2dayofweek(f):#function for changing date 2 day
    f=f.split('/')
    g=f[0]+" "+f[1]+" "+f[2]
    day=datetime.datetime.strptime(g,'%m %d %Y').strftime('%A')
    return day

def timeslot(f):#converting time into 8 slots
    time24=datetime.datetime.strptime(f,'%I:%M:%S %p').strftime('%X')
    timesl=int(time24[0:2])/3
    timesl=timesl+1#divided time into 8 slots 
    return timesl
    
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)




# reformatting data into proper format
reformattedCrime=crimeData.map(lambda line: [date2dayofweek(line[1].split(' ',1)[0]),timeslot(line[1].split(' ',1)[1]),line[2].split(' ',1)[1],line[3],line[4],line[5]])

#loading data into spark sql
schemaCrime = sqlContext.createDataFrame(reformattedCrime, ['day', 'timeslot', 'block', 'crimetype', 'latitude', 'longitude'])
schemaCrime.registerTempTable("chicagocrimedata")


# querying spark sql for required data
results=sc.parallelize(sqlContext.sql("SELECT latitude,longitude FROM chicagocrimedata").collect())


                        
x1=results.map(lambda line: ((line.latitude),(line.longitude)))

results=x1.map(lambda line: (float(line[0]),float(line[1])))

#print "intial seeds",results.take(20)[2].latitude
#print "intial seeds",float(results.take(20)[0][0])-float(results.take(20)[0][1])
noofseeds=50
intialseeds=results.take(noofseeds)# taking intial seeds for kmeans
countno=1
iterationcount=0
noofiterations=10
resultcentroids=[]

for num in range(0,noofiterations): 
    iterationcount+=1
    
    
    
    def distancecalculated(x,y):
        dist=sqrt((x[0]-y[0])**2+(x[1]-y[1])**2)
        return dist
    
    def mindistance(x):
        minDis=sys.float_info.max
        index=-1
        enumeratecount=0
        for i in intialseeds:
            dis=distancecalculated(i, x)
            if(dis<minDis):
                minDis=dis
                index=enumeratecount+1
                
            enumeratecount = enumeratecount+1
            
        return (index,(x,1))
                
                
            
            
        
    indexsample=results.map(lambda point: mindistance(point))

    
   
    #(1, ((41.833667467, -87.685190333),1)), (2, (41.809777926, -87.673509973)
    
    
        
    
    
    
    newcentroids=indexsample.reduceByKey(lambda a,b: (((a[0][0]+b[0][0])/2,(a[0][1]+b[0][1])/2),(a[1]+b[1])))#new centroids are calculated here
    
    newcentroids=newcentroids.sortByKey()
    
    
    print"newcentroids",newcentroids.values().collect()
    print "iterationcount",iterationcount
    if(iterationcount==noofiterations):
        resultcentroids=newcentroids.collect()
    intialseeds=newcentroids.values().map(lambda x:x[0]).collect()
   
    
    
    
latit =float(sys.argv[2])
longit=float(sys.argv[3])
querytuple=(latit,longit)
#lattitude longitude are stored



def distancecalculated2(x,y):
        dist=sqrt((x[0]-y[0])**2+(x[1]-y[1])**2)
        return dist
def mindistance2(x):#finding out which cluster does the location belongs to
    minDis=sys.float_info.max
    index=-1
    enumeratecount=0
    for i in resultcentroids:
        dis=distancecalculated2(i[1][0], x)
        if(dis<minDis):
            minDis=dis
            index=enumeratecount+1
                
        enumeratecount = enumeratecount+1
            
    return (index,x)



nearestcentroid=mindistance2(querytuple)

def getKey(item):
    return item[1][1]

sortlist=sorted(resultcentroids, key=getKey)#sorting list according to no of crimes in the cluster
# categorising area upon number of crimes in the corresponding cluster
counterno=1
for i in sortlist:
   
    if(cmp(resultcentroids[nearestcentroid[0]][1][0],i[1][0])==0):
        
        alertlevel=int(counterno/int(noofseeds/5))+1
        print"crime rating",alertlevel
        if(alertlevel==1):
            print"its okay"
        if(alertlevel==2):
            print"watch out"
        if(alertlevel==3):
            print"be careful"
        if(alertlevel==4):
            print"be cautious"
        if(alertlevel==5):
            print"dangerous place"
            
    counterno+=1
    







  
    

sc.stop()