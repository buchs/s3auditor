import time, re
import multiprocessing
from multiprocessing import Process, Queue
import click
import boto3

# Tuning parameters
workerMultiplier = 4 # utilize 4*cpu_count number of workers 
batchSize = 1000  # retrieve information from this number
                  # of objects, maximum, at one time. Must be 1000 or less.

# Used to collect results from Lister process and print reports
class ListerResults:
  def __init__(self, listerId, taskId, bucket, fileCount, \
               totalSize, lastModified, contenders, batch, incomplete):
    self.listerId = listerId
    self.taskId = taskId
    self.bucket = bucket
    self.fileCount = fileCount
    self.totalSize = totalSize
    self.lastModified = lastModified
    self.contenders = contenders
    self.batch = batch
    self.incomplete = incomplete

  # merge a new ListerResults with the current one
  def merge(self, listerResults, config):
    if self.bucket != listerResults.bucket:
      raise AssertionException("Mismatch of buckets on merge of results")
    else:
      self.fileCount += listerResults.fileCount
      self.totalSize += listerResults.totalSize
      # Though lastModified could be set to None by __init__, those
      # cases of empty bucket will not have subsequent calls to
      # merge
      if listerResults.lastModified > self.lastModified:
        self.lastModified = listerResults.lastModified
      combinedContenders = self.contenders + listerResults.contenders
      if config['oldest']: # if true look for oldest
        reverse = False
      else:
        reverse = True
      combinedContenders.sort(key=lambda obj: obj[1], reverse=reverse)
      self.contenders = combinedContenders[0:config['numfiles']]

  def report(self, CreationDate, config):
    print("Bucket: " + self.bucket)
    print("Creation Date: " + CreationDate.isoformat())
    if self.lastModified is not None:
      print("Last Modified: " + self.lastModified.isoformat())
    print("Number Files: %d" % (self.fileCount,))
    print("Total Size: " + sizeString(self.totalSize,config['sizeUnit']))
    if len(self.contenders) > 0:
      if config['oldest']:
        print("Oldest Files:")
      else:
        print("Newest Files:")
      print(" key  modified date")
      print(" ---  -------------")
      for contender in self.contenders:
        t = contender[1]
        line = '  ' + contender[0] + '  ' + t.isoformat()
        print(line)
    print('\n')

# Implemented with right shift instead of division because it is
# faster :-). Of course, doing a float division would be more
# accurate. Float division plus round up to next nearest integer
# would be another option. What are a few bytes among friends?
# Especially if we are dealing with petabytes!
def sizeString(value, sizeUnit):
  if sizeUnit == 'KB':
    return(str(value >> 10)+' KB')
  elif sizeUnit == 'MB':
    return(str(value >> 20)+' MB')
  elif sizeUnit == 'GB':
    return(str(value >> 30)+' GB')
  else:
    return(str(value))

# This will be a whole account summary, rather than broken down by buckets
def createSummaryReport(bucketInfo,config):
  # algorithm - aggregate one bucket at a time, initialize first
  fileCount = 0
  totalSize = 0
  lastModified = None
  contenders = []
  firstCreation = None
  if config['oldest']: # if true look for oldest
    reverse = False
  else:
    reverse = True
  numfiles = config['numfiles']
  
  # for this summary report, the list of the top newest/oldest files
  # will only make sense if we will add the bucket name to the key name;
  # we will do so with a colon interspersed 
  for bucket in bucketInfo.keys():
    results = bucketInfo[bucket]['Results']
    fileCount += results.fileCount
    totalSize += results.totalSize
    if lastModified is None:
      lastModified = results.lastModified
    elif results.lastModified > lastModified:
      lastModified = results.lastModified
    # extend list, sort and trim it  
    for contendertuple in results.contenders:
      contenders.append((bucket+':'+contendertuple[0],contendertuple[1]))
    contenders.sort(key=lambda obj: obj[1], reverse=reverse)
    if len(contenders) > numfiles:
      del(contenders[numfiles:])
    if firstCreation is None:
      firstCreation = bucketInfo[bucket]['CreationDate']
    elif bucketInfo[bucket]['CreationDate'] < firstCreation:
      firstCreation = bucketInfo[bucket]['CreationDate']

  # Now output the report
  print('\n------- Full Account Report --------\n')
  print("Earliest Bucket Creation Date: " + firstCreation.isoformat())
  print("Last Modified Object Date: " + lastModified.isoformat())
  print("Total Number Files: %d" % (fileCount,))
  print("Total Size: " + sizeString(totalSize,config['sizeUnit']))
  if len(contenders) > 0:
    if config['oldest']:
      print("Oldest Files:")
    else:
      print("Newest Files:")
    print(" key  modified date")
    print(" ---  -------------")
    for contender in contenders:
      t = contender[1]
      line = '  ' + contender[0] + '  ' + t.isoformat()
      print(line)
  print('\n')


# This will create a boto s3 client for required operations based on
# the config dictionary.
def getS3Client(config):
  
  if config['authtype'] == 'profile':
    session = boto3.Session(profile_name=config['profile'])
    client = session.client('s3')
  elif config['authtype'] == 'key':
    client = boto3.client('s3',aws_access_key_id=config['keyid'],
                          aws_secret_access_key=config['secret'])
  else:
    client = boto3.client('s3')

  # future improvement: can we verify here that we have a workable client?
  return client



# This is the worker that runs in a parallel subprocess and retrieves
# information on a batch of objects. 
def listerWorker(id,workQueue,resultsQueue,config):
  client = getS3Client(config)
  # report back that we are up and running & debugging info
  resultsQueue.put("worker active,%d" % (id,))
  if config['oldest']: # if true look for oldest
    reverse = False
  else:
    reverse = True
  numfiles = config['numfiles'] # number of contenders to keep

  working = True
  while working:
    items = workQueue.get()
    taskId = int(items[0])
    commandString = items[1]
    
    if commandString.startswith("list,"):
      commandParts = commandString.split(',')
      command = commandParts[0]
      bucket = commandParts[1]
      contToken = commandParts[2]
      batch = int(commandParts[3])
      # Report back that we got a task to work on
      resultsQueue.put("start,%d,%d,%s,%d" % (id,taskId,bucket,batch))

      # make request to S3
      if contToken == "None":
        response = client.list_objects_v2(Bucket=bucket,MaxKeys=batchSize)
      else:
        response = client.list_objects_v2(Bucket=bucket,MaxKeys=batchSize, \
                                        ContinuationToken=contToken)

      if config['debug']:
        resultsQueue.put("response received,%d,%d" % (id, taskId))

      # check return - if truncated, then queue up the next batch
      # immediately, before processing this batch
      if response['IsTruncated']:
        schedulenext = "next,%s,%s,%d" %  \
                       (bucket,response['NextContinuationToken'],batch+1)
        resultsQueue.put(schedulenext)
      
      # process results, find modified date contenders, total size, count
      fileCount = 0
      totalSize = 0
      contenders = []

      # Did we get no contents (empty bucket)? Handle that simply
      if config['debug']:
        resultsQueue.put('response keys: %s' % (repr(response.keys()),))
      if 'Contents' not in response:
        lastModified = None

      else:   # have real contents
        
        # Pick the first modified date found just to start off the
        # search for the newest
        lastModified = response['Contents'][0]['LastModified']

        # Algorithm for keeping contenders: The best choice is debatable. It
        # would certainly depend upon the statistics on the number of objects
        # per bucket. Without any hard data, I choose the easiest to implement:
        # For each object, create a maximum numfiles+1 length list, sort and
        # truncate to numfiles length. The assumption is that sorting a mostly
        # sorted list is fast.
      
        for object in response['Contents']:
          # maintain list of contenders for newest/oldest objects.
          # maintain only the object key and mod date
          contenders.append((object['Key'],object['LastModified']))
          contenders.sort(key=lambda obj: obj[1], reverse=reverse)
          if len(contenders) > numfiles:
            del(contenders[-1])

          # track overall statistics
          fileCount += 1
          totalSize += object['Size']
          if object['LastModified'] > lastModified:
            lastModified = object['LastModified']

      # Either the if/else above found no contents or it did. 
      # Now package up the results in a dict so they can be placed
      # on the results queue easily
      results = ListerResults(id,taskId,bucket,fileCount,totalSize, \
                              lastModified,contenders,batch, \
                              response['IsTruncated'])
      resultsQueue.put(results)
      
    elif commandString == "finished":
      working = False
      resultsQueue.put("finished,worker,%d" % (id,))
    

# enable all three help options for the command
CONTEXT_SETTINGS = dict(help_option_names=['-h', '-help', '--help'])

@click.command(context_settings=CONTEXT_SETTINGS)
@click.option('-profile', help='Which if your defined AWS profiles to use. Default is to use the default profile.')
@click.option('-keyid', envvar='AWS_ACCESS_KEY_ID', help='AWS access key id. Not necessary if using -profile.')
@click.option('-secret', envvar='AWS_SECRET_ACCESS_KEY', help='AWS access key secret paired with the access key. Not necessary if using -profile.')
@click.option('-numfiles', default=5, help='Number of newest/oldest objects to report.')
@click.option('-oldest', is_flag=True, default=False, help='Provide a report of oldest modified objects instead of the default, newest modified.')
@click.option('-sizeunit', type=click.Choice(['KB','MB','GB']), help='Size unit for file sizes, use: KB, MB, GB for kilo-, mega- and giga-bytes, default is bytes.')
@click.option('-byaccount', is_flag=True, help='The most recently modified object list is created just for the account and not the buckets, otherwise it is just for the buckets.')
@click.option('-include', help='A comma separated list of buckets to be included (others will be omitted). Use apart from -exclude.')
@click.option('-exclude', help='A comma-separated list of buckets to be excluded (others will be included). If used with -include, it only excludes those matching from the -include clause and therefore not useful.')
@click.option('-debug', is_flag=True, default=False, help='Print extra debugging information.')

def main(profile,keyid,secret,numfiles,oldest,sizeunit,byaccount,include,exclude,debug):
  """
  Program which audits all the AWS S3 buckets in use for the AWS account specified (or default used). Totals for sizes and modified dates are kept per bucket, but also the -numfiles newest (or oldest) modified objects per bucket (or per account) are returned.
  """

  # Operate on command line arguments, storing several in the config
  # dictionary, which will be passed to the subprocesses.
  config = {}
  if profile is not None:
    config['authtype'] = 'profile'
    config['profile'] = profile
  elif keyid is not None and secret is not None:
    config['authtype'] = 'key'
    config['keyid'] = keyid
    config['secret'] = secret
  else:
    config['authtype'] = 'default'

  config['numfiles'] = numfiles

  if oldest is None:
    config['oldest'] = False
  else:
    config['oldest'] = oldest

  # look at include/exclude options, set up lists. If both include and exclude
  # are given, just check to see if any excluded ones are also listed on
  # the include list. If so, subtract them from the include list.
  if include is not None:
    includelist = include.split(',')
  else:
    includelist = None

  if exclude is not None:
    excludelist = exclude.split(',')
  else:
    excludelist = None

  if include is not None and exclude is not None:
    # subtract the excludes from the includes.
    for ex in excludelist:
      if includelist.count(ex) > 0:
        include.remove(ex)
    del excludelist
    excludelist = None

  config['sizeUnit'] = sizeunit
  config['debug'] = debug
  
  # listerWorkers pull work from one queue, workQueue.
  # they deliver results into another queue, resultsQueue
  workQueue = Queue()
  resultsQueue = Queue()


  # spin up the required number of workers
  numberWorkers = workerMultiplier * multiprocessing.cpu_count()
  listerList = []
  checkoff = []  # this list to checkoff workers which start up
  for w in range(1,numberWorkers+1):
    listerList.append(Process(target=listerWorker,args=(w,workQueue,resultsQueue,config)))
    listerList[-1].start()
    checkoff.append(str(w))

  # Queue up tasks/work to be done.
  tasksRunning = []
  taskId = 1

  bucketInfo = {} # This is where we will collect all the data we find
  # get the boto S3 client and start doing some work, retrieving buckets
  client = getS3Client(config)

  bucketResp = client.list_buckets()
  bucketDataList = bucketResp['Buckets']

  for bucket in bucketDataList:
    bucketName = bucket['Name']
    if includelist is not None and includelist.count(bucketName) == 0:
      # not on includelist, so skip it
      continue
    if excludelist is not None and excludelist.count(bucketName) > 0:
      continue
    # Capture the creation data of the bucket.
    bucketInfo[bucketName] = { 'CreationDate': bucket['CreationDate'] }

    # queue up this bucket to be listed
    workQueue.put((taskId,"list,"+bucketName+",None,1"))
    tasksRunning.append(taskId)
    taskId += 1


  # Next, Monitor the Results Queue.
  # This process will reschedule any tasks that need to be done while tracking
  # the tasks in flight. When the number in flight hits zero, we can start
  # to close things down by putting the finish task on the work queues.

  notFinished = True
  while notFinished:

    output = resultsQueue.get()

    if type(output) == str:
      if debug: print("--> " + output)
      outputWords = output.split(",")
      
      if outputWords[0] == "next":
        batch = int(outputWords[3])
        details = "list,"+outputWords[1]+","+outputWords[2]+ \
                  ","+str(batch)
        workQueue.put((taskId,details))
        tasksRunning.append(taskId)
        taskId += 1
  
      elif outputWords[0] == "worker active":
        checkoff.remove(str(outputWords[1]))
        if len(checkoff) == 0:
          print("%d workers started" % (numberWorkers,))

      elif outputWords[0] == "start":
        print("bucket %s, batch %s starting" % (outputWords[3],outputWords[4]))
        
    elif type(output) == ListerResults:
      if 'Results' in bucketInfo[output.bucket]:
        bucketInfo[output.bucket]['Results'].merge(output,config)
      else:
        bucketInfo[output.bucket]['Results'] = output
      returnedTaskId = output.taskId
      if output.incomplete:
        print("bucket %s, batch %d complete, more to do" % (output.bucket,output.batch))
      else:
        print("bucket %s, batch %d complete, bucket finished" % (output.bucket,output.batch))
      if tasksRunning.count(returnedTaskId) > 0:
        if debug: print("removing task " + str(returnedTaskId))
        tasksRunning.remove(returnedTaskId)
        

    if len(tasksRunning) == 0:
      notFinished = False

  
  # Work is done. Now tell the subprocesses they are through.
  # Put this on workQueue, once per process
  print("shutting down workers\n\n")
  for n in range(numberWorkers):
    workQueue.put((-1,"finished"))

  # Then terminate those processes
  for p in listerList:
    p.terminate()

  # Now output report
  if byaccount:

    createSummaryReport(bucketInfo,config)

  else:
    
    print('\n-------- Bucket Report --------\n')
    for bucket in bucketInfo.keys():
      bucketInfo[bucket]['Results'].report(
         bucketInfo[bucket]['CreationDate'],config)

  
# This module must be able to be imported by subprocess workers.
if __name__ == '__main__':
  main()
