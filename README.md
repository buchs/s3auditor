# s3auditor
Audits S3 storage in an account, provides summary size and largest/smallest objects

## tl;dr ##

  * Execute: **python S3-audit.py <command line options...>**.
  * Use the option -h (help) to get a list of other command line
    options.
  * The report will be output to standard output. You may wish to
    capture this report by redirecting to a file or piping through
    less, for example, if you have a large number of buckets. 

## Requirements ##

  * Python 3.6+
  * The click and boto3 libraries must be installed (e.g. *pip install
    boto3 click*).
  * AWS access credentials (access key/access secret) must be
  available for the program to use. One way to do so is via
  environment variables (AWS_ACCESS_KEY_ID and
  AWS_SECRET_ACCESS_KEY). Alternatively, these may be given on the
  command line for the program (see -h). Alternatively the
  .aws/credentials file may be set up and used. If a particular
  profile is referenced, then the -profile command line argument must
  be used or without it, the *default* profile is used. See [Boto documentation](http://boto3.readthedocs.io/en/latest/guide/configuration.html) for further information. 

## Limitations ##

 * Occasionally it has been noted during testing that the AWS S3
   service has a very high latency in response to a list request. This
   could be a side-effect of running the requests from different
   subprocesses (which is used to enhance performance). Unfortunately,
   there is not a simple way to exit out of a long request with
   Control-C, for example. It is best to simply wait for it to
   complete. You might think of this limitation as like owning a hot
   rod which idles very roughly.  
 * For optimal speed, the math to compute sizeunits (e.g. GB) result
   in truncation of the numbers, so a slight error is introduced in
   the resulting number of a magnitude equivalent to adding a decimal
   value of .99999... to the output integer. 
 
## Design Goals ##

  * Gather data regarding AWS S3 usage for a given account
	* List of S3 buckets along with: their creation and last
      modification dates, total number of objects, total object size
      for bucket and account
    * A collection of files, sized N (that is, user-chosen), which are
      either the oldest or newest modified for either each bucket or
      the account.
    * A option can request sizes be reported in bytes, kilobytes,
      megabytes or gigabytes 
    * An optional include or exclude options can be used to control
      which buckets are included in the audit. 
  * Operate from a Windows, Linux, OS X system shell command line
  * Maximum performance: operate in shortest elapsed time as possible

## Theory of Operation ##

  * For a given AWS account, S3 may have one or more buckets and each
    bucket may have one or more objects.
  * When running a query for information about objects in a single
    bucket, up to 1000 items in a batch may be returned with a Marker
    which allows retrieving a subsequent batch of times and continues
    until the full set of objects is exhausted. 
  * This application will run a large number of parallel processes,
    each handling some number of tasks. The tasks will include the
    initial listing for each bucket and then additional tasks for the
    subsequent listings for each batch for a larger bucket. These
    processes are run in a pool. We call these processes **listers**.
  * Two queues are used, one of work needing to be performed and one
    for the results from a given batch. They will be referred to as
    the **work queue** and the **results queue**.
  * The main process will gather the results from the results queue.
  * The main process will initially get a list of buckets and load up
    the work queue with the initial batch for each bucket found.
  * Listers will poll for new work on the work queue and when found
    execute it. If that AWS list_objects request returns a response
    indicating there are more objects (i.e. the IsTruncated field in
    the response is True), then that information will be returned on
    the results queue where additional task can be added to the work
    queue. The lister will continue with processing on the items
    returned. 
  * The number of lister processes will contribute to performance. The
    default number of processes will be four times the number of CPUs
    present on the system, as returned by
    multiprocessing.cpu_count(). This number can be overridden by the
    command line. Other factors will also contribute to performance,
    such as the number of requests reaching the S3 bucket, in addition
    to those from this application. In some cases the number of
    parallel lister processes used may need to be decreased or
    increased. No specific guidelines can be given because
    circumstances beyond the application's control will influence the
    outcome. Therefore, the user seeking optimal performance is
    encouraged to experiment with different numbers of lister
    processes.
  * Internally, sizes will be stored as bytes, using Python's
    arbitrarily larger number type (in Python 3). If sizes are
    requested in KB, MB, etc. then those numbers will be divided.
  * Most recently modified list will be created per bucket, but if the
    option is provided which requests it for the account, then the
    bucket data will be merged. 

## Obscure Cautions ##

In the event you are tempted, you should know that the python code for
this application will not run interactively. It must be a script file
that is executed from the command line, so that the subprocesses can
import the same file. See the notes [here](https://docs.python.org/3/library/multiprocessing.html#using-a-pool-of-workers).

## Future Work ##

  * Create a finalized PyPi project and release.
  * Debug why the requests to AWS occasionally have high latency.
  * Experiment with different sorting options and different batch
    sizes. Build-in intelligence to set the options based on the
    number of buckets and number of objects observed per bucket
    (i.e. gets smarter as it runs).
  * Understand how to effectively distribute an application with
    pipenv. 
