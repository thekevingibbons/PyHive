from time import time, sleep
import os
from multiprocessing import Pool
#from copy import deepcopy
from multiprocessing.dummy import Pool as ThreadPool
from Controllers.Experimenting.Remote import Remote

class Host(object):
    # Used to check how many cores the remote host has
    @staticmethod
    def getRemoteCPUs():
        import os
        return os.cpu_count()

    def __init__(self, hostname, username="", password="", port=22, processes=None, bees= None, timeout= None, reconnectTime= None):
        self.name = "{0}@{1}".format(username, hostname)
        self.hostname = hostname
        self.username = username
        self.password = password
        self.port = port
        self.connection = None
        self.connect()

        # Skip if we couldn't connect to the remote host
        if hostname is "queen" or self.connection is not None:
            # QueenProcesses should always be greater than zero, so we can safely do this
            self.processes = processes or self.connection.run(Host.getRemoteCPUs)

        # Used by Hive
        # Dict for holding onto the arguments we send off to each host. If we have a failure, we can simply re-send them to another machine.
        self.parameterChunk = None
        # Keeps track of if this host is currently doing work
        self.threadActive = False
        # Number of iterations of any function passed to one of the buzz methods that will be passed to each remote host at a time.
        # i.e. if this is 50, we'll run the function passed to buzz 50 times on each remote host before assigning off another chunk.
        self.bees = bees
        # Amount of seconds after which we'll call it quits on the current set of arguments this host is working on.
        # The args will be put back in the Hivebuzz.parameters queue at this point.
        self.timeout = timeout
        # Timestamp at which we started the current set of arguments
        self.lastStarted = None
        # Timestamp at which the host was noticed as inactive, or the last time we attempted to reconnect and failed
        self.lastInactive = None
        # How long to wait between reconnection attempts
        self.reconnectTime = reconnectTime

    def connect(self, throwaway=None):
        try:
            # Ignore all this if we're dealing with the local machine
            if self.hostname is not 'queen':
                self.connection = Remote(self.hostname, self.username, self.password, port= self.port)
                # If not assigned a number, get the number of cores from the remote machine
                print("Connected to {0}".format(self.hostname))
                self.lastInactive = None
        except Exception as anyException:
            print("{0}@{1}: {2}".format(self.username, self.hostname, anyException))
            self.connection = None
            self.lastInactive = time()

    # Same as Host.connect but happens on a thread so we don't wait around for it
    def reconnect(self):
        if self.lastInactive is None or time() - self.lastInactive > self.reconnectTime:
            self.lastInactive = time()
            print("@debug@ reconnecting {0}".format(self.name))
            pool = Pool(1)
            pool.map_async(self.connect, ["throwaway"])
            pool.close()

class Hive(object):
    def __init__(self, hosts, sleep=1.0, bees=20, timeout= None, reconnectTime= None, queenProcesses=0):
        self.hosts = self.trimUnconnectedHosts(hosts, bees, timeout, reconnectTime)  # List of Host objects that we'll be running code on
        if sleep <= 0:
            raise ValueError("Hive.sleep must greater than 0 (can be a decimal value)")
        self.sleep = sleep  # How long we'll wait between iterations of checking if the hosts are done with their current batch of work
        # Number of processes we want the user's machine to be using in addition to the remote hosts. If zero, we'll only use remote hosts.
        # Always leave one core open for running this main process, dispatching to remotes, etc.
        # Could technically do without this since the processes will figure it out, but won't be as efficient since the main thread might have to wait sometimes.
        if queenProcesses >= os.cpu_count() >= 2:
            queenProcesses = os.cpu_count() - 1
        if queenProcesses >= 1:
            self.hosts.append(Host('queen', processes=queenProcesses))

    def trimUnconnectedHosts(self, hosts, bees, timeout, reconnectTime):
        activeHosts = []
        for host in hosts:
            #if host.connection is not None:
            # See Host.bees; we're filling in any None values here
            if host.bees is None:
                host.bees = bees
            # See Host.bees; if we pass a timeout to Hive and the Host doesn't have one, copy it down
            if timeout is not None and host.timeout is None:
                host.timeout = timeout
            # See Host.bees; if we pass a recconectTime to Hive and the Host doesn't have one, copy it down
            if reconnectTime is not None and host.reconnectTime is None:
                host.reconnectTime = reconnectTime
            activeHosts.append(host)
        return activeHosts

    def buzz(self, function, parameters):
        # Will return all values at the end
        allReturnValues = []
        # Dict to keep track of return values from each host
        returnDict = {}
        # Dict of threads for holding our processes across hosts. Using threads because they're basically just waiting to get return values;
        # we don't have to spawn whole new processes for something IO bound like that.
        threads = {}
        for host in self.hosts:
            threads[host.name] = ThreadPool(1)
            returnDict[host.name] = None

        # Run until we break out when we're done
        while True:
            # Loop over each host
            for host in self.hosts:
                # Try reconnecting if we've been bumped off
                if host.connection is None:
                    pass
                    host.connect()
                # If we have active processes to check on...
                elif host.threadActive:
                    retvals = returnDict[host.name]._value[0]
                    # If we've passed the timeout for this host
                    timedOut = host.timeout is not None and \
                               host.lastStarted is not None and \
                               time() - host.lastStarted > host.timeout

                    # If something went wrong on the remote host or we timed out...
                    if (retvals is not None and type(retvals) is not list) \
                            or timedOut:
                        if timedOut:
                            host.lastStarted = None
                            print("{0} timed out".format(host.name))
                            # Close out the SSH session and open a new one.
                            host.connection.close()
                            host.connection = None
                        else:
                            print('Exception on host {0}: {1}'.format(host.name, retvals))
                            # Close out the SSH session and open a new one.
                            host.connection.close()
                            host.connection = None
                        # Reset returnDict for this host
                        returnDict[host.name] = None
                        # We didn't get results for this chunk of parameters, so add them back to the list and clear it out
                        parameters.extend(host.parameterChunk)
                        host.parameterChunk = None
                        host.threadActive = False
                        # In case there's something wrong with this host, we'll continue on to the next one.
                        # Ensures that we don't caught in a loop where we're trying to execute the last bees number of arguments on a host that isn't working correctly.
                        continue
                    # If things were fine and we have the expected number of results...
                    elif retvals is not None and \
                            len(retvals) == len(host.parameterChunk):
                        #print('@debug@ Chunk completed from {0}'.format(host.name))
                        # save them off to be returned at the end and set this node of the dict back to None
                        allReturnValues += retvals
                        returnDict[host.name] = None
                        host.threadActive = False
                    # Still waiting for the host to finish up
                    elif retvals is None:
                        pass
                # If we need a new set of processes...
                elif len(parameters) > 0 and \
                        not host.threadActive and \
                        returnDict[host.name] is None:
                    #print("@debug@ giving chunk to {0}".format(host.name))
                    # Get the parameters for this chunk. Always grab from the end because it's easier to delete from the back.
                    host.parameterChunk = parameters[-host.bees:]
                    # Delete those parameters from the original list so we don't run over them again
                    del parameters[-host.bees:]
                    host.threadActive = True
                    host.lastStarted = time()
                    # The processes running on this remote host will be kept track of by the thread, returning results back to returnDict[host.name] when it's done
                    returnDict[host.name] = threads[host.name].starmap_async(Hive.sendFunction, [(function, host, host.parameterChunk)])

            # Are we done yet?
            # Have to be all the way through the parameters we were sent
            if len(parameters) == 0:
                # Also have to make sure all the threads are done
                if all(not host.threadActive for host in self.hosts):
                    # We are, in fact, done
                    break
            # Wait some time before checking again
            sleep(self.sleep)
        # Once we have all the results, send them back
        return allReturnValues

    def buzzbuzz(self, function, parameters, localFunction, localProcesses= 1):
        # Pool of processes to handle the execution of the localFunction
        localPool = Pool(processes= localProcesses)
        # Dict to keep track of return values from each host
        returnDict = {}
        # Dict of threads for holding our processes across hosts. Using threads because they're basically just waiting to get return values;
        # we don't have to spawn whole new processes for something IO bound like that.
        threads = {}
        for host in self.hosts:
            threads[host.name] = ThreadPool(1)
            returnDict[host.name] = None

        # Run until we break out when we're done
        while True:
            # Loop over each host
            for host in self.hosts:
                # If we have active processes to check on...
                if host.threadActive:
                    retvals = returnDict[host.name]._value[0]
                    # If something went wrong on the remote host...
                    if retvals is not None and type(retvals) is not list:
                        print('Exception on host {0}: {1}'.format(host.name, returnDict[host.name]))
                        # Reset returnDict for this host
                        returnDict[host.name] = None
                        # We didn't get results for this chunk of parameters, so add them back to the list and clear it out
                        parameters.extend(host.parameterChunk)
                        host.parameterChunk = None
                        host.threadActive = False
                        # In case there's something wrong with this host, we'll continue on to the next one.
                        # Ensures that we don't caught in a loop where we're trying to execute the last bees number of arguments on a host that isn't working correctly.
                        continue
                    # If things were fine and we have the expected number of results...
                    elif retvals is not None and len(retvals) == len(host.parameterChunk):
                        #print('@debug@ Chunk completed from {0}'.format(host.name))
                        # Send values to local function for handling. Wrap retvals in brackets so the whole list gets passed.
                        localPool.map_async(localFunction, [retvals])
                        returnDict[host.name] = None
                        host.threadActive = False
                    elif retvals is None:
                        pass
                        #print("waiting still")
                # If we need a new set of processes...
                if len(parameters) > 0 and not host.threadActive and returnDict[host.name] is None:
                    # Get the parameters for this chunk. Always grab from the end because it's easier to delete from the back.
                    host.parameterChunk = parameters[-host.bees:]
                    # Delete those parameters from the original list so we don't run over them again
                    del parameters[-host.bees:]
                    host.threadActive = True
                    # The processes running on this remote host will be kept track of by the thread, returning results back to returnDict[host.name] when it's done
                    returnDict[host.name] = threads[host.name].starmap_async(Hive.sendFunction, [(function, host, host.parameterChunk)])
                    #returnDict[host.name] = Hive.sendFunction(function, host, host.parameterChunk)


            # Are we done yet?
            # Have to be all the way through the parameters we were sent
            if len(parameters) == 0:
                # Also have to make sure all the threads are done
                if all(not host.threadActive for host in self.hosts):
                    # We are, in fact, done
                    break
            # Wait some time before checking again
            if self.sleep > 0:
                sleep(self.sleep)
        # Done
        return

    # Sends the function to be executed on the remote host
    @staticmethod
    def sendFunction(function, host, parameterChunk):
        try:
            # If this is to be executed on a remote host
            if host.hostname is not 'queen':
                result = host.connection.run(Hive.hostShell, function, host.processes, parameterChunk)
            # If this is happening locally, we don't need host.connection.run
            else:
                result = Hive.localShell(function, host.processes, parameterChunk)
            return result
        # We'll catch any exception back in the buzz, making sure we re-run the function over this parameterChunk
        except Exception as anyException:
            return anyException

    # This is what will be executed on the remote host
    @staticmethod
    def hostShell(function, processes, parameterChunk):
        from multiprocessing import Pool
        pool = Pool(processes=processes)
        results = pool.starmap(function, parameterChunk)
        pool.close()
        # Ensure that we've completed all the processes before sending back a result
        pool.join()
        return results

    # Exact copy of hostShell, except localShell has the if-main protection for Windows. For more info, see these links:
    # https://stackoverflow.com/a/20360812/3875775 (and other answers on this question)
    # https://stackoverflow.com/questions/20222534/python-multiprocessing-on-windows-if-name-main
    @staticmethod
    def localShell(function, processes, parameterChunk):
        #if __name__ == '__main__':
        from multiprocessing import Pool
        pool = Pool(processes=processes)
        results = pool.starmap(function, parameterChunk)
        pool.close()
        # Ensure that we've completed all the processes before sending back a result
        pool.join()
        return results


# ===================================================================================================
# Testing below
# ===================================================================================================
# Very simplistic test of Remote.py to ensure connection

'''
def do_remote_thing(dir='/'):
    import os
    print('I am remote!')
    return os.listdir(dir)

s = Remote('192.168.1.91', 'kevin', '122104', port=22)

try:
    dir_list = s.run(
        do_remote_thing,
        None,
    )
except Exception as e:
    print('I even handle remote exceptions! %s'%e)
    dir_list = None

for dir in dir_list:
    print(dir)
'''

#=======================================================================================================================
# Testing pip installing dependencies on remote machines

'''
host = Remote('192.168.1.91', 'kevin', '122104', port=22)
reqsPath = "C:/Users/theke/Documents/PycharmProjects/BullsOnParadeAlphaVantage/BullsOnParadeAlphaVantage/Controllers/Experimenting/requirements.txt"
host.pipInstall(reqsPath)
'''

#=======================================================================================================================
# Testing moving .py files + classes.
'''
host = Remote('192.168.1.91', 'kevin', '122104', port=22)

pyPath = "C:/Users/theke/Documents/PycharmProjects/BullsOnParadeAlphaVantage/BullsOnParadeAlphaVantage/Controllers/Experimenting/Models/DataSets/IntradayEdgeDataSet.py"
from Controllers.Experimenting.Models.DataSuperClass import DataSuperClass
pyClass = DataSuperClass

host.setupDependency(pyPath)
host.setupDependency(pyClass)
'''

#=======================================================================================================================
# Testing moving static resources to remote machines

'''
host = Remote('192.168.1.91', 'kevin', '122104', port=22)
resourcePath = "C:\\Users\\theke\\Pictures\\Boys at Tetons.jpg"
resourcePathForwardSlashes = "C:/Users/theke/Pictures/Boys at Tetons.jpg"
host.copyOverStaticResource(resourcePathForwardSlashes)
'''

#=======================================================================================================================
# Testing conversion of import statements on remote machines
'''
# codeLines = ["from Controllers.Experimenting.Hive import Hive"]
codeLines = ["import subprocess",
             "import inspect",
             "import pickle",
             "import paramiko",
             "import sys",
             "",
             "#@@@todo: remove",
             "from time import time",
             "",
             "class Remote(object):",
             "    def __init__(self, host, username=None, password=None, port=22, key_filename=None):",
             "        self.host = host",
             "        self.username = username",
             "        self.password = password",
             "        self.ssh = paramiko.SSHClient()",
             "        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())",
             "        self.ssh.connect(",
             "            self.host,",
             "            port = port,",
             "            username=username,",
             "            password=password,",
             "            key_filename=key_filename",
             "        )"]
print(Remote.tryExceptFinallyImports(codeLines))
'''

#=======================================================================================================================
# Simple test of Hive.buzz multiprocessing

'''
# The worker function that will be run a bajillion times
def worker(x, y, z):
    return x + y - z

if __name__ == '__main__':
    # Setup the hive
    hosts = []
    hosts.append(Host('192.168.1.89', username='kevin', password='122104', port=22, processes=2))  # Silver Dell, 2 cores
    hosts.append(Host('192.168.1.94', username='kevin', password= '122104', port=22, processes= 4)) # Nora Dell, 2 cores
    hosts.append(Host('192.168.1.57', username='Kevin', password= 'Arkenemyste7', port=22, processes= 4)) # work MacTop, 4 cores
    hive = Hive(hosts, sleep=0.2, bees=50, queenProcesses=0)

    # Setup parameters to run over the worker function
    parameters = []
    for i in range(0, 500):
        parameters.append((i, i * 3, (i - 3) * 2))
        #retval = worker(i, i*3, (i-3)*2)

    startTime = time()
    # Aaaaaaand we're off
    retvals = hive.buzz(worker, parameters)
    #Hive.localShell(worker, 3, parameters)
    print('Time taken: {0}'.format(time() - startTime))
    # Just to check that all went well
    print('Done')
'''

#=======================================================================================================================
# Simple test of Hive.buzzbuzz multiprocessing w/ local handling of returned results

'''
# The worker function that will be run a bajillion times
def worker(x, y, z):
    return x + y - z

def localHandling(listOfResults):
    i = 0
    for thing in listOfResults:
        print("{0}: {1}".format(i, thing))
        i += 1

if __name__ == '__main__':
    # Setup the hive
    hosts = []
    #hosts.append(Host('192.168.1.89', username='kevin', password= '122104', port=22))
    #hosts.append(Host('192.168.1.90', username='kevin', password= '122104', port=22))
    hosts.append(Host('192.168.1.91', username='kevin', password= '122104', port=22))
    hive = Hive(hosts, sleep=1, bees=50, queenProcesses=0)

    # Setup parameters to run over the worker function
    parameters = []
    for i in range(0, 50):
        parameters.append((i, i * 3, (i - 3) * 2))
        #retval = worker(i, i*3, (i-3)*2)

    startTime = time()
    # Aaaaaaand we're off
    hive.buzzbuzz(worker, parameters, localHandling)
    #Hive.localShell(worker, 3, parameters)
    print('Time taken: {0}'.format(time() - startTime))
    # Just to check that all went well
    print('Done')
'''

#=======================================================================================================================
# Actually moving SQLite resources to remote machines

'''
host = Remote('192.168.1.91', 'kevin', '122104', port=22)
resourcePath = "C:/Users/theke/Documents/PycharmProjects/BullsOnParadeAlphaVantage/BullsOnParadeAlphaVantage/Controllers/IntradayData.db"
host.copyOverStaticResource(resourcePath)
'''
