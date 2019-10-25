from Remote import Remote 
# ===================================================================================================
# Testing below
# ===================================================================================================
# Very simplistic test of Remote.py to ensure connection
# prints (to the local machine) a list of the directories at the root of the remote host if successful 

'''
def do_remote_thing(dir='/'):
    import os
    print('I am remote!')
    return os.listdir(dir)

s = Remote('123.456.7.89', 'username', 'password', port=22)

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
host = Remote('123.456.7.89', 'username', 'password', port=22)
reqsPath = "C:/filepath/requirements.txt" # Normal python requirements.txt file 
host.pipInstall(reqsPath) # will install all python packages from reqsPath
'''

#=======================================================================================================================
# Testing moving .py files + classes.
'''
host = Remote('123.456.7.89', 'username', 'password', port=22)

pyClass = "C:/filepath/myPythonClass.py"

# Moves a copy of the class to the remote machine, doing some extra steps along the way to ensure it works when accessed from the 
# ssh prompt 
host.setupDependency(pyClass) 
'''

#=======================================================================================================================
# Testing moving static resources to remote machines

'''
host = Remote('123.456.7.89', 'username', 'password', port=22)
resourcePath = "C:/pics/myPicture.jpg"

# Similar to setupDependency, but moves a non-python file over to the remote host for use by your program 
host.copyOverStaticResource(resourcePath)
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
    hosts.append(Host('123.456.7.89', username='username', password='password', port=22, processes=2))  # 2 core computer, so we run 2 processes
    hosts.append(Host('987.654.3.21', username='username', password='password', port=22, processes= 4)) # 2 cor3e computer, demonstrating that we can technically run as many process as we want
    hosts.append(Host('741.852.9.63', username='username', password='password', port=22, processes= 4)) # 4 core computer 
    
    hive = Hive(hosts, sleep=0.2, bees=50, queenProcesses=0)

    # Setup arguments to run over the worker function
    parameters = []
    for i in range(0, 500):
        parameters.append((i, i * 3, (i - 3) * 2)) # Generate a bunch of arguments 
        #retval = worker(i, i*3, (i-3)*2)

    # One magical line of code 
    retvals = hive.buzz(worker, parameters)
    
    # Do whatever you want with the results
    for ret in retvals: 
        # ... 
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
    hosts.append(Host('123.456.7.89', username='username', password='password', port=22))
    hosts.append(Host('123.456.7.89', username='username', password='password', port=22))
    hosts.append(Host('123.456.7.89', username='username', password='password', port=22))
    hive = Hive(hosts, sleep=1, bees=50, queenProcesses=0)

    # Setup arguments to run over the worker function
    parameters = []
    for i in range(0, 50):
        parameters.append((i, i * 3, (i - 3) * 2))
    
    # one magical line of code 
    hive.buzzbuzz(worker, parameters, localHandling)
'''
