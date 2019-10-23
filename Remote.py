#!/usr/bin/python

import inspect
import pickle
import paramiko
import sys
import os
import shutil
from uuid import uuid4

class Remote(object):
    INDENT_SIZE = 4
    def __init__(self, host, username=None, password=None, port=22, key_filename=None):
        self.host = host
        self.username = username
        self.password = password
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.ssh.connect(
            self.host,
            port=port,
            username=username,
            password=password,
            key_filename=key_filename
        )
        # Assume the machine uses bash. If it doesn't, output is empty and we assume it's a Windows.
        self.os = "Linux"
        # static directory lives in different places on different machine types
        output, errors = self.execute("uname", outputPrefix="os:")
        if "Linux" in output:
            self.os = "Linux"
            self.staticDirectory = "/home/{0}/QueenBee/static".format(self.username)
        # Darwin = Mac
        elif "Darwin" in output:
            self.os = "Mac"
            self.staticDirectory = "/Users/{0}/QueenBee/static".format(self.username)
        # Command failed? Probs a Windows.
        else:
            self.os = "Windows"
            self.staticDirectory = "Users/{0}/QueenBee/static".format(self.username)

    def close(self):
        self.ssh.close()

    def run(self, func, *args, **kwargs):
        tmpfile = self.makeTempDir()
        functionCode = _getNestedFunctionCode(func, args, kwargs)
        code = '\n'.join([
            functionCode,
            inspect.getsource(_wrapper_function).format(
                func.__name__,
                repr(pickle.dumps(args)),
                repr(pickle.dumps(kwargs)),
                repr(tmpfile),
            ),
            "_wrapper_function()",
        ])
        # cd to the QueenBee/static directory before executing so we can take advantage of the static resources there
        self.setupStaticDirectory()

        if sys.version_info[0] == 2:
            input, output, errors = self.ssh.exec_command('cd && cd {0} && python'.format(self.staticDirectory))
        else:
            if self.os is "Mac":
                input, output, errors = self.ssh.exec_command('cd && cd {0} && /usr/local/bin/python3'.format(self.staticDirectory))
            elif self.os is "Linux":
                input, output, errors = self.ssh.exec_command('cd && cd {0} && python3'.format(self.staticDirectory))
            # Windows
            else:
                self.sendCodeAsExecutable(code)
                input, output, errors = self.ssh.exec_command('cd\\ && cd {0} && python executable.py'.format(self.staticDirectory))
        input.write(code)
        #print("@debug@ Wrote code for {0} to remote host".format(func.__name__))
        input.channel.shutdown_write()
        # Get the remote output/errors and print them on the Queen's console
        output, errors = self.printFromRemote(output, errors, prefix= "")
        #print("@debug@ For {0}, should have gotten back print and error information from remote host".format(func.__name__))
        if self.os is not "Windows":
            input, output, errors = self.ssh.exec_command('cat {0};rm {0}'.format(tmpfile))
        else:
            input, output, errors = self.ssh.exec_command('type {0} && del {0}'.format(tmpfile))
        ret = pickle.loads(output.read())
        #print("@debug@ Should have gotten return value for {0}".format(func.__name__))
        if isinstance(ret, Exception):
            raise ret
        return ret

    def makeTempDir(self):
        # Linux + Mac
        if self.os is not "Windows":
            input, output, errors = self.ssh.exec_command('mktemp')
            tempFile = output.read().rstrip().decode('utf-8')
        # Windows requires a whole different process
        else:
            # Get a unique identifer
            uuid = str(uuid4())
            tempFile = "/{0}/{1}.txt".format(self.staticDirectory, uuid)
            makeTempCommand = "type NUL > {0}".format(tempFile)
            self.execute(makeTempCommand)
        return tempFile

    def sendCodeAsExecutable(self, code):
        try:
            # Create a temporary directory on the local machine for our prepped python files
            os.makedirs("/QueenBee/temp")
        except FileExistsError:
            # Nothing to worry about since the filepath already exists
            pass

        fileName = "executable.py"
        with open("/QueenBee/temp/" + fileName, 'w+') as newPyFile:
            newPyFile.write(code)

        self.copyOverDependency("/QueenBee/temp/" + fileName, "executable.py")

    def pipInstall(self, requirementsFilepath):
        self.copyOverStaticResource(requirementsFilepath)

        # Python 2 variant
        if sys.version_info[0] == 2:
            installCommand = "cd {0} && pip install -r requirements.txt".format(self.staticDirectory)
        # Python 3
        else:
            if self.os is "Mac":
                installCommand = "cd {0} && /usr/local/bin/pip3 install -r requirements.txt".format(self.staticDirectory)
            elif self.os is "Linux" or self.os is "Windows":
                installCommand = "cd {0} && pip3 install -r requirements.txt".format(self.staticDirectory)
            else:
                installCommand = ""
        # Install the contents
        self.execute(installCommand)
        # Delete the temporary file
        deleteCommand = "cd {0} && rm requirements.txt".format(self.staticDirectory)
        self.execute(deleteCommand)

    def copyOverStaticResource(self, resourcePath):
        # @@@ todo: implement an "overwrite" parameter to conditionally overwrite or not if the file is already there
        # for right now, overwrite is the default behavior.
        # [ -f /tmp/foo.txt ] || do something if the file isn't already there

        resourceName = Remote.getFileNameFromPath(resourcePath)
        # create the file path
        mkdirCommand = "mkdir -p {0}".format(self.staticDirectory)
        self.execute(mkdirCommand)

        # Write the contents of the local requirements.txt to the remote host
        sftp = self.ssh.open_sftp()
        sftp.put(resourcePath, "{0}/{1}".format(self.staticDirectory, resourceName))
        sftp.close()

    @staticmethod
    def getFileNameFromPath(path):
        # Get the name of the resource. Checks for both forward and back slashes.
        resourceName = path.split('/')[-1:][0]
        return resourceName.split('\\')[-1:][0]

    def setupStaticDirectory(self):
        try:
            # Create directory on remote host for static files
            if self.os is not "Windows":
                self.execute("mkdir -p {0}".format(self.staticDirectory))
            else:
                self.execute('mkdir "{0}"'.format(self.staticDirectory))
        except FileExistsError:
            # Nothing to worry about since the filepath already exists
            pass
        finally:
            # Always want to wind up in this directory
            self.execute("cd {0}".format(self.staticDirectory))

    def setupDependencies(self, dependencies, overwrite=False):
        # dependencies is a list of class references and/or filepaths to .py files.
        # If overwrite = False and the dependency already exists on the host, don't try to copy it over again.
        # Cleans and moves over all dependencies
        for dep in dependencies:
            self.setupDependency(dep, overwrite=overwrite)

    def setupDependency(self, dependency, overwrite=False):
        try:
            # Create a temporary directory on the local machine for our prepped python files
            os.makedirs("/QueenBee/temp")
        except FileExistsError:
            # Nothing to worry about since the filepath already exists
            pass

        # Handle the case where we're passed a class reference
        if isinstance(dependency, type):
            fileName = dependency.__name__ + ".py"
            code = inspect.getsource(dependency)
            # Strip out the whitespace lines; they cause issue when copied into the terminal
            lines = code.split("\n")
            newCode = Remote.tryExceptFinallyImports(lines)
            with open("/QueenBee/temp/" + fileName, 'w+') as classFile:
                classFile.write(newCode)
        # Handle the case where we're passed a .py file
        elif type(dependency) is str:
            fileName = Remote.getFileNameFromPath(dependency)
            with open(dependency) as pyFile:
                lines = [line.rstrip('\n') for line in pyFile]
            newCode = Remote.tryExceptFinallyImports(lines)
            with open("/QueenBee/temp/" + fileName, 'w+') as newPyFile:
                newPyFile.write(newCode)
        else:
            raise TypeError("Dependency {0} is not a Python class or .py file".format(dependency))

        # Send our prepped python file over to the remote host
        self.copyOverDependency("/QueenBee/temp/" + fileName, fileName, overwrite)

        # Clean up (delete)
        shutil.rmtree('/QueenBee/')

    # @@@ todo: combine this and copyOverStaticResource, or at least have them share code
    def copyOverDependency(self, dependency, name, overwrite=False):
        # create the file path
        if self.os is not "Windows":
            mkdirCommand = "mkdir -p {0}".format(self.staticDirectory)
        else:
            mkdirCommand = 'mkdir "{0}"'.format(self.staticDirectory)

        self.execute(mkdirCommand)

        # Write the contents of the dependency file to the remote host
        sftp = self.ssh.open_sftp()
        # Windows is the worst
        if self.os is not "Windows":
            sftp.put(dependency, "{0}/{1}".format(self.staticDirectory, name))
        else:
            sftp.put(dependency, "/{0}/{1}".format(self.staticDirectory, name))
        sftp.close()

    @staticmethod
    def tryExceptFinallyImports(codeLines):
        indentSpaces = Remote.INDENT_SIZE * " "
        newCode = []
        for line in codeLines:
            if "import" in line:
                newLinePieces = []
                leadingSpaces = len(line) - len(line.lstrip())
                line = line.lstrip()
                leadingSpaces = " " * leadingSpaces
                linePieces = line.split(" ")
                for piece in linePieces:
                    # For each space-delimited piece of the line, cutout anything that has a period after it.
                    # Turns "from Controllers.Experiments.Hive import Hive" into "from Hive import Hive"
                    newLinePieces.append(piece.split(".")[-1:][0])
                tryExceptLine = " ".join(newLinePieces)

                newLine = leadingSpaces + "try: \n"
                newLine += leadingSpaces + indentSpaces + line + "\n"
                newLine += leadingSpaces + "except: \n"
                newLine += leadingSpaces + indentSpaces + "try: \n"
                newLine += leadingSpaces + indentSpaces * 2 + tryExceptLine + "\n"
                newLine += leadingSpaces + indentSpaces + "except: \n"
                newLine += leadingSpaces + indentSpaces * 2 + "raise ImportError('Something went wrong executing the line \"{0}\"')".format(line.lstrip())
            else:
                newLine = line
            newCode.append(newLine)

        return "\n".join(newCode)

    def execute(self, command, outputPrefix=""):
        # Always return to the root directory before trying anything else
        if self.os is not "Windows":
            self.ssh.exec_command("cd")
        else:
            self.ssh.exec_command("cd\\")
        # Do the actual command
        input, output, errors = self.ssh.exec_command(command)
        output, errors = self.printFromRemote(output, errors, outputPrefix)
        return output, errors

    def printFromRemote(self, output, errors, prefix= ""):
        # Get the remote output/errors and print them on the Queen's console\
        decodedOutput, decodedErrors = output.read().decode('utf-8'), errors.read().decode('utf-8')
        if decodedOutput is not None and decodedOutput is not "" and not decodedOutput.isspace():
            # Gets printed output to the remote console and prints it locally
            #sys.stdout.write("{0}@{1}: ".format(self.username, self.host) + output.read().decode('utf-8'))
            print("{0}@{1}: {2} ".format(self.username, self.host, prefix) + decodedOutput)
        if decodedErrors is not None and decodedErrors is not "" and not decodedErrors.isspace():
            # Gets errors from the remote host and prints them locally
            sys.stderr.write("{0}@{1}: {2} ".format(self.username, self.host, prefix) + decodedErrors)

        return decodedOutput, decodedErrors

def _wrapper_function():
    import sys
    import os
    import pickle
    args = pickle.loads({1})
    kwargs = pickle.loads({2})
    tmpfile = {3}
    try:
        ret = {0}(*args, **kwargs)
    except Exception as e:
        ret = e
    with open(tmpfile, "wb") as f:
        pickle.dump(ret, f)


def _dependenciesToCode(dependencies):
    cleanLines = []
    if dependencies is None:
        return ""
    # Get all the dependency code
    for dependency in dependencies:
        code = inspect.getsource(dependency)
        # Strip out the whitespace lines; they cause issue when copied into the terminal
        lines = code.split("\n")
        for line in lines:
            # If the line isn't just whitespace...
            if not line.isspace() and not len(line) == 0:
                # And it isn't a local import statement
                if not line.startswith("from") and not line.startswith("import"):
                    cleanLines.append(line)
        # The trailing \n ensures that each class/function gets its own code block. Otherwise, python prompt could think that
        # a function is attempting to belong to a class.
        cleanLines.append("\n")

    # The trailing \n ensures that each class/function gets its own code block. Otherwise, python prompt could think that
    # a function is attempting to belong to a class.
    return "\n".join(cleanLines)


def _getNestedFunctionCode(func, args=None, kwargs=None):
    # Code for this function only. Use the \n\n to keep spacing that the Python prompt likes between functions outside of classes.
    myCode = _cleanFunctionSource(inspect.getsource(func)) + "\n\n"
    # Will hold all the code from our recursive calls
    allCode = ''
    if args is not None:
        for arg in args:
            if callable(arg):
                # Recurse
                allCode += _getNestedFunctionCode(arg)
                # Get the name of the parameter that holds the function
                parameterFunctionName = inspect.getfullargspec(func).args[0]
                # Replace all instances with the actual name of the function we're trying to call
                myCode = myCode.replace(parameterFunctionName, arg.__name__)
                # Replace the first one (which should be the parameter) with the actual parameter name again
                myCode = myCode.replace(arg.__name__, parameterFunctionName, 1)
    if kwargs is not None:
        for kwarg in kwargs.values():
            if callable(kwarg):
                # Recurse
                allCode += _getNestedFunctionCode(kwarg)
                # Get the name of the parameter that holds the function
                parameterFunctionName = inspect.getfullargspec(func).args[0]
                # Replace all instances with the actual name of the function we're trying to call
                myCode = myCode.replace(parameterFunctionName, kwarg.__name__)
                # Replace the first one (which should be the parameter) with the actual parameter name again
                myCode = myCode.replace(kwarg.__name__, parameterFunctionName, 1)

    return myCode + allCode


def _cleanFunctionSource(funcSource):
    funcLines = funcSource.split("\n")
    leadingSpaces = len(funcLines[0]) - len(funcLines[0].lstrip(' '))
    stripSequence = funcLines[0][0:leadingSpaces]
    cleanFuncLines = []

    # Check the lines for various bad things
    for line in funcLines:
        # If this exists in a class and is a static method, strip the decorator
        if '@staticmethod' in line:
            line = ''
        # If there are leading spaces in the method header
        elif leadingSpaces > 0:
            if line.startswith(stripSequence):
                # Strip out that number of spaces for each line
                line = line[leadingSpaces:]
        # If the line isn't just whitespace, we care about it
        if not line.isspace() and not len(line) == 0:
            cleanFuncLines.append(line)

    cleanFunction = Remote.tryExceptFinallyImports(cleanFuncLines) #'\n'.join(cleanFuncLines) #

    return cleanFunction
