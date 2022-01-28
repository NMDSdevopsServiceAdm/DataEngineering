# Windows Setup

# Prerequisite installs:
Note, all files to be installed using the correct architecture. By default this should be the 64 bit windows versions.

Tool | Windows 
--- | --- 
Python 3.7.3 | https://www.python.org/downloads/
Git | https://github.com/git-guides/install-git
Pyenv | https://github.com/pyenv-win/pyenv-win
Pipenv | https://www.pythontutorial.net/python-basics/install-pipenv-windows/
VS Code | https://code.visualstudio.com/
Github | https://github.com/git-guides/install-git
Hadoop Version 3.2.2 | https://hadoop.apache.org/release/3.2.2.html
Java Development Kit Version 8 | https://www.oracle.com/java/technologies/downloads/#java8-windows

### Java Development Kit Install:
1. Delete any instances of Java (JRE) which are already installed, these can be found in the file paths:
```
C:\
C:\Program Files
```

2. Download JDK Version 8
```
jdk-8u311-windows-x64.exe
```

3. Save the files in the below file path
```
C:\Program Files\Java
```

4. Copy the jdk1.8.0_311 folder from 
```
C:\Program Files\Java
```
and save it in C:\ as
```
C:\jdk
```

5. Within the C:\jdk folder, copy the jre folder and save it in C:\ as:
```
C:\jre
```


### Hadoop Install:
1. Download the tar.gz file from the url provided above

2. Unzip the file to c:\ using 7zip, this will create the below folder:
```
C:\hadoop-3.2.2.tar
```

3. Unzip the .tar file within `c:\hadoop-3.2.2.tar` to `c:\` using 7zip.

4. Your file will error towards the end of unzipping as it will not be able to fully unzip all of the files within the ‘bin’ folder.

5. Press 'close' on 7zip when step 4. occurs

6. Navigate to the winutils github repo below. Here you can download the missing binaries that will allow hadoop to work on Windows. Click code > download zip. This will download the missing binaries which 7zip could not unzip.
```
https://github.com/cdarlint/winutils
```

7. Copy and paste all files from the below folder:
```
Downloads\winutils-master.zip\winutils-master\hadoop-3.2.2\bin
```
to
```
C:\hadoop-3.2.2\bin
```

8. Press 'replace' if any files have the same names. Hadoop is now installed.


### Adding user variables:
1. Go to 'control panel' and type 'edit environment variables for your account'

2. Under 'user variables', select 'path' and then select 'edit'

3. Add the below path variables replacing "awheatley" for your windows user name. 
```
C:\Users\awheatley\AppData\Local\Programs\Microsoft VS Code\bin
C:\Users\awheatley\AppData\Local\Programs\Git\cmd 
C:\Users\awheatley\.pyenv\pyenv-win\bin 
C:\Users\awheatley\AppData\Roaming\Python\Python37\site-packages 
C:\Users\awheatley\AppData\Roaming\Python\Python37\Scripts 
```


### Adding system variables - In Progress:
1. In 'system variables', select 'new' and add the below:

Variable name | Value
--- | --- 
HADOOP_HOME | C:\hadoop-3.2.2


2. Select the 'Path' from 'system variables', press edit and add the below path variables:
PATH | 
--- | 
C:\Program Files (x86)\Common Files\Oracle\Java\javapath |
C:\Users\awheatley\.pyenv\pyenv-win\bin |
C:\ProgramData\Anaconda3 |
C:\ProgramData\Anaconda3\Library\mingw-w64\bin |
C:\ProgramData\Anaconda3\Library\usr\bin |
C:\ProgramData\Anaconda3\Library\bin |
C:\ProgramData\Anaconda3\Scripts |
C:\WINDOWS\system32 |
C:\WINDOWS |
C:\WINDOWS\System32\Wbem |
C:\WINDOWS\System32\WindowsPowerShell\v1.0\ |
C:\Program Files (x86)\Enterprise Vault\EVClient\ |
C:\Program Files\IBM\SPSS\Statistics\25\JRE\bin |
C:\WINDOWS\System32\OpenSSH\ |
C:\hadoop-3.2.2\bin |
C:\Users\awheatley\AppData\Local\Microsoft\WindowsApps |
C:\Users\awheatley\AppData\Local\Programs\Microsoft VS Code\bin |
C:\Users\awheatley\AppData\Local\Programs\Git\cmd |
C:\Users\awheatley\.pyenv\pyenv-win\bin |
C:\Users\awheatley\AppData\Roaming\Python\Python37\site-packages |
C:\Users\awheatley\AppData\Roaming\Python\Python37\Scripts |



### Clone the project
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
```


### Create virtual environment and install dependencies
```
cd DataEngineering
pipenv install --dev
```


### Opening the data engineering project in vscode
From VS Code: <br>
File> Open Folder> Navigate to ‘Data Engineering’ and press ‘Select Folder’


### Installing python on VS code:
From VS Code:<br>
Click ‘Extensions’ in the left-hand pane > type in ‘Python’> click ‘Install’ on ‘Python’
You can now run python in VS code


### Json settings in VS code
From VS Code: <br>
Ensure you have the launch.json and settings.json files.
<br>
If not, the code for both can be found below
<BR> <br>
#### launch.json
```
{
    // Use IntelliSense to learn about possible attributes.
    // Hover to view descriptions of existing attributes.
    // For more information, visit: https://go.microsoft.com/fwlink/?linkid=830387
    "version": "0.2.0",
    "configurations": [
        {
            "name": "Python: Current File",
            "type": "python",
            "request": "launch",
            "program": "${file}",
            "console": "integratedTerminal",
            "env": {
                "PYTHONPATH": "${workspaceRoot}"
            }
        }
    ]
}

```
<br>
    
#### settings.json
```
{
    "python.testing.unittestArgs": [
        "-v",
        "-s",
        "./tests",
        "-p",
        "test*.py"
    ],
    "python.testing.pytestEnabled": false,
    "python.testing.unittestEnabled": true,
    "python.envFile": "${workspaceFolder}/.env"
     }
```
