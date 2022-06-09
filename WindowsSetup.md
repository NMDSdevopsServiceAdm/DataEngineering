# Windows Setup

# Requires IT Assistance

### Uninstall any current Java installations (requires IT)
If you have Java, it will be found in the following file paths:
```
C:\
C:\Program Files
```

### Adding system variables (requires IT)
1. In the search panel, type 'env' and select 'Edit the system environment variables' - this requires IT admin rights

2. In 'system variables', select 'new' and add the following:

Variable name | Value
--- | --- 
HADOOP_HOME | C:\hadoop-3.2.2
JAVA_HOME | C:\Program Files\Java\jdk1.8.0_311


3. Select the 'Path' from 'system variables', press edit and add the below path variables, replacing "_your_name_" for your windows user name:
```
C:\Program Files (x86)\Common Files\Oracle\Java\javapath
C:\hadoop-3.2.2\bin
C:\'Program Files (x86)'\'Common Files'\Oracle\Java\javapath
C:\'Program Files (x86)'\'Common Files'\Java\'Java Update'
C:\Program Files (x86)\Common Files\Oracle\Java\javapath
C:\Program Files\Java\jdk1.8.0_311\bin
C:\Terraform
C:\Users\_your_name_\AppData\Local\Programs\Microsoft VS Code\bin
C:\Users\_your_name_\AppData\Local\Programs\Git\cmd
C:\Users\_your_name_\.pyenv\pyenv-win\bin
C:\Users\_your_name_\.pyenv\pyenv-win\shims
C:\Users\_your_name_\AppData\Roaming\Python\Python39
C:\Users\_your_name_\AppData\Roaming\Python\Python39\Scripts
```

# The rest of the process should not require IT assistance

### Install the following:
Note, all files to be installed using the correct architecture. By default this should be the 64 bit windows versions.

Tool | Windows 
--- | --- 
Python 3.9.6 | https://www.python.org/downloads/
Git | https://github.com/git-guides/install-git
Pyenv | https://github.com/pyenv-win/pyenv-win
Pipenv | https://www.pythontutorial.net/python-basics/install-pipenv-windows/
VS Code | https://code.visualstudio.com/
Github | https://github.com/git-guides/install-git
Terraform Windows Amd64 | https://www.terraform.io/downloads


### Java Development Kit Install:
1. Java Development Kit Version 8 (you might need to create an Oracle account for this)
```
https://www.oracle.com/java/technologies/downloads/#java8-windows
jdk-8u311-windows-x64.exe
```

2. Save the files in the below file path
```
C:\Program Files\Java
```

3. Copy the jdk1.8.0_311 folder from 
```
C:\Program Files\Java
```
and save it in C:\ as
```
C:\jdk
```

4. Within the C:\jdk folder, copy the jre folder and save it in C:\ as:
```
C:\jre
```


### Hadoop Install:
1. Download Hadoop Version 3.2.2 (tar.gz)
```
https://hadoop.apache.org/release/3.2.2.html
```

2. In your Downloads folder, unzip the file to c:\ using 7zip, this will create the below folder:
```
C:\hadoop-3.2.2.tar
```

3. Unzip the .tar file within `c:\hadoop-3.2.2.tar` to `c:\` using 7zip.

4. Your file will error towards the end of unzipping as it will not be able to fully unzip all of the files within the ‘bin’ folder.

5. Press 'close' on 7zip when step 3. occurs

6. On the webpage below, click Code > Download ZIP. This will download the missing binaries which 7zip could not unzip.
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
1. In the search panel, type 'env' and select 'Edit environment variables for your account'

2. Under 'user variables', select 'path' and then select 'edit'

3. Add the below path variables replacing "_your_name_" for your windows user name. 
```
C:\Users\_your_name_\AppData\Local\Programs\Microsoft VS Code\bin
C:\Users\_your_name_\AppData\Local\Programs\Git\cmd 
C:\Users\_your_name_\.pyenv\pyenv-win\bin 
C:\Users\_your_name_\AppData\Roaming\Python\Python39\ 
C:\Users\_your_name_\AppData\Roaming\Python\Python39\Scripts 
```

4. Add the following new variables, replacing "_your_name_" for your windows user name:
Variable name | Value
--- | --- 
PYSPARK_PYTHON | python
PYENV | C:\Users\_your_name_\.pyenv\pyenv-win\
PYENV_ROOT | C:\Users\_your_name_\.pyenv\pyenv-win\


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


### Installing VS code extensions:
From VS Code:<br>
Click ‘Extensions’ in the left-hand pane (the 4 squares)
Search for and install the following:
HashiCorp Terraform
Jupyter
Live Share
Pylance
Python


### Pointing the python extension to the virtual environment:
Type (Ctrl+Shift+P) to open the Command Palette
Search for 'Python: Select Interpreter'
Select: Python 3.9.6 ('DataEngineering-...')

You should now be able to see the Data Engineering project in VS code.


### Json settings in VS code
From VS Code: <br>
Ensure you have the launch.json and settings.json files.
<br>
If not, the code for both can be found below
<br>
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
Replace _your_name_ and _your_code_ in the filepath following "python.formatting.blackPath" with your personal options:
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
    "python.envFile": "${workspaceFolder}/.env",
    "python.linting.flake8Enabled": true,
    "python.linting.enabled": true,
    "python.formatting.provider": "black",
    "python.formatting.blackPath": "C:\\Users\\_your_name_\\.virtualenvs\\DataEngineering-_your_code_\\Scripts\\black.exe",
    "editor.formatOnSave": true,
    "python.linting.pylintEnabled": false,
    "python.formatting.blackArgs": ["--line-length=120"],
    "python.linting.flake8Args": [
        "--max-line-length=120",
        "--ignore=E402,F841,F401,E302,E305,W503",
    ],
    "editor.defaultFormatter": "ms-python.python",
      }
```
