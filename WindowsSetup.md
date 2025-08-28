# Windows Setup

# Requires IT Assistance

### Uninstall any current Java installations (requires IT)
If you have Java, it will be found in the following file paths:
```
C:\
C:\Program Files
```

### Java Development Kit Install (requires IT)
1. Java Development Kit Version 8 (you might need to create an Oracle account for this)
```
https://www.oracle.com/java/technologies/downloads/#java8-windows
jdk-8u....-windows-x64.exe
```

2. Save the files in `C:\Program Files\Java`

3. Rename the `jdk1.8...` folder to `jdk`

4. Rename the `jre1.8...` folder to `jre`

5. Cut both these folders and paste directly into your C: drive so it looks like `C:\jdk` and `C:\jre`


### Downloading JDBC driver to connect AWS Athena to Tableau (requires IT)

1. Install JDBC driver on your pc - version JDBC 2.x with AWS SDK. The file name is AthenaJDBC42-2.1.5.1000.jar
`https://docs.aws.amazon.com/athena/latest/ug/connect-with-jdbc.html`

2. Save here: `C:\Program Files\Tableau\Drivers`

3. Open a new Tableau document. On the left-hand side, select "Amazon Athena" from the "Connect'" menu under "To a server".

4. This will open a screen where you need to add credentials into the fields:

Field | To be filled with
--- | --- 
Server | athena.eu-west-2.amazonaws.com
Port | 443
S3 Staging Directory | s3://skillsforcare/tableau-staging-directory/
Access Key ID | Tableau user AWS access key from the AWS credentials page. Around 20 characters long.
Secret Access Key | Tableau user AWS secret key from the AWS credentials page. Around 40 characters long.

5. Once connected select 'main-data-engineering-database' from the 'Database' dropdown in Tableau. All of the tables should now appear and are now available to analyse. Other databases can be selected.


### AWS Command Line Interface (AWS CLI) (requires IT)
1. Download and run the AWS CLI MSI installer for Windows (64-bit):
`https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html`


### Adding system variables (requires IT)
1. In the search panel, type 'env' and select 'Edit the system environment variables' - this requires IT admin rights

2. In 'system variables', select 'new' and add the following:

Variable name | Value
--- | --- 
HADOOP_HOME | C:\hadoop-3.2.2
JAVA_HOME | C:\jdk


3. Select the 'Path' from 'system variables', press edit and add the below path variables, replacing "_your_name_" for your windows user name:
```
C:\Program Files (x86)\Common Files\Oracle\Java\javapath
C:\hadoop-3.2.2\bin
C:\'Program Files (x86)'\'Common Files'\Oracle\Java\javapath
C:\'Program Files (x86)'\'Common Files'\Java\'Java Update'
C:\Program Files (x86)\Common Files\Oracle\Java\javapath
C:\jdk\bin
C:\Terraform
C:\Users\_your_name_\AppData\Local\Programs\Microsoft VS Code\bin
C:\Users\_your_name_\AppData\Local\Programs\Git\cmd
C:\Users\_your_name_\.pyenv\pyenv-win\bin
C:\Users\_your_name_\.pyenv\pyenv-win\shims
C:\Users\_your_name_\AppData\Roaming\Python\Python39
C:\Users\_your_name_\AppData\Roaming\Python\Python39\Scripts
```

### Adding user variables (requires IT):
1. In the search panel, type `env` and select `Edit environment variables for your account`

2. Under 'user variables', select `path` and then select `edit`

3. Add the below path variables replacing "_your_name_" for your windows user name. 
```
C:\Users\_your_name_\AppData\Local\Programs\Microsoft VS Code\bin
C:\Users\_your_name_\AppData\Local\Programs\Git\cmd
C:\Users\_your_name_\.pyenv\pyenv-win\bin
C:\Users\_your_name_\AppData\Roaming\Python\Python39\
C:\Users\_your_name_\AppData\Roaming\Python\Python39\Scripts
%HADOOP_HOME%\bin
```

4. Add the following new variables, replacing "_your_name_" for your windows user name:

Variable name | Value
--- | ---
PYSPARK_PYTHON | python
PYENV | C:\Users\_your_name_\.pyenv\pyenv-win\
PYENV_ROOT | C:\Users\_your_name_\.pyenv\pyenv-win\
PYENV_HOME | C:\Users\_your_name_\.pyenv\pyenv-win\


### Python install (requires IT):
1. If a version of Python which is not 3.9 is already installed on your computer then you will need to uninstall it

2. Go to https://www.python.org/downloads/

3. Select Ctrl+F and search for `3.9.6`

4. Click on the `Download` button

5. Scroll down and select `Windows installer (64-bit)`

6. In your download folder, click on `python-3.9.6-amd64` and install Python manually (in order to change the file location to the location below)

7. When asked for a location, choose `C:\Users\_your_name_\AppData\Roaming\Python\Python39`

**Note:** When you set the location for python to install, make sure it matches to the system and user variables for python path we set in the previous step.

# The rest of the process should not require IT assistance

### Hadoop Install:
1. Download Hadoop Version 3.2.2 (tar.gz)
https://hadoop.apache.org/release/3.2.2.html

2. In your Downloads folder, right click on `hadoop-3.2.2.tar.gz`, select 7-Zip and Extract Here

3. Right click the extracted `hadoop-3.2.2.tar`, select 7-Zip, Extract files and extract to `C:\` using 7zip.

4. The file will error towards the end of unzipping, this is expected. Press close when this happens

6. On the webpage below, click Code > Download ZIP
https://github.com/cdarlint/winutils

7. In your Downloads folder, right click on `winutils-master`, select 7-Zip and Extract Here

8. Copy and paste all files from `Downloads\winutils-master\hadoop-3.2.2\bin` into `C:\hadoop-3.2.2\bin`

9. Press 'replace' if any files have the same names. Hadoop is now installed.


### Git install:
<i>Note: The following pyenv instructions are from https://github.com/git-guides/install-git</i>

1. Go to https://gitforwindows.org/ and select `Download` and follow the instructions

2. Open windows command prompt (type `cmd` in the Windows search panel and Enter)

3. Type `git version` and Enter to verify Git was installed correctly

4. If successful you should see something like `git version 2.38.1.windows.1`

5. Set your username: `git config --global user.name "First_Name Last_Name"`

6. Set your email address: `git config --global user.email "Your.Name@skillsforcare.org.uk"`

5. Leave command prompt open for the next install


### Pyenv install:
<i>Note: The following pyenv instructions are from https://github.com/pyenv-win/pyenv-win</i>

1. Paste `pip install pyenv-win --target %USERPROFILE%\\.pyenv` and click Enter

2. You should get the message `Successfully installed pyenv-win-3.1.1`

3. If you having an error on above command, try `pip install pyenv-win --target %USERPROFILE%\\.pyenv --no-user --upgrade`

4. Run `pyenv install 3.9.6` to install the supported version

5. Run `pyenv global 3.9.6` to set a Python version as the global version

6. Check which Python version you are using and its path using `pyenv version`
Output: `3.9.6 (set by C:\Users\.....\.pyenv\pyenv-win\version)`

7. Check that Python is working using `python -c "import sys; print(sys.executable)"`
Output: `C:\Users\.....\.pyenv\pyenv-win\versions\3.9.6\python.exe`

8. Leave command prompt open for the next install


### pipenv install:
<i>Note: The following pipenv instructions are from https://www.pythontutorial.net/python-basics/install-pipenv-windows/</i>

1. Before installing the pipenv tool, you need to have Python and pip installed on your computer. You can check these by entering
`python -V` - which should return 'Python 3.9.6'
`pip -V` - which should return 'pip xx.x from c:\users\.....'

2. Paste `pip install pipenv` and click Enter

3. Check pipenv has been installed correctly using `pipenv --version`

4. If successful you should see something like `pipenv, version yyyy.mm.dd`

5. If unsuccessful, check your PATH environment variables

6. Leave command prompt open for the next install


### Install boto3
1. Enter `pip install boto3` into command prompt

2. Close command prompt


### Install Terraform
1.  Download Terraform from https://www.terraform.io/downloads (Amd64 version)

2. In your Downloads folder, right click on `terraform_1.3.3_windows_amd64`, select 7-Zip and Extract Here

3. Copy the file named `terraform` and paste here `C:\Terraform` (you'll need to create this folder)


### Clone the project
1. Search for `Git Bash` in the Windows search panel and Enter

2. Type `git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git` and Enter

3. Keep Git Bash open


### Create virtual environment and install dependencies
1. In Git Bash, enter `cd DataEngineering`

2. Enter `pipenv install --dev`

3. Close Git Bash


### Install and set up VS Code
1. Download VS Code from https://code.visualstudio.com/

2. Open the data engineering project in vscode `File> Open Folder`, select `DataEngineering` then `Select Folder`

3. Select `Extensions` (the 4 squares) in the left-hand pane. Search for and install the following:
```
HashiCorp Terraform
Jupyter
Live Share
Pylance
Python
```

4. Point the python extension to the virtual environment by typing `Ctrl+Shift+P` to open the Command Palette

5. Search for `Python: Select Interpreter` and select: `Python 3.9.6 ('DataEngineering-...')`

6. You should now be able to see the Data Engineering project in VS code.

7. Add `launch.json` and `settings.json` files. Right click in the `EXPLORER` space under the DATAENGINEERING files and folder and select `New folder...`

8. Name the folder `.vscode`

9. Right click on this folder, add a file named `launch.json` and paste in the code below:
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
    
10. Right click on the folder again, add a file named `settings.json` and paste in the code below:
<i>Note: Replace `_your_name_` and `_your_code_` in the filepath with your personal options:</i>
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


### Setting permissions in VS Code
1. In VS Code, go to `View > Terminal`

2. You might see an error message referring to permissions. If so, enter`Get-ExecutionPolicy -List` in the terminal

3. If CurrentUser is set to 'Undefined' then enter `Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser`

4. Enter`Get-ExecutionPolicy -List` again to check the change has been made

5. Close and re-open VS Code and this error message should no longer appear


### Install black

1. In the terminal, type `pip install black` and Enter


### aws credentials
1. Open cmd

2. Type `aws configure`

3. Add your aws access key and secret access key
4. Region: eu-west-2
5. Default output format: json


### Set up your AWS crendentials as terraform variables
1. In the terminal, type `cp terraform/pipeline/terraform.tfvars.example terraform/pipeline/terraform.tfvars` and Enter

2. In the explorer section, go to `terraform > pipeline > terraform.tfvars` and enter your aws access key and secret access key.

3. If you do not have these to hand you can generate new ones by following these steps
`https://docs.aws.amazon.com/IAM/latest/UserGuide/id_credentials_access-keys.html`

4. Ensure you're in the Terraform directory `cd terraform/pipeline`

5. Run `terraform plan` to evaluate the planned changes

6. Check the planned changes to make sure they are correct!

7. Run `terraform apply` to deploy the changes. Confirm with `yes` when prompted

8. Enter `C:\Users\_your_name_\DataEngineering` to change back to data engineering directory
