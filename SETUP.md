# Local Environment Setup

## Prerequisites
| Tool | Windows | Mac/Linux |
|------|---------|-----------|
| Python 3.9.6 | https://www.python.org/downloads/ | https://www.python.org/downloads/ |
| Git | https://github.com/git-guides/install-git | https://github.com/git-guides/install-git |
| Pyenv | https://github.com/pyenv-win/pyenv-win | https://github.com/pyenv/pyenv |
| Pipenv | https://www.pythontutorial.net/python-basics/install-pipenv-windows/ | https://pipenv-fork.readthedocs.io/en/latest/install.html |
| Java JDK8 | https://www.java.com/en/download/ | https://www.java.com/en/download/ |

## MacOS Java Installation
This project is using jdk8. We recommend using Brew (https://brew.sh) to install the java development kit. This project is using **jdk8**.
```
brew update
brew install adoptopenjdk8
```

## Project setup
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
cd DataEngineering
pipenv install --dev
pipenv shell
```

To exit the environment
```
exit
```
_Do not use `deactivate` or `source deactivate`_

For detailed Windows setup, see [WindowsSetup.md](https://github.com/NMDSdevopsServiceAdm/DataEngineering/blob/main/WindowsSetup.md)
