# Local Environment Setup

## Prerequisites
| Tool | Windows | Mac/Linux |
|------|---------|-----------|
| Python 3.11.9 | https://www.python.org/downloads/ | https://www.python.org/downloads/ |
| Git | https://github.com/git-guides/install-git | https://github.com/git-guides/install-git |
| uv | https://docs.astral.sh/uv/getting-started/installation/ | https://docs.astral.sh/uv/getting-started/installation/ |
| Java JDK8 | https://www.java.com/en/download/ | https://www.java.com/en/download/ |

## MacOS Java Installation
This project currently uses SPARK_VERSION 3.5.4 as this is compatible with Python 3.11 and Glue 5.0.

For Spark installation, note that several JDKs can be used with Spark 3.5, see the guide on [Spark 3.5.4](https://spark.apache.org/docs/3.5.4/)

Curently, users are using jdk8. We recommend using Brew (https://brew.sh) to install the java development kit. This project is using **jdk8**.
```
brew update
brew install adoptopenjdk8
```

## Project setup (Mac/Linux)
This is currently untested as we have no Mac/Linux users on the team.
```
git clone https://github.com/NMDSdevopsServiceAdm/DataEngineering.git
cd DataEngineering
curl -LsSf https://astral.sh/uv/install.sh | sh
uv sync
```


For detailed Windows setup, see [WindowsSetup.md](https://github.com/NMDSdevopsServiceAdm/DataEngineering/blob/main/WindowsSetup.md)
