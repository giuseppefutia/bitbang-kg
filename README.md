# bitbang-kg
Course materials on "Knowledge Graphs" for BitBang.

### How to use the code this repository

Make sure that the Neo4j instance you want to use is up and running.

Update the [config.ini](config.ini) file with the relevant neo4j credentials.

It is recommended to set up a Python virtual environment for this project. For example:
```shell
$ python -m venv venv
```

Unless otherwise stated, the code in this repo is tested with Python version 3.8/3.9/3.10.

Modules make use of a `MakeFiles` based approach to simplfy operations, make sure you can run 
the make command:

```shell
$ make -version
```

Generally the `GNU make` is available on many package managers for a wide range of OSes.

```shell
$ choco install make # Windows Os
$ apt install make # Debian & derivated OSes (including ubuntu)
$ yum install make # Centos 
```

macOS's users should have `make` available through XCode - Command line tools:

```shell
$ xcode-select --install
```
alteratively  `GNU Make` can be installed via brew
```shell
brew install make
```

For further information, please refere to the README.md available in each modules's (MD) directory.

