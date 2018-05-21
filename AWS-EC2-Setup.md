# Setting up Pyspark on AWS EC2 Instance

## Provisioning EC2 instance

1. Login to AWS, select EC2 from services and click on ```Launch Instance``` button.
2. Select ```Ubuntu Server ***XXX---$$$``` AMI, which is free tier eligible.
3. Select ```t2.micro``` instance with all the default settings, add tags and generate a key pair for connecting through cli.
4. Make sure you have the key pair (.pem file) in your local, hit launch instance.

## Connecting to the Instance through cli
1. Open Terminal window and navigate to the path, where you have stored the key pair (.pem file).
2. To connect to the instance through SSH type the following command:
```
$ssh -i <YOURKEYPAIR.pem> ubuntu@<PUBLIC DNS FOR THE EC2 INSTANCE>
```
Now you are at the command line interface of your EC2 instance.

To test if you AMI is setup correctly, type ```python3``` in the cli, and you will be directed to the python cli.

## Installing PySpark and Dependencies on EC2
**1. Update all the packages in the AMI:**
```
$ sudo apt-get update
```
**2. Install pip3, which will help us install python3 packages**
```
$ sudo apt install python3-pip
```
**3. Install Jupyter notebook**
```
$ pip3 install jupyter
```

**4. Install Java**
```
$ sudo apt-get install default-jre
```
To check if Java is installed correctly, type the following command:
```
$ java -version
```
It will show the current version of Java on the AMI

**5. Install Scala**
```
$ sudo apt-get install scala
```
To check if Scala is installed correctly, type the following command:
```
$ scala -version
```
It will shoe the version of the scala on the EC2

**6. Install [Py4J](https://www.py4j.org/) library for connecting python to java**
```
$ pip3 install py4j
```

**7. Installing Spark**

Get the spark file from apache.org archives
```
$ wget http://archive.apache.org/dist/spark/spark-2.1.1/spark-2.1.1-bin-hadoop2.7.tgz
```
Unzip the downloaded file, this command will install spark in the EC2 instance
```
$ sudo tar -zxvf spark-2.1.1-bin-hadoop2.7.tgz
```

**8. Take a note of the spark installation directory for future use**

Go to the directory and type pwd:
```
$ cd spark-2.1.1-bin-hadoop2.7
$ pwd
/home/ubuntu/spark-2.1.1-bin-hadoop2.7
$ cd    # go back to home dir
```
We will need this path while connecting via jupyter notebook

**9. Install [findspark](https://github.com/minrk/findspark) module.**

PySpark isn't on sys.path by default. It helps us to connect python with spark very easily.
```
$ pip3 install findspark
```

**10. Generate a configuration file for jupyter notebook**
```
$ jupyter notebook --generate-config
```
It will generate a jupyter notebook config file inside the following directory
```/home/ubuntu/.jupyter/jupyter_notebook_config.py```

Now create a directory called ```certs``` inside the home directory
```
$ cd
$ mkdir certs
$ cd certs
$ sudo openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mycert.pem -out mycert.pem
# hit enter and fill the details such as country, state, city etc., if you want to
```
It will create pem files which we will use for jupyter configuration files

Go to the hidden direcory of the jupyter configuration file and add the configurations:
```
$ cd ~/.jupyter
$ vi jupyter_notebook_config.py
```
Insert the following lines on the top of the file:
```{python}
c = get_config()
c.NotebookApp.certfile = u'/home/ubuntu/certs/mycert.pem'
c.NotebookApp.ip = '*'
c.NotebookApp.open_browser = False
c.NotebookApp.port = 8888
```
The first 10 lines of the file should look like this:
```{python}
# Configuration file for jupyter-notebook.
c = get_config()
c.NotebookApp.certfile = u'/home/ubuntu/certs/mycert.pem'
c.NotebookApp.ip = '*'
c.NotebookApp.open_browser = False
c.NotebookApp.port = 8888
#------------------------------------------------------------------------------
# Application(SingletonConfigurable) configuration
#------------------------------------------------------------------------------
```

Now the Jupyter notebook is setup. Go to the home directory.
```
$ cd
```

**11. Running the jupyter notebook**
```
$ jupyter notebook
```
Running this command will start serving jupyter notebook at your EC2 instance, it will display a URL on the cli to connect to the jupyter notebook, the url will look something like this:

```https://localhost:8888/?token=5kjrkn4f9fvjbsi9ufv4jhjf3uhb47342jhbjhb4```

You need to replace the localhost in the above url with the public DNS of your EC2 instance, which will look something like this:

```https://<YOUR EC2 PUBLIC DNS>:8888/?token=5kjrkn4f9fvjbsi9ufv4jhjf3uhb47342jhbjhb4```

```https://<ec2-33-33-33-33.compute-9.amazonaws.com>:8888/?token=5kjrkn4f9fvjbsi9ufv4jhjf3uhb47342jhbjhb4```

Copy and paste the above URL into the browser and you will be able to access jupyter notebook system.

To load pyspark into your jupyter notebook, type the following lines of code:
```{python}
import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')
import pyspark
```
