ARG AIRFLOW_VERSION=2.9.2
ARG PYTHON_VERSION=3.10

FROM apache/airflow:${AIRFLOW_VERSION}-python{PYTHON_VERSION}
#next we define the airflow home enviroment variable,which is the directory which will contain
#the main important airflow folders and files like the DAGs and logs folders and obviosly the airflow
ENV AIRFLOW_HOME=/opt/airflow

#now at this stage we can define the requirements.txt , which will contain all the extra pakages that we want the image to have , that doesn't come with the base airflow image

#=> to copy all the requirements from the requirements.txt here: 
COPY requirements.txt / 
#there we are seesntially saying to copy the requirements.txt file from our local directory tp the root directory 
#of the docker images file system


#finally we use the run command which install the specified version of airflow and the packages in requirements.txt
#using pip install 

RUN pip install --no--cache--dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt