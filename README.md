# Ingestion and data analysis: Python

This project can be used to modify almost all type of structured file and send the data to a endpoint through a post request (tipically a Rest API request).
The project is very flexible and you can handle every your need by the .cfg file.
You can also produce a simple statistc file about the data in your file before any data mining action.

# First steps

## Setup

These are the instructions to follow to set up the project on your local environment.
The steps 3 and 4 are optional.

I used Python 3.6 to launch the commands in the project so if you prefer (or you have already installed) a version Python 2.* there are some **not optional** edits to do. 

>_In this case just have look to the comments in the code to fix the enviroment and be ready to start_

**Prerequisites**

 **Install every library used in the files**: 
  * pandas
  * requests
  * json
  * time
  * datetime

You can use the pip command: 
    `python -m pip install library-name`;

### Steps
1. Git clone the repository into your folder.
        
        git clone https://github.com/your_username/data-ingestion.git

2. Copy project.cfg.example to project.cfg

3. Launch the summury.py file to get basic statistics about the dataset or some particolar columns. Set in project.cfg file the columns you want to analyze using **STATISTICSCOLS** section.
It will generate a .csv file in the folder `statistics`. 
    
        python summury.py



4. Launch the mining.py file to go do data mining and correct the dataset changing column names, modifying values or dropping columns.
Set in project.cfg file (in **GENERAL** section) the flag to decide if it's necessary any edits in the dataset, then setthe columns you want to modify (**MODIFIERSCOLS** section), the values to change (**MODIFIERSVALUES** section) and the column(s) to drop.
It will generate a .csv file in the folder `files` with a name that has to be set in the project.cfg file; it will produce also some logs file to trace every step. 
    
        python mining.py


5. Launch the ingestion.py file to finally send the data into your file to a endpoint (maybe a your application in which you want to increment the data).
Set in project.cfg file every parameter (in **INGESTION** section) that is necessary to send the data.
This step will generate a **errors.csv** file in the folder `history_errors` with a name that is incremental and composed by date_hour_minute to keep every file of error and reuse this file. It will produce also some logs file to trace every step.  
    
        python ingestion.py
