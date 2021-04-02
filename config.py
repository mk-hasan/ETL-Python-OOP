"""
Author: Kamrul Hasan
Email: hasana.alive@gmail.com
Date: 11.02.2021
"""

"""
config.py
~~~~~~~~~~
This Python module contains configuration details of the application.
The input file path and output file path can be configured here.  
"""

import os

path = os.getcwd()

cost_data_path = {'cost_data_path': path + "/data/Cost Data.xlsx"}
id_data_path = {'id_data_path': path + "/data/IDs.csv"}
order_data_path = {'order_data_path': path + "/data/Order Data.xlsx"}
