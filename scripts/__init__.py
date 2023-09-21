# ----------------------------------------------------------------------------
# Copyright (c) 2023--, GG2 Database development team.
#
# Distributed under the terms of the Modified BSD License.
#
# The full license is in the file COPYING.txt, distributed with this software.
# ----------------------------------------------------------------------------
from .database_operations import create_databases, insert_bakta, insert_software_info, insert_run_info 
from .process_bakta_output import parse_bakta, delete_bakta_output_files
from .submit_bakta import submit_bakta_job, monitor_job_status

# -*- coding:utf-8 -*-
name = "gg2_database"
__version__ = "0.0.1"
