#!/usr/bin/python3
# -*- coding: utf-8 -*-
# europeana-mdic/src/main/submit_menu.py

import sys
import subprocess
import shlex
import ic_scoring
import conf
import index_settings

INDEX_NAME = conf.index_name
TYPE_NAME = conf.type_name
INDEX_SETTINGS = index_settings.index_setting
INDEX_MAPPING = index_settings.index_mapping
IC_INDEX_NAME = conf.ic_index_name
IC_TYPE_NAME = conf.ic_type_name
IC_INDEX_SETTINGS = index_settings.ic_index_setting
IC_INDEX_MAPPING = index_settings.ic_index_mapping
sp_master = conf.spark_master

def show_menu(options):
    """Show options menu and return the chosen one."""
    for op in options:
        print("            " + op)
    answer = input("Enter your choice: ")
    if answer in ["1", "2", "3", "4"]:
        su = input("Are you sure? [y/n]:".format(op=options[int(answer) - 1]))
        if su not in ["y", "Y"]:
            print("Wrong answer!")
            return "go"
    return answer


def submit_job(job_file, job_name, index_name, type_name):
    """Submit a job to Spark master."""
    submit_command ="".join(["bash /usr/local/spark/bin/spark-submit\
                              --master spark://", sp_master, " \
                              --name '", job_name, "' \
                              --executor-memory 2G " , job_file])
    args = shlex.split(submit_command)
    p = subprocess.call(args)
    if not p:
        print(ic_scoring.docs_count(index_name, type_name),'documents are indexed')

    
def transform_and_indexing_job_submit():
    """Prepare for submitting transform and indexing job."""
    if ic_scoring.init_index(
            INDEX_NAME,
            TYPE_NAME,
            INDEX_SETTINGS,
            INDEX_MAPPING):
        submit_job("data_transform.py", "Transform and indexing job",INDEX_NAME, TYPE_NAME)
    input("Press anykey to show the menu again!")


def ic_scoring_job_submit():
    """Prepare for submitting ic_scoring job."""
    if ic_scoring.init_index(
            IC_INDEX_NAME,
            IC_TYPE_NAME,
            IC_INDEX_SETTINGS,
            IC_INDEX_MAPPING):
        submit_job("ic_scoring.py", "IC scoring job", IC_INDEX_NAME, IC_TYPE_NAME)
    input("Press anykey to show the menu again!")

                
def main():
    
    my_options = [
        "1- Transformation and indexing documents",
        "2- Calculating document IC scores",
        "3- Adding origin index replicas",
        "4- Adding result index replicas",
        "5- Exit"]
    ans = True
    # menu loop
    while ans:
        ans = show_menu(my_options)
        if ans == "1":
            transform_and_indexing_job_submit()
        elif ans == "2":
            ic_scoring_job_submit()
        elif ans == "3":
            n = int(input("Enter number of replicas: "))
            ic_scoring.es_replication(INDEX_NAME, n)
            input("press anykey to show the menu again!")
        elif ans == "4":
            n = int(input("Enter number of replicas: "))
            ic_scoring.es_replication(IC_INDEX_NAME, n)
            input("press anykey to show the menu again!")
        elif ans == "5":
            sys.exit(0)
        else:
            ans = "go"
            print("Invalid choice, Try again!!!")
            input("press anykey to show the menu again!")


if __name__ == "__main__":
    main()
