# -*- coding: utf-8 -*-
__author__ = 'Ni Chunen'
import time
import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from apscheduler.schedulers.blocking import BlockingScheduler

from scheduler.workers.cube import CubeWorker
from jobs.cube import CubeJob
from models.io.readers import CSVReader
from jobs.build import CubeBuildJob


class ClientJob:
    @staticmethod
    def build(cube_name_list, endtime=None):
        run_cube_job_id = '1'
        check_cube_job_id = '2'
        scheduler = BackgroundScheduler()
        CubeWorker.job_instance_dict = {}

        for cube_name in cube_name_list:
            CubeWorker.job_instance_dict[cube_name] = None

        CubeWorker.scheduler = scheduler
        CubeWorker.run_cube_job_id = run_cube_job_id
        CubeWorker.check_cube_job_id = check_cube_job_id
        # start the run cube job immediately
        CubeWorker.run_cube_job(endtime)

        scheduler.add_job(CubeWorker.run_cube_job, 'interval', seconds=30, id=run_cube_job_id, args=[endtime])
        scheduler.add_job(CubeWorker.check_cube_job, 'interval', seconds=30, id=check_cube_job_id)
        scheduler.start()

        while True:
            if CubeWorker.all_finished():
                print "all cube jobs are finished"
                scheduler.remove_job(check_cube_job_id)
                scheduler.remove_job(run_cube_job_id)
                scheduler.shutdown()
                break

            time.sleep(15)

    @staticmethod
    def init(cube_name_list):
        pass

    @staticmethod
    def build_cube_from_csv(csv_file, database, endtime=None, timed_build=None, crontab_options=None):
        cube_dic_list = CSVReader.get_cube_desc_list_from_csv(csv_file, database)
        cube_name_list = []

        for cube_dic in cube_dic_list:
            cube_name_list.append(cube_dic['cube_desc'].name)

        print "building cubes for", cube_name_list

        if timed_build is not None and crontab_options is not None:
            scheduler = BlockingScheduler()
            if timed_build is 'i' and crontab_options is not None:
                scheduler.add_job(ClientJob.build, 'interval', hours=int(crontab_options),
                                  args=[cube_name_list, endtime])
                try:
                    scheduler.start()
                except (KeyboardInterrupt, SystemExit):
                    scheduler.shutdown()
            elif timed_build is 't' and crontab_options is not None:
                time_list = crontab_options.split(',')
                if time_list.__len__() == 6:
                    scheduler.add_job(ClientJob.build, 'date',
                                      run_date=datetime.datetime(int(time_list[0]), int(time_list[1]),
                                                                 int(time_list[2]), int(time_list[3]),
                                                                 int(time_list[4]), int(time_list[5])),
                                      args=[cube_name_list, endtime])
                    try:
                        scheduler.start()
                    except (KeyboardInterrupt, SystemExit):
                        scheduler.shutdown()
                else:
                    print 'Bad command line!'
            else:
                print 'Bad command line!'
        else:
            ClientJob.build(cube_name_list, endtime)

    @staticmethod
    def build_cube_from_names_or_file(cube_name, names_file, endtime=None, timed_build=None, crontab_options=None):
        cube_name_list_from_file = []

        if names_file is not None:
            cube_name_list_from_file = CSVReader.get_cube_names_from_csv(names_file)
        if cube_name is not None:
            cube_name_list = cube_name.split(',')

        cube_name_list += cube_name_list_from_file
        print "building cubes for", cube_name_list

        if timed_build is not None and crontab_options is not None:
            scheduler = BlockingScheduler()

            if timed_build is 'i' and crontab_options is not None:
                scheduler.add_job(ClientJob.build, 'interval', hours=int(crontab_options),
                                  args=[cube_name_list, endtime])
                try:
                    scheduler.start()
                except (KeyboardInterrupt, SystemExit):
                    scheduler.shutdown()
            elif timed_build is 't' and crontab_options is not None:
                time_list = crontab_options.split(',')

                if time_list.__len__() == 6:
                    scheduler.add_job(ClientJob.build, 'date',
                                      run_date=datetime.datetime(int(time_list[0]), int(time_list[1]),
                                                                 int(time_list[2]), int(time_list[3]),
                                                                 int(time_list[4]), int(time_list[5])),
                                      args=[cube_name_list, endtime])
                    try:
                        scheduler.start()
                    except (KeyboardInterrupt, SystemExit):
                        scheduler.shutdown()
                else:
                    print 'Bad command line!'
            else:
                print 'Bad command line!'
        else:
            ClientJob.build(cube_name_list, endtime)

    @staticmethod
    def create_cube_from_csv(csv_file, project, database):
        cube_dic_list = CSVReader.get_cube_desc_list_from_csv(csv_file, database)

        for cube_dic in cube_dic_list:
            print 'creating cube for', cube_dic['cube_desc'].name, '@project=', project
            # create cube under project default
            # print cube_desc.to_json()
            cube_request_result = CubeJob.create_cube(cube_dic['cube_desc'], cube_dic['model_desc'], project)
            print 'result=', 'OK' if cube_request_result else 'FAILED'

    @staticmethod
    def check_job_status(cube_name, names_file, status):
        cube_name_list = []
        job_instance_list = []
        job_status = None

        if status is not None:
            if status is 'R':
                job_status = CubeJob.RUNNING_JOB_STATUS
            elif status is 'F':
                job_status = CubeJob.FINISHED_JOB_STATUS
            elif status is 'D':
                job_status = CubeJob.DISCARDED_JOB_STATUS
            else:
                job_status = CubeJob.ERROR_JOB_STATUS

        if cube_name is not None:
            cube_name_list = cube_name.split(',')
        elif names_file is not None:
            cube_name_list = CSVReader.get_cube_names_from_csv(names_file)

        if cube_name_list.__len__() > 0:
            for cube_name in cube_name_list:
                job_instance_list += CubeJob.get_cube_job(cube_name, job_status)
        else:
            job_instance_list = CubeJob.get_cube_job(None, job_status)

        if job_instance_list.__len__() == 0:
            print "No job found."

        for job_instance in job_instance_list:
            print "JOB name " + job_instance.name + " of cube " + job_instance.related_cube + "'s status is " + job_instance.get_status()

    @staticmethod
    def cancel_job(cube_name, names_file):
        cube_name_list = []
        job_instance_list = []

        if cube_name is not None:
            cube_name_list = cube_name.split(',')

        if names_file is not None:
            cube_name_list += CSVReader.get_cube_names_from_csv(names_file)

        if cube_name_list.__len__() > 0:
            for cube_name in cube_name_list:
                job_instance_list += CubeJob.get_cube_job(cube_name, CubeJob.ERROR_JOB_STATUS)
                job_instance_list += CubeJob.get_cube_job(cube_name, CubeJob.RUNNING_JOB_STATUS)
        else:
            job_instance_list = CubeJob.get_cube_job(None, CubeJob.ERROR_JOB_STATUS)

        if job_instance_list.__len__() == 0:
            print "No job found."

        for job_instance in job_instance_list:
            print "Cancel job " + job_instance.name + "\n"
            CubeBuildJob.cancel_job(job_instance.uuid)

    @staticmethod
    def resume_job(cube_name, names_file):
        cube_name_list = []
        job_instance_list = []

        if cube_name is not None:
            cube_name_list = cube_name.split(',')

        if names_file is not None:
            cube_name_list += CSVReader.get_cube_names_from_csv(names_file)

        if cube_name_list.__len__() > 0:
            for cube_name in cube_name_list:
                job_instance_list += CubeJob.get_cube_job(cube_name, CubeJob.ERROR_JOB_STATUS)
        else:
            job_instance_list = CubeJob.get_cube_job(None, CubeJob.ERROR_JOB_STATUS)

        if job_instance_list.__len__() == 0:
            print "No error job found."

        for job_instance in job_instance_list:
            print "Resume job " + job_instance.name + "\n"
            CubeBuildJob.resume_job(job_instance.uuid)

    @staticmethod
    def __init__():
        pass
