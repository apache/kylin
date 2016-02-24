# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

import time
import datetime
import calendar
from jobs.build import CubeBuildJob
from jobs.cube import CubeJob
from models.request import JobBuildRequest
from models.job import JobInstance, CubeJobStatus
from settings.settings import KYLIN_JOB_MAX_COCURRENT, KYLIN_JOB_MAX_RETRY


class CubeWorker:
    job_instance_dict = {}
    job_retry_dict = {}
    scheduler = None
    run_cube_job_id = None
    check_cube_job_id = None

    def __init__(self):
        pass

    @staticmethod
    def run_cube_job(endtime):
        if CubeWorker.all_finished():
            return True

        running_job_list = CubeWorker.get_current_running_job()
        print "current running", len(running_job_list), "jobs"

        if running_job_list and len(running_job_list) >= KYLIN_JOB_MAX_COCURRENT:
            print "will not schedule new jobs this time because running job number >= the max cocurrent job number", KYLIN_JOB_MAX_COCURRENT
        else:
            max_allow = KYLIN_JOB_MAX_COCURRENT - len(running_job_list)
            for cube_name in CubeWorker.job_instance_dict:
                if max_allow <= 0: break

                job_instance = CubeWorker.job_instance_dict[cube_name]
                if job_instance is None or (
                    isinstance(job_instance, JobInstance) and job_instance.get_status() == CubeJobStatus.ERROR):
                    try_cnt = CubeWorker.job_retry_dict.get(cube_name, -1)

                    if try_cnt >= KYLIN_JOB_MAX_RETRY:
                        # have already tried KYLIN_JOB_MAX_RETRY times
                        CubeWorker.job_instance_dict[cube_name] = 0
                    else:
                        # try to cancel the error cube build segment
                        error_job_list = CubeJob.get_cube_job(cube_name, CubeJob.ERROR_JOB_STATUS)
                        if error_job_list:
                            for error_job in error_job_list:
                                CubeBuildJob.cancel_job(error_job.uuid)
                                print "cancel an error job with uuid=", error_job.uuid, "for cube=", cube_name

                        # run cube job
                        # instance_list = CubeJob.list_cubes(cube_name)
                        build_request = JobBuildRequest()
                        if endtime is not None:
                            # build_request.startTime = instance_list[0].segments[instance_list[0].segments.__len__() - 1].date_range_end
                            build_request.endTime = \
                                (int(time.mktime(datetime.datetime.strptime(endtime,
                                                                            "%Y-%m-%d").timetuple())) - time.timezone) * 1000
                        else:
                            d = datetime.datetime.utcnow()
                            build_request.endTime = calendar.timegm(d.utctimetuple()) * 1000

                        current_job_instance = CubeBuildJob.rebuild_cube(cube_name, build_request)

                        if current_job_instance:
                            print "schedule a cube build job for cube =", cube_name
                            CubeWorker.job_instance_dict[cube_name] = current_job_instance
                            max_allow -= 1
                        CubeWorker.job_retry_dict[cube_name] = try_cnt + 1

    @staticmethod
    def check_cube_job():
        for cube_name in CubeWorker.job_instance_dict:
            job_instance = CubeWorker.job_instance_dict[cube_name]
            if isinstance(job_instance,
                          JobInstance) and job_instance.uuid and job_instance.get_status() == CubeJobStatus.RUNNING:
                current_job_instance = CubeBuildJob.get_job(job_instance.uuid)
                if current_job_instance:
                    CubeWorker.job_instance_dict[cube_name] = current_job_instance

            job_instance = CubeWorker.job_instance_dict[cube_name]
            if job_instance is None:
                print "status of cube =", cube_name, "is NOT STARTED YET"
            elif isinstance(job_instance, JobInstance):
                print "status of cube =", cube_name, "is", job_instance.get_status(), "at %d/%d" % (
                job_instance.get_current_step(), len(job_instance.steps))

    @staticmethod
    def get_current_running_job():
        if not CubeWorker.job_instance_dict:
            return None

        running_job_list = []
        for cube_name in CubeWorker.job_instance_dict:
            job_instance = CubeWorker.job_instance_dict[cube_name]

            if job_instance and isinstance(job_instance,
                                           JobInstance) and job_instance.get_status() == CubeJobStatus.RUNNING:
                running_job_list.append(job_instance)

        return running_job_list

    @staticmethod
    def all_finished():
        if not CubeWorker.job_instance_dict:
            return True

        for cube_name in CubeWorker.job_instance_dict:
            job_instance = CubeWorker.job_instance_dict[cube_name]

            if job_instance == 0:
                pass
            elif job_instance is None:
                return False
            elif isinstance(job_instance, JobInstance) and job_instance.get_status() == CubeJobStatus.RUNNING:
                return False

        return True
