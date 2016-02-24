# -*- coding: utf-8 -*-
__author__ = 'Ni Chunen'

from optparse import OptionParser
from settings.settings import KYLIN_REST_HOST
from client.client_interface import ClientJob


def menu_parser():
    print 'Current kylin rest host is ' + KYLIN_REST_HOST + ', if not, please quit and modify it from your setting file.'
    parser = OptionParser()
    parser.add_option("-c", "--create_cubes", action="store_true",
                      dest="create_cubes",
                      help="Create cubes with descriptions in the csv file to your project.")
    parser.add_option("-b", "--build_cubes", action="store_true",
                      dest="build_cubes",
                      help="Build cubes with descriptions in the csv file to your project.")
    parser.add_option("-s", "--check_job_status", action="store_true",
                      dest="check_job_status",
                      help="Check job status with options.")
    parser.add_option("-k", "--cancel_job", action="store_true",
                      dest="cancel_job",
                      help="Cancel jobs with options.")
    parser.add_option("-r", "--resume_job", action="store_true",
                      dest="resume_job",
                      help="Resume jobs with options.")

    parser.add_option("-D", "--database",
                      dest="database",
                      default="default",
                      help="Specify your database,[default=%default].")
    parser.add_option("-P", "--project",
                      dest="project",
                      default="learn_kylin",
                      help="Specify your project,[default=%default].")
    parser.add_option("-T", "--time",
                      dest="time",
                      default=None,
                      help="Set the end time of cube building,[default=%default].")
    parser.add_option("-F", "--cubeDefFile",
                      dest="cubeDefFile",
                      default="cube_def.csv",
                      help="Specify your cubes definition file,[default=%default].")
    parser.add_option("-f", "--cubeNameFile",
                      dest="cubeNameFile",
                      default=None,
                      help="Specify your cube names' file,[default=%default].")
    parser.add_option("-S", "--status",
                      dest="status",
                      default=None,
                      help="Specify the job status, R for Running, E for Error, F for Finished, D for Discarded, [default=%default].")
    parser.add_option("-C", "--cube_name",
                      dest="cube_name",
                      default=None,
                      help="Specify the cube name, [default=%default].")
    parser.add_option("-B", "--schedule_build",
                      dest="schedule_build",
                      default=None,
                      help="Schedule cube building with options, 'i' for intervally build, 't' for time build, [default=%default].")
    parser.add_option("-O", "--crontab_options",
                      dest="crontab_options",
                      default=None,
                      help="Set the options of timed building, like '-B i -O 24' for building every 24 hours, '-B t -O 2016,3,1,0,0,0' for building at '2016-3-1 0:0:0', [default=%default].")

    (options, args) = parser.parse_args()
    status = True

    if options.create_cubes == True and options.cubeDefFile is not None and status == True:
        ClientJob.create_cube_from_csv(options.cubeDefFile, options.project, options.database)
        status = False

    if options.build_cubes == True and (
            options.cubeNameFile is not None or options.cube_name is not None) and status == True:
        ClientJob.build_cube_from_names_or_file(options.cube_name, options.cubeNameFile, options.time,
                                                options.schedule_build, options.crontab_options)
        status = False

    if options.build_cubes == True and options.cubeDefFile is not None and status == True:
        ClientJob.build_cube_from_csv(options.cubeDefFile, options.database, options.time, options.schedule_build,
                                      options.crontab_options)
        status = False

    if options.check_job_status == True and status == True:
        ClientJob.check_job_status(options.cube_name, options.cubeNameFile, options.status)
        status = False

    if options.cancel_job == True and status == True:
        ClientJob.cancel_job(options.cube_name, options.cubeNameFile)
        status = False

    if options.resume_job == True and status == True:
        ClientJob.resume_job(options.cube_name, options.cubeNameFile)
        status = False

    if status:
        print 'Bad command line!'
