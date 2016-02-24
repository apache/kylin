# -*- coding: utf-8 -*-
__author__ = 'Huang, Hua'

from models.object import JsonSerializableObj


class CubeJobStatus:
    RUNNING = 'RUNNING'
    ERROR = 'ERROR'
    FINISHED = 'FINISHED'
    DISCARD = 'DISCARDED'


class JobInstance(JsonSerializableObj):
    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.uuid = None
        self.last_modified = None
        self.name = None
        self.type = None
        self.duration = None
        self.related_cube = None
        self.related_segment = None
        self.exec_start_time = None
        self.exec_end_time = None
        self.mr_waiting = None
        self.steps = None
        self.submitter = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        ji = JobInstance()

        ji.uuid = json_dict.get('uuid')
        ji.last_modified = json_dict.get('last_modified')
        ji.name = json_dict.get('name')
        ji.type = json_dict.get('type')
        ji.duration = json_dict.get('duration')
        ji.related_cube = json_dict.get('related_cube')
        ji.related_segment = json_dict.get('related_segment')
        ji.exec_start_time = json_dict.get('exec_start_time')
        ji.exec_end_time = json_dict.get('exec_end_time')
        ji.mr_waiting = json_dict.get('mr_waiting')
        # deserialize json for steps
        if json_dict.get('steps') and type(json_dict.get('steps')) == list:
            step_list = json_dict.get('steps')
            ji.steps = [JobStep.from_json(step) for step in step_list]
        ji.submitter = json_dict.get('submitter')

        return ji

    def get_status(self):
        if not self.steps:
            return CubeJobStatus.ERROR

        for job_step in self.steps:
            if job_step.step_status in CubeJobStatus.ERROR:
                return CubeJobStatus.ERROR
            if job_step.step_status in CubeJobStatus.DISCARD:
                return CubeJobStatus.DISCARD

        # check the last step
        job_step = self.steps[-1]
        if job_step.step_status not in CubeJobStatus.FINISHED:
            return CubeJobStatus.RUNNING

        return CubeJobStatus.FINISHED

    def get_current_step(self):
        if not self.steps:
            return 0

        step_id = 1
        for job_step in self.steps:
            if job_step.step_status not in CubeJobStatus.FINISHED:
                return step_id
            step_id += 1

        return len(self.steps)


class JobStep(JsonSerializableObj):
    def __init__(self):
        JsonSerializableObj.__init__(self)

        self.name = None
        self.sequence_id = None
        self.exec_cmd = None
        self.interrupt_cmd = None
        self.exec_start_time = None
        self.exec_end_time = None
        self.exec_wait_time = None
        self.step_status = None
        self.cmd_type = None
        self.info = None
        self.run_async = None

    @staticmethod
    def from_json(json_dict):
        if not json_dict or type(json_dict) != dict: return None

        js = JobStep()

        js.name = json_dict.get('name')
        js.sequence_id = json_dict.get('sequence_id')
        js.exec_cmd = json_dict.get('exec_cmd')
        js.interrupt_cmd = json_dict.get('interrupt_cmd')
        js.exec_start_time = json_dict.get('exec_start_time')
        js.exec_end_time = json_dict.get('exec_end_time')
        js.exec_wait_time = json_dict.get('exec_wait_time')
        js.step_status = json_dict.get('step_status')
        js.cmd_type = json_dict.get('cmd_type')
        js.info = json_dict.get('info')
        js.run_async = json_dict.get('run_async')

        return js
