from ast import Str
from time import sleep
import requests
import json


class DatabricksJob:
    """Class to create and execute jobs on Databricks

    Attributes
    ----------
    result_state: str
        a string with the final result of the jobs

    run_id: str
        a string with the id of the job executing

    run_page_url: str
        a string with the url with information about the job executing on Databricks

    Methods
    -------

    
    """
    def __init__(self) -> None:
        self.status = "not submited"
        self._cluster = None
        self._run_id = None
        self._job_data_without_cluster = {}

        self._end_point = None
        self._headers = None

        self._run_page_url = None
        self._result_state = None

    @property
    def result_state(self):
        return self._result_state
        
    @property
    def run_id(self):
        return self._run_id

    @run_id.setter
    def run_id(self, run_id):
        self._run_id = run_id

    @property
    def run_page_url(self):
        if self._run_page_url is None:
            resp = self.get_run_information()
            self._run_page_url = resp.json()['run_page_url']

        return self._run_page_url

    def set_workspace(self, databricks_url: str, databricks_token: str) -> None:
        """Set databricks workspace that the job will execute

        Args:
            databricks_url (str): url of databricks workspace
            databricks_token (str): token of databricks workspace
        """
        self._end_point = f'{databricks_url}/api/2.0/'
        self._headers = {'Authorization': f'Bearer {databricks_token}'}

    def workspace_is_set(self) -> bool:
        """return if the databricks workspace was setted.

        Returns:
            bool: True if the workspace is setted
        """
        return (self._end_point is not None)

    def set_cluster(self, cluster_json: dict) -> None:
        """set the cluster that will execute the job

        Args:
            cluster_json (dict): dictionary with cluster settings.
        """
        self._cluster = cluster_json

    def set_job_data(self,
                    notebook_path: str,
                    timeout_seconds=3600,
                    base_parameters={}):
        """set data about the job that will execute

        Args:
            notebook_path (str): path of the notebook that will be executed
            timeout_seconds (int, optional): max execution time. Defaults to 3600.
            base_parameters (dict, optional): parameters that will be send to job that can be read by the widgets. Defaults to {}.
        """

        self._job_data_without_cluster = {
                    "run_name": f"Notebook {notebook_path}",
                    "timeout_seconds": timeout_seconds,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": base_parameters
                    }
        }

    def get_job_data(self) -> str:
        """convert cluster and job data to json format before submit on databricks

        Returns:
            str: str like json that has information that will be send to databricks.
        """
        return json.dumps(dict(**self._cluster, **self._job_data_without_cluster))

    def submit_job(self):
        """submit job 

        Raises:
            Exception: if workspace is not setted will have the exception 'workspace must be set, use set_workspace function'
            Exception: if job have already been submited will raise the exception 'job has already been submited'
        """
        if not self.workspace_is_set():
            raise Exception('workspace must be set, use set_workspace function')

        if self.run_id is None:
            job_data = self.get_job_data()
            resp = requests.post(self._end_point+'jobs/runs/submit', data=job_data, headers=self._headers)

            if resp.status_code == 200:
                response_json = resp.json()
                self.run_id = response_json['run_id']
                self.status = 'submited'
            else:
                print(f'an erro ocurred \n{resp.text}')
        else:
            raise Exception('job has already been submited')

    def get_run_information(self) -> requests.Response:
        """get information about the job running

        Raises:
            Exception: if the job was not submitted the exception is raised 'job was not submitted'

        Returns:
            requests.Reponse: request Response object
        """
        if self.status=="not submited":
            raise Exception('job was not submitted')
        else:
            resp = requests.get(self._end_point+f'jobs/runs/get?run_id={self.run_id}', headers=self._headers)
            return resp

    def check_execution(self) -> str:
        """check the status of the job

        Returns:
            str: str with the status
        """
        resp = self.get_run_information()

        resp_json = resp.json()
        self.status = resp_json['state']['life_cycle_state']

        if 'result_state' in resp_json['state'].keys():
            self._result_state = resp_json['state']['result_state']


        return self.status

    def wait_job_complete(self, sleep_time_seconds=5) -> str:
        """wait the job complete

        Args:
            sleep_time_seconds (int, optional): define the time in seconds that the function will wait before check if the job finish. Defaults to 5.

        Returns:
            str: str with the result state of the job
        """
        processing = True

        while processing:
            resp_life_cycle = self.check_execution()

            if (resp_life_cycle=='PENDING') or (resp_life_cycle=='RUNNING'):
                    sleep(sleep_time_seconds)
            else:
                processing = False
                print(f"run {self.run_id} finished with status: {self._result_state}")
                return self._result_state


class DatabricksRun:
    def __init__(self, databricks_url, databricks_token):
        self.databricks_url = databricks_url
        self._databricks_token = databricks_token
        self._job_list = list()
        self._job_executing = list()
        self._job_completed = list()

    def _add_job_to_process_list(self, job_list: list):
        """add job list to process checking if the jobs are instances of DatabricksJob

        Args:
            job_list (list): list of DatabricksJob instances

        Raises:
            Exception: if a element of the list is not a 4
        """
        job_is_databricksjob_inst = [isinstance(job, DatabricksJob) for job in job_list]
        if False in job_is_databricksjob_inst:
            raise Exception('must be send a instance of DatabricksJob or a list of istances DatabricksJob')
        else:
            self._job_list += job_list

    def add_job_to_process(self, job: DatabricksJob) -> bool:
        """add to the list of processing the jobs

        Args:
            job (DatabricksJob or list): a job from DatabricksJob or list of Databricksjob

        Raises:
            Exception: job must be a list or instance of DatabricksJob

        Returns:
            bool: return True if has success
        """
        if isinstance(job, list):
            self._add_job_to_process_list(job)
            return True
        elif isinstance(job, DatabricksJob):
            self._job_list.append(job)
            return True

        raise Exception('must be send a instance of DatabricksJob or a list of istances DatabricksJob')


    def submit_jobs(self):
        """submit to databricks all jobs in job_list to start.
        """
        move_job_to_executing = list()

        for job in self._job_list:
            if not job.workspace_is_set():
                job.set_workspace(self.databricks_url, self._databricks_token)
            
            job.submit_job()

            move_job_to_executing.append(job)

        for job in move_job_to_executing:
            self._job_list.remove(job)
            self._job_executing.append(job)

    def get_run_information(self, run_id: str) -> requests.Response:
        """return information about the job submitted

        Args:
            run_id (str): id of the job submitted

        Returns:
            requests.Response: object reponse from requests
        """
        resp = requests.get(self._end_point+f'jobs/runs/get?run_id={run_id}', headers=self._headers)
        return resp

    def check_executing_jobs(self) -> bool:
        """update status of jobs executing

        Returns:
            bool: return true if job is updated
        """
        jobs_finished = list()

        for job in self._job_executing:
            resp_life_cycle = job.check_execution()

            if (resp_life_cycle=='PENDING') or (resp_life_cycle=='RUNNING'):
                pass
            else:
                print(f"run {job.run_id} finished with status: {job.result_state}")
                jobs_finished.append(job)

        for job in jobs_finished:
            self._job_executing.remove(job)
            self._job_completed.append(job)

        return True

    def get_finished_runs(self) -> list:
        """return list of jobs finished

        Returns:
            list: list of jobs finished
        """
        return self._job_completed.copy()

    def get_executing_runs(self) -> list:
        """return a list of executing jobs

        Returns:
            list: list of jobs executing
        """
        return self._job_executing.copy()

    def wait_all_jobs_complete(self) -> None:
        """wait until all jobs executing finish
        """
        while len(self._job_executing)>0:
            sleep(5)
            self.check_executing_jobs()

    def wait_a_job_complete(self, job: DatabricksJob) -> None:
        """wait a specific job to finish

        Args:
            job (DatabricksJob): DatabricksJob that is to wait
        """
        while job in self._job_executing:
            sleep(5)
            self.check_executing_jobs()
