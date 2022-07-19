from time import sleep
import requests
import json

class DatabricksRun:
    def __init__(self, databricks_url, databricks_token):
        self._end_point = f'{databricks_url}/api/2.0/'
        self._headers = {'Authorization': f'Bearer {databricks_token}'}
        self._runs_executing = dict()
        self._runs_completed = dict()

    def _job_data(self,
                notebook_path,
                cluster_id,
                timeout_seconds=3600,
                base_parameters={},
                all_purpose_cluster=True):
        
        if all_purpose_cluster:
            job_data= json.dumps(
                {
                    "run_name": f"Notebook {notebook_path}",
                    "timeout_seconds": timeout_seconds,
                    "existing_cluster_id": cluster_id,
                    "notebook_task": {
                        "notebook_path": notebook_path,
                        "base_parameters": base_parameters
                    }
                }
            )

            return job_data

    def submit_job(self,
                notebook_path,
                cluster_id,
                timeout_seconds=3600,
                base_parameters={},
                all_purpose_cluster=True):
                
        job_data = self._job_data(notebook_path, cluster_id, timeout_seconds, base_parameters, all_purpose_cluster)
        resp = requests.post(self._end_point+'jobs/runs/submit', data=job_data, headers=self._headers)

        if resp.status_code == 200:
            response_json = resp.json()
            self._runs_executing[response_json['run_id']] = response_json
            return response_json['run_id']

        else:
            print('an erro ocurred')

    def get_run_information(self, run_id):
        resp = requests.get(self._end_point+f'jobs/runs/get?run_id={run_id}', headers=self._headers)
        return resp

    def check_executing_jobs(self):
        runs_finished = list()

        for run_id in self._runs_executing.keys():
            resp = self.get_run_information(run_id)

            resp_json = resp.json()
            resp_life_cycle = resp_json['state']['life_cycle_state']

            if (resp_life_cycle=='PENDING') or (resp_life_cycle=='RUNNING'):
                pass
            else:
                print(f"run {run_id} finished with the status: {resp_json['state']['result_state']}")
                runs_finished.append(run_id)
                self._runs_completed[run_id] = resp_json

        for run_id in runs_finished:
            del self._runs_executing[run_id]

        return True

    def get_finished_runs(self):
        return self._runs_completed.copy()

    def get_executing_runs(self):
        return self._runs_executing.copy()

    def wait_all_jobs_complete(self):
        while len(self._runs_executing)>0:
            sleep(5)
            self.check_executing_jobs()

    def wait_a_job_complete(self, run_id):
        while run_id in self._runs_executing.keys():
            sleep(5)
            self.check_executing_jobs()
