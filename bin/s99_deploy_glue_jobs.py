# -*- coding: utf-8 -*-

from typing import Tuple, List, Dict, Optional, Any, Union

import copy
import dataclasses

import attr
from superjson import json
from boto_session_manager import BotoSesManager
from s3pathlib import S3Path, context
import cottonformation as cf
from cottonformation.res import glue

from aws_glue_automations import dir_project_root, pygitrepo_config

path_glue_etl_jobs_json = dir_project_root / "glue-etl-jobs.json"


@dataclasses.dataclass
class GlueJob:
    name: str = dataclasses.field()
    iam_role: str = dataclasses.field()
    script: str = dataclasses.field()
    libraries: str = dataclasses.field(default_factory=list)
    glue_version: str = dataclasses.field(default="3.0")
    worker_type: str = dataclasses.field(default="G.1X")
    number_of_workers: int = dataclasses.field(default=5)
    max_retries: int = dataclasses.field(default=0)
    max_concurrency_runs: int = dataclasses.field(default=1)
    timeout: int = dataclasses.field(default=60)

    @property
    def logic_id(self) -> str:
        return "GlueJob" + self.name.title().replace("_", "")


def merge_values(data: dict, common: dict):
    for k, v in copy.deepcopy(common).items():
        if k not in data:
            data[k] = v


def load_jobs(stage: str = "dev") -> List[GlueJob]:
    """
    Load all declared AWS Glue Job.
    """
    glue_etl_jobs_data = json.load(
        f"{path_glue_etl_jobs_json}", ignore_comments=True, verbose=False,
    )
    common = glue_etl_jobs_data.get("common", dict())
    stage_data = glue_etl_jobs_data.get("stages", dict()).get(stage, dict())
    stage_common = stage_data.get("common", dict())
    merge_values(stage_common, common)
    jobs: List[GlueJob] = list()
    for job_name, job_data in stage_data.get("jobs", dict()).items():
        merge_values(job_data, stage_common)
        job_data["name"] = job_name
        glue_job = GlueJob(**job_data)
        jobs.append(glue_job)
    return jobs


@attr.s
class GlueStack(cf.Stack):
    project_name: str = attr.ib()
    stage: str = attr.ib()

    @property
    def env_name(self):
        """
        A prefix for most of naming convention. Isolate resource from each other.
        """
        return f"{self.project_name}-{self.stage}"

    @property
    def stack_name(self):
        """
        CloudFormation stack name.
        """
        return f"{self.env_name}-iam-stack"

    def mk_rg1(self):
        """
        Make resource group 1
        """
        # declare a resource group, you can use Stack.rg1 to access it later.
        self.rg1 = cf.ResourceGroup("RG1")

        for job in jobs:
            default_arguments = {
                "--job-language": "python",
            }
            if job.libraries:
                default_arguments["--extra-py-files"] = ",".join(job.libraries)
                default_arguments[
                    "--data_source_s3_uri"] = "s3://501105007192-us-east-1-data/projects/gluelib_json_to_parquet/data/sensors/"
            glue_job_resource = glue.Job(
                job.logic_id,
                rp_Role=job.iam_role,
                rp_Command=glue.PropJobJobCommand(
                    p_Name="glueetl",
                    p_PythonVersion="3",
                    p_ScriptLocation=job.script,
                ),
                p_Name=f"{job.name}-{self.stage}",
                p_DefaultArguments=default_arguments,
                p_GlueVersion=job.glue_version,
                p_MaxRetries=float(job.max_retries),
                p_WorkerType=job.worker_type,
                p_NumberOfWorkers=job.number_of_workers,
                p_ExecutionProperty=glue.PropJobExecutionProperty(
                    p_MaxConcurrentRuns=float(job.max_concurrency_runs),
                ),
                p_Timeout=job.timeout,
            )
            self.rg1.add(glue_job_resource)

    def post_hook(self):
        """
        A user custom post stack initialization hook function. Will be executed
        after object initialization.

        We will put all resources in two different resource group.
        And there will be a factory method for each resource group. Of course
        we have to explicitly call it to create those resources.
        """
        self.mk_rg1()


if __name__ == "__main__":
    bsm = BotoSesManager(profile_name="aws_data_lab_open_source_sanhe")
    context.attach_boto_session(bsm.boto_ses)

    stage = "dev"

    jobs = load_jobs(stage)

    # copy source code from artifacts to deployment
    # because artifacts should be immutable, but the ETL script for Glue job
    # might be updated from the console
    for job in jobs:
        s3path_script_artifacts = S3Path.from_s3_uri(job.script)
        s3path_script_deployment = (
            S3Path.from_s3_uri(pygitrepo_config.aws_glue_deployment_s3_uri)
            / s3path_script_artifacts.relative_to(S3Path.from_s3_uri(pygitrepo_config.aws_glue_artifact_s3_uri))
        )
        s3path_script_artifacts.copy_to(s3path_script_deployment, overwrite=True)
        job.script = s3path_script_deployment.uri

    # create all cloudformation objects
    glue_stack = GlueStack(
        project_name="glue-stack",
        stage=stage,
    )

    # create cloudformation template
    tpl = cf.Template(
        Description="Demo: Resource Group best practice",
    )

    # add resource group from stack to template, in ascending order.
    tpl.add(glue_stack.rg1)

    # tagging resources
    common_tags = dict(
        ProjectName=glue_stack.project_name,
        Stage=glue_stack.stage,
        Owner="sanhehu@amazon.com",
        Md5="9579dcf0a14ec89ab5aa4b483151ed86",
    )
    tpl.batch_tagging(**common_tags)

    # post process
    for res in tpl.Resources.values():
        if isinstance(res, glue.Job):
            # The CloudFormation type for these two field is double
            # however the server side can only take Integer
            res.p_MaxRetries = int(res.p_MaxRetries)
            res.p_ExecutionProperty.p_MaxConcurrentRuns = int(res.p_ExecutionProperty.p_MaxConcurrentRuns)
            # AWS Glue Job Tags is very weird. Other resources are list of tags,
            # but Glue Job use dictionary
            res.p_Tags = common_tags

    # dump to local for debug
    path_template_json = dir_project_root / "bin" / "template.json"
    tpl.to_json_file(f"{path_template_json}")

    # deploy to AWS CloudFormation
    env = cf.Env(bsm=bsm)
    env.deploy(
        template=tpl,
        stack_name=glue_stack.stack_name,
        include_iam=False,
    )
