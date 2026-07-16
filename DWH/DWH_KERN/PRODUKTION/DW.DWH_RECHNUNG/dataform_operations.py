"""Module containing structural interfaces to manage Dataform workflows."""

import time
import logging
from google.cloud import dataform_v1beta1 as dataform

logger = logging.getLogger(__name__)


class DataformExecutionHelper:
    """Orchestrates dynamic compilation and invocation of target Dataform models."""

    def __init__(self, project_id: str, location: str, repository_id: str):
        self.client = dataform.DataformClient()
        self.project_id = project_id
        self.location = location
        self.repository_id = repository_id
        self.repo_path = self.client.repository_path(
            project_id, location, repository_id
        )

    def trigger_model_compilation(self, git_commitish: str, vars_dict: dict) -> str:
        """Sends compilation requests dynamically applying dynamic parameters."""
        compilation_result = self.client.create_compilation_result(
            parent=self.repo_path,
            compilation_result=dataform.CompilationResult(
                git_commitish=git_commitish,
                code_compilation_config=dataform.CodeCompilationConfig(
                    vars=vars_dict
                ),
            ),
        )
        logger.info(f"Compiled Dataform result: {compilation_result.name}")
        return compilation_result.name

    def execute_target_model(
        self, compilation_result_name: str, dataset_id: str, table_id: str
    ) -> str:
        """Invokes specific model target actions inside compiled environment states."""
        invocation = self.client.create_workflow_invocation(
            parent=self.repo_path,
            workflow_invocation=dataform.WorkflowInvocation(
                compilation_result=compilation_result_name,
                invocation_config=dataform.InvocationConfig(
                    included_targets=[
                        dataform.Target(
                            database=self.project_id,
                            schema=dataset_id,
                            name=table_id,
                        )
                    ]
                ),
            ),
        )
        logger.info(f"Workflow invocation triggered: {invocation.name}")
        return invocation.name

    def await_execution(self, invocation_name: str, check_interval_sec: int = 15) -> bool:
        """Awaits execution completion of target Dataform operations."""
        while True:
            invocation = self.client.get_workflow_invocation(name=invocation_name)
            state = invocation.state.name
            
            if state in ["SUCCEEDED"]:
                return True
            if state in ["FAILED", "CANCELLED"]:
                raise RuntimeError(f"Dataform execution pipeline failed with state: {state}")
                
            time.sleep(check_interval_sec)