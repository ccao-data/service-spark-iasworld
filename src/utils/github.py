import os
import time

import jwt
import requests

from utils.helpers import create_python_logger

logger = create_python_logger(__name__)


class GitHubClient:
    def __init__(self, gh_pem_path: str) -> None:
        """
        Class to generate and store the credentials associated with GitHub,
        along with methods to dispatch a GitHub Actions workflow.

        Attributes:
            gh_pem_path: Container path to the GitHub certificate file.
            gh_app_id: GitHub Application ID for running a workflow.
            gh_api_url: GitHub URL prefix to use for workflow triggers.
            gh_jwt: GitHub JSON Web Token for authenticating with the API.
        """
        self.gh_pem_path = gh_pem_path
        self.gh_app_id = os.getenv("GH_APP_ID")
        self.gh_api_url = "https://api.github.com/repos/"
        self.gh_jwt = self.create_jwt_token()

    def create_jwt_token(self) -> str:
        """
        Generates a JSON web token for authentication with the GitHub API.

        Returns:
            str: The JWT token for authentication.
        """

        with open(self.gh_pem_path, "rb") as pem_file:
            signing_key = jwt.jwk_from_pem(pem_file.read())

        payload = {
            "iat": int(time.time()),
            "exp": int(time.time()) + 60,
            "iss": self.gh_app_id,
        }

        jwt_instance = jwt.JWT()
        encoded_jwt = jwt_instance.encode(payload, signing_key, alg="RS256")

        return encoded_jwt

    def run_workflow(
        self, repository, workflow, inputs: dict | None = None
    ) -> None:
        """
        Dispatch a GitHub Actions workflow using the GitHub API.

        Args:
            repository: API URL for the target repository containing a workflow.
            workflow: Workflow YAML file, relative to `.github/workflows`.
            inputs: Optional dict of input variables.
        """

        def create_headers(bearer: str) -> dict:
            """Create headers with different bearers for the GitHub API requests"""
            headers = {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {bearer}",
                "X-GitHub-Api-Version": "2022-11-28",
            }
            return headers

        if self.gh_app_id and self.gh_pem_path:
            try:
                response = requests.get(
                    "https://api.github.com/app/installations",
                    headers=create_headers(self.gh_jwt),
                )
                response.raise_for_status()
                gh_tokens_url = response.json()[0]["access_tokens_url"]

                response = requests.post(
                    gh_tokens_url, headers=create_headers(self.gh_jwt)
                )
                response.raise_for_status()
                gh_token = response.json()["token"]

                data: dict[str, str | dict] = {"ref": "master"}
                if inputs is not None:
                    data["inputs"] = inputs

                response = requests.post(
                    f"{repository}/actions/workflows/{workflow}/dispatches",
                    headers=create_headers(gh_token),
                    json=data,
                )
                response.raise_for_status()
                logger.info(f"GitHub workflow triggered: {workflow}")

            except Exception as e:
                logger.error(f"GitHub workflow run failed: {e}")
