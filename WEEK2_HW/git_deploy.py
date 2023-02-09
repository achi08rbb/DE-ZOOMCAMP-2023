from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_web_to_gcs import etl_web_to_gcs_parent
from prefect.filesystems import GitHub

github_block = GitHub.load("gitzoomcamp")
docker_block = DockerContainer.load("zoom")

docker_dep = Deployment.build_from_flow(
    flow=etl_web_to_gcs_parent, name='git-docker-flow', infrastructure=docker_block, storage=github_block) # environment where you would run the deployment, you can use kubernetees, gc run,

if __name__=="__main__":
    docker_dep.apply()


prefect deployment build ./etl_web_to_gcs.py:etl_web_to_gcs_parent -n 'git-docker-flow' -q 'default' -ib 'DockerContainer/zoom' -sb 'github/gitzoomcamp'
--apply    