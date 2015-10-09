from pybuilder.core import Author, init, use_plugin, task, depends
import os
import shutil
import subprocess

use_plugin("python.core")
use_plugin("python.install_dependencies")
use_plugin("python.distutils")
use_plugin("source_distribution")

default_task = "publish"

name = "neumann"
summary = "Neumann is Neo4j recommendation generator"
description = "Neumann is Neo4j recommendation generator"
authors = [Author("guilherme", "guilherme@vventures.asia"),
           Author("jocki", "jocki@vventures.asia")]
url = "https://github.com/predictry/neumann"
version = "0.3"


def _docker_build(project, logger):
    profile = project.get_property('profile')

    # Creating docker staging directory
    docker_dir = project.expand("$dir_target/docker")
    logger.info("Creating docker staging at {0}".format(docker_dir))
    shutil.rmtree(docker_dir, True)
    os.mkdir(docker_dir)

    # Copying Dockerfile
    logger.info("Copying Dockerfile to {0}".format(docker_dir))
    shutil.copy('Dockerfile', docker_dir)
    artifact_file = project.expand("$dir_dist/dist/neumann-{0}.tar.gz".format(project.version))
    logger.info("Copying {0} to {1}".format(artifact_file, docker_dir))
    shutil.copy(artifact_file, docker_dir)

    # Copying requirements.txt
    requirements_file = project.expand("$basedir/requirements.txt")
    logger.info("Copying {0} to {1}".format(requirements_file, docker_dir))
    shutil.copy(requirements_file, docker_dir)

    # Copying config files to staging directory
    config_dir = project.expand("$basedir/config/{0}".format(profile))
    for filename in os.listdir(project.expand(config_dir)):
        full_filename = os.path.join(config_dir, filename)
        if os.path.isfile(full_filename):
            logger.info("Copying {0} to {1}".format(full_filename, docker_dir))
            shutil.copy(full_filename, docker_dir)

    # Executing docker build
    logger.info("Executing docker build")
    subprocess.call(["docker", "build", "-t", "predictry/neumann-{0}:{1}".format(profile, project.version), docker_dir])


def _docker_run(project, logger):
    profile = project.get_property('profile')

    # Deleting existing container
    logger.info("Stopping container neumann")
    subprocess.call(["docker", "stop", "neumann-{0}-{1}".format(profile, project.version)])
    logger.info("Removing container")
    subprocess.call(["docker", "rm", "-f", "neumann-{0}-{1}".format(profile, project.version)])

    # Running new container
    data_dir = project.expand("$dir_target/tmp/data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
    s3copy_dir = project.expand("$dir_target/tmp/s3copy")
    if not os.path.exists(s3copy_dir):
        os.makedirs(s3copy_dir)
    logger.info("Executing docker run with data_dir [{0}] and s3copy_dir [{1}]".format(data_dir, s3copy_dir))
    subprocess.call(["docker", "run", "-d", "-P", "--name=neumann-{0}-{1}".format(profile, project.version),
                     "-v", "{0}:/app/data".format(data_dir), "-v", "{0}:/app/s3copy".format(s3copy_dir),
                     "--net=host", "predictry/neumann-{0}:{1}".format(profile, project.version)])


@task
@depends("publish")
def docker_build_dev(project, logger):
    project.set_property('profile', 'dev')
    _docker_build(project, logger)


@task
@depends("docker_build_dev")
def docker_run_dev(project, logger):
    project.set_property('profile', 'dev')
    _docker_run(project, logger)


@task
@depends("publish")
def docker_build_prod(project, logger):
    project.set_property('profile', 'prod')
    _docker_build(project, logger)


@task
@depends("docker_build_prod")
def docker_run_prod(project, logger):
    project.set_property('profile', 'prod')
    _docker_run(project, logger)


@task
@depends("docker_build_prod")
def docker_push(project, logger):
    project.set_property('profile', 'prod')
    profile = project.get_property('profile')

    # Tag image
    logger.info("Tagging image")
    subprocess.call(["docker", "tag", "-f", "predictry/neumann-{0}:{1}".format(profile, project.version),
                     "fisher.predictry.com:5000/predictry/neumann-{0}:{1}".format(profile, project.version)])

    # Push image
    logger.info("Push image")
    subprocess.call(["docker", "push",
                     "fisher.predictry.com:5000/predictry/neumann-{0}:{1}".format(profile, project.version)])


@task
def install_dev_configs(project, logger):
    # Copying configuration files
    target_dir = os.path.expanduser('~/.local/share/neumann/config')
    if not os.path.exists(target_dir):
        logger.info("Creating {0}".format(target_dir))
        os.makedirs(target_dir)
    config_dir = project.expand('$basedir/config/dev')
    for filename in os.listdir(project.expand(config_dir)):
        full_filename = os.path.join(config_dir, filename)
        if os.path.isfile(full_filename):
            logger.info("Copying {0} to {1}".format(full_filename, target_dir))
            shutil.copy(full_filename, target_dir)


@init
def initialize(project):
    project.depends_on_requirements("requirements.txt")
