"""
Command-line interface to Hadoop, Written by jinchao
"""

import os
import logging

from galaxy.model import Job
job_states = Job.states

from galaxy.jobs.runners.cli_job import BaseJobExec

log = logging.getLogger( __name__ )

__all__ = ('Hadoop',)

mapper_template = """#!/bin/sh
# Prepare job working directory
mkdir %s
mkdir %s 

# Preapre file directory
mkdir %s

# Prepare input file and output file in local filesystem
cat > %s

# Set galaxy environment
GALAXY_LIB="%s"
if [ "$GALAXY_LIB" != "None" ]; then
    if [ -n "$PYTHONPATH" ]; then
        export PYTHONPATH="$GALAXY_LIB:$PYTHONPATH"
    else
        export PYTHONPATH="$GALAXY_LIB"
    fi
fi

# Get env setup clause
%s
# Cd workding directory
cd %s
# Start execution
%s
# Ecfile
echo $? > %s

# Put output data to stdout
cat %s
"""

driver_template2 = '%s/bin/hadoop fs -rmr %s && \
%s/bin/hadoop fs -rmr %s && \
%s/bin/hadoop fs -mkdir %s && \
%s/bin/hadoop fs -put %s %s && \
%s/bin/hadoop jar %s -mapper %s -reducer %s -input %s -output %s -file %s -numReduceTasks 1 && \
rm %s && \
%s/bin/hadoop fs -get %s %s'


driver_template = '%s/bin/hadoop fs -mkdir %s && \
%s/bin/hadoop fs -put %s %s && \
%s/bin/hadoop jar %s -mapper %s -reducer %s -input %s -output %s -file %s -numReduceTasks 1 && \
rm %s && \
%s/bin/hadoop fs -get %s %s'

class Hadoop(BaseJobExec):
    def __init__(self, **params):
        # TODO: to be updated with a configuration file
        self.hdfs_input = '/galaxy_input'
        self.hdfs_output = '/galaxy_output'
        self.hadoop_home = '/opt/hadoop'
        self.streaming_jar = '/opt/hadoop/contrib/streaming/hadoop-streaming-1.1.2.jar'
        self.galaxy_home = '/home/test/galaxy-galaxy-dist-cc0fe62e54dc'

        self.params = {}
        for k, v in params.items():
            self.params[k] = v

    def get_job_template(self, ofile, efile, job_name, job_wrapper, command_line, ecfile):
        # Get mapper input/output file names
        mapper_input_fname = job_wrapper.get_input_fnames()[0]
        mapper_output_fname = job_wrapper.get_output_fnames()[0].real_path 
        # Get id tag
        id_tag = job_wrapper.get_id_tag()

        # Prepare hdfs input files
        # self.__prepare_hdfs_input_files(mapper_input_fname, id_tag)

        # Fill mapper template
        return mapper_template % (self.galaxy_home + "/database/job_working_directory/000",
                                  self.galaxy_home + "/database/job_working_directory/000" + "/" + id_tag,
                                  self.galaxy_home + "/database/files/000",
                                  mapper_input_fname,
                                  job_wrapper.galaxy_lib_dir,
                                  job_wrapper.get_env_setup_clause(),
                                  os.path.abspath(job_wrapper.working_directory),
                                  command_line,
                                  ecfile,
                                  mapper_output_fname)

    def submit(self, script_file, job_wrapper):
        # Get id tag
        id_tag = job_wrapper.get_id_tag()
        # Get mapper file name
        mapper_file_paths = script_file.split("/")
        mapper_short_name = mapper_file_paths[len(mapper_file_paths) - 1]
        # Get mapper input/output file name
        mapper_input_fname = job_wrapper.get_input_fnames()[0]
        mapper_output_fname = job_wrapper.get_output_fnames()[0].real_path 
        # Get hdfs output file name, there is only one reducer
        hdfs_output_fname = self.hdfs_output + "/" + id_tag + '/part-00000'
        # Get reducer command
        # reducer_output_fname = os.path.abspath(job_wrapper.working_directory) + "/" + "reducer_output.dat"
        # reducer_command = reducer_template % (reducer_output_fname, reducer_output_fname)

        print "\nHadoop commands:"
        print driver_template2 % (self.hadoop_home, 
                                  self.hdfs_output + "/" + id_tag, 
                                  self.hadoop_home, 
                                  self.hdfs_input + "/" + id_tag, 
                                  self.hadoop_home, 
                                  self.hdfs_input + "/" + id_tag, 
                                  self.hadoop_home, 
                                  mapper_input_fname,
                                  self.hdfs_input + "/" + id_tag, 
                                  self.hadoop_home, 
                                  self.streaming_jar, 
                                  mapper_short_name, 
                                  '/bin/cat', 
                                  self.hdfs_input + '/' + id_tag, 
                                  self.hdfs_output + '/' + id_tag, 
                                  script_file,
                                  mapper_output_fname,
                                  self.hadoop_home,
                                  hdfs_output_fname,
                                  mapper_output_fname)
        return driver_template % (self.hadoop_home, 
                                  self.hdfs_input + "/" + id_tag, 
                                  self.hadoop_home, 
                                  mapper_input_fname,
                                  self.hdfs_input + "/" + id_tag, 
                                  self.hadoop_home, 
                                  self.streaming_jar, 
                                  mapper_short_name, 
                                  '/bin/cat', 
                                  self.hdfs_input + '/' + id_tag, 
                                  self.hdfs_output + '/' + id_tag, 
                                  script_file,
                                  mapper_output_fname,
                                  self.hadoop_home,
                                  hdfs_output_fname,
                                  mapper_output_fname)

    def parse_job_info(self, job_info):
        index = job_info.index("job_")
        return job_info[index : (index + 21)]

    def delete(self, job_id):
        print "\nStop job"
        return '%s/bin/hadoop job -kill %s' % (self.hadoop_home, job_id)

    def get_status(self, job_ids=None):
        print "\nGet status"
        return '%s/bin/hadoop job -list all' % self.hadoop_home

    def get_single_status(self, job_id):
        print "\nGet single status"
        return '%s/bin/hadoop job -status %s' % (self.hadoop_home, job_id)

    def parse_status(self, status, job_ids):
        print "\nParse status"
        rval = {}
        for line in status.strip().splitlines():
            line = line.strip()
            values = line.split("\t")
            if values is not None:
                index = -1
                try:
                    index =  values[0].index('job_')
                except Exception, e:
                    index = -1
                if index != -1:
                    job_id = values[0]
                    state = values[1]
                    rval[job_id] = self.__get_job_state(state)
        print "\nParse status RVAL:"
        print rval
        return rval

    # TODO
    def parse_single_status(self, status, job_id):
        print "\nParse single status"
        '''
        for line in status.splitlines():
            line = line.split(' = ')
            if line[0] == 'job_state':
                return line[1]
        # no state found, job has exited
        '''
        return job_states.OK

    def handle_output_data(self, job_wrapper):
        # Get local output file name
        local_output_fname = job_wrapper.get_output_fnames()[0].real_path 
        # Get hdfs output file name, there is only one reducer
        id_tag = job_wrapper.get_id_tag()
        hdfs_output_fname = self.hdfs_output + "/" + id_tag + '/part-00000'

        # Put hdfs data to local file
        os.system("rm " + local_output_fname)
        os.system(self.hadoop_home + "/bin/hadoop fs -get " + hdfs_output_fname + " " + local_output_fname)
        
    def __get_job_state(self, state):
        return { '1' : job_states.RUNNING,
                 '2' : job_states.OK,
                 '3' : job_states.ERROR,
                 '4' : job_states.QUEUED }.get(state, state)

    def __prepare_hdfs_input_files(self, input_fname, id_tag):
        # Prepare HDFS DIRs
        os.system(self.hadoop_home + "/bin/hadoop fs -rmr " + self.hdfs_input + "/" + id_tag)
        os.system(self.hadoop_home + "/bin/hadoop fs -mkdir " + self.hdfs_input + "/" + id_tag)
        os.system(self.hadoop_home + "/bin/hadoop fs -rmr " + self.hdfs_output + "/" + id_tag)
        # Prepare HDFS input files
        os.system(self.hadoop_home + "/bin/hadoop fs -put " + input_fname + " " + self.hdfs_input + "/" + id_tag)
