#this program assumes there is already a nifi instance running
#then requests the root_pg_id(successfully)
#then creates a new process group,
#then populates it with processors, 
#then connects these processors,
#sets up the ports, 
#and starts the pg

import os
import string
import nipyapi
from nipyapi import nifi
from nipyapi import canvas
import warnings

import urllib3

# Ignore the InsecureRequestWarning
warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)


cwd= os.getcwd()
print("cwd:", cwd)
# Disable SSL certificate verification
print("Disabling SSL certification and hostname verification; FOR TESTING ONLY!!!")
nipyapi.config.nifi_config.verify_ssl = False
#nipyapi.config.nifi_config.check_hostname = False




# connect to Nifi
print("Setting NiFi endpoint...")
nipyapi.utils.set_endpoint("https://localhost:8443/nifi-api", 
                           login=True,
                           ssl=True,
                           username="3ca4a9fa-bda3-481b-8f9d-75d45efcb595",
                             password="JiRRlJjRDyhEw9zobQfD+mWT8S/8l1fd")

# wait for connection to be set up
print("waiting for server to be up...")
try:
    connected = nipyapi.utils.wait_to_complete(
        test_function=nipyapi.utils.is_endpoint_up,
        endpoint_url="https://localhost:8443/nifi",
        nipyapi_delay=nipyapi.config.long_retry_delay,
        nipyapi_max_wait=nipyapi.config.short_max_wait,
    )
except Exception as e:
    print("Exception raised: ",e)
    print("If you just started NiFi, wait a few moments before retrying, NiFi tends to take a while before starting to accept requests ")
    print("Exiting program now.")
    quit()

# connect to Nifi Registry
#print("Connecting to NiFi registry...")
#nipyapi.utils.set_endpoint("http://localhost:18080/nifi-registry-api")
#connected = nipyapi.utils.wait_to_complete(
#    test_function=nipyapi.utils.is_endpoint_up,
#    endpoint_url="http://localhost:18080/nifi-registry",
#    nipyapi_delay=nipyapi.config.long_retry_delay,
#    nipyapi_max_wait=nipyapi.config.short_max_wait
#)
#print("Connected:)")
print("Setting up SSL context...")
try:
    nipyapi.security.set_service_ssl_context(service='nifi',
        ca_file=None,
        client_cert_file="./ssl/certificate.crt",
        client_key_file=None,
        client_key_password=None,
        check_hostname=None)
except Exception as e:
    print("Failed to set up SSL context; Exception raised: ",e)
    print("contents of current:")
    print(os.listdir())
    print("contents of ./ssl/:")
    print(os.listdir("./ssl/"))
    print("Exiting program now.")
    quit()
print("ssl context set up")
#idk
#nipyapi.security.service_login(service='nifi',
##    username='3ca4a9fa-bda3-481b-8f9d-75d45efcb595',
#    password='JiRRlJjRDyhEw9zobQfD+mWT8S/8l1fd',
#    bool_response=False)


#Getting process group ID
print("First contact with the NiFi instance:\n      Getting process group ID...        :nervous:")
try:
    #print("\n##NOTE: Le Warning suivant est dû a la desactivation de la verification decertificat SSL; Ceci est fait pour des raisons de developpement seulement;")
    root_pg_id = canvas.get_root_pg_id()
except Exception as e:
    print("failed to get process grop ID: ", e)
    print("Exiting program now.")
    quit()
print("\nprocess group id = ", root_pg_id)

root_pg = canvas.get_process_group( root_pg_id, 'id')
#creating new pg
proc_group_test = canvas.create_process_group(root_pg,"File Fetch ang put test Group", (0,1000),
                        'First process group created witn nipyapi, yay!; meant to be a test;')
print("process group created:", proc_group_test.id)


#trying processors


#getfile
processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.GetFile')
update_param = nifi.models.processor_config_dto.ProcessorConfigDTO()
GetFile_processor = canvas.create_processor(proc_group_test,processor_param,(0,0), name=None, config=None)
config = nifi.models.ProcessorConfigDTO()
config.auto_terminated_relationships = ['success']
config.properties = {
    'Input Directory': cwd + '/datain',
    'File Filter': ".*\\.xlsx$"
}
canvas.update_processor(GetFile_processor, config, refresh=True)
print("GetFile processor created :", GetFile_processor.id)
#TODO: when the user chooses a file, find a way to separate the path to file anf filename

#convert?

#TODO: convert



#Putfile
processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.PutFile')
update_param = nifi.models.processor_config_dto.ProcessorConfigDTO()
PutFile_processor = canvas.create_processor(proc_group_test,processor_param,(500,0), name=None, config=None)
config = nifi.models.ProcessorConfigDTO()
config.auto_terminated_relationships = ['success','failure']
config.properties = {
    'Directory': cwd+'/dataout',
    'Conflict Resolution Strategy': 'replace',
    'Create Missing Directories' : 'true'
}
canvas.update_processor(PutFile_processor, config, refresh=True)
print("PutFile processor created :", PutFile_processor.id)
#Creating connexions
canvas.create_connection(GetFile_processor,PutFile_processor)
print("     GetFile - PutFile created :" )
#Return type (ConnectionEntity)
invalid_processor_list = nipyapi.canvas.list_invalid_processors(proc_group_test.id, summary=True)
if len(invalid_processor_list)!=0:
    print("\n\n*******there are invalid processors*******\n")
    print(invalid_processor_list)
    canvas.delete_process_group(proc_group_test, force=True, refresh=True)#force:Experimental
    quit()



#scheduling processors
try:
    canvas.schedule_processor(PutFile_processor, True, refresh=True)
    canvas.schedule_processor(GetFile_processor, True, refresh=True)
except Exception as e:
    print("Error scheduling processors, Deleting process group and quitting")
    canvas.delete_process_group(proc_group_test, force=True, refresh=True)#force:Experimental
    quit()
#input_port = canvas.create_port(proc_group_test.id, "INPUT_PORT", "RUNNING",nifi.models.PortDTO.TYPE_INPUT)
#output_port = canvas.create_port(proc_group_test.id, "OUTPUT_PORT", "RUNNING",nifi.models.PortDTO.TYPE_OUTPUT)

# Connect ports to processors
#canvas.create_connection(input_port, GetFile_processor)
#canvas.create_connection(PutFile_processor, output_port)

#Start process group
canvas.schedule_process_group(proc_group_test.id, True)
#is there any invalid processors