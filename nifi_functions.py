import warnings
import requests
import json
import nipyapi
import urllib3
#function to wait for the NiFi server to be reachable (up to a while)
def wait_for_endpoint_to_be_up(url):
    try:
        connected = nipyapi.utils.wait_to_complete(
            test_function=nipyapi.utils.is_endpoint_up,
            endpoint_url=url, #"https://localhost:8443/nifi"
            nipyapi_delay=nipyapi.config.long_retry_delay,
            nipyapi_max_wait=nipyapi.config.short_max_wait,
        )
    except Exception as e:
        print("Exception raised: ",e)
        print("If you just started NiFi, wait a few moments before retrying, NiFi tends to take a while before starting to accept requests ")
        print("Exiting program now.")
        return False
    
    return connected 


def login_nifi(usr, psswrd):#onmy useful for debug; bool-response is false so i should get an error insteaf of False
    # Ignore the InsecureRequestWarning; helps with terminal crowding; 
    #temporary solution, TODO: remove after SSL certificate validation
    warnings.filterwarnings("ignore", category=urllib3.exceptions.InsecureRequestWarning)
    # Disable SSL certificate verification
    nipyapi.config.nifi_config.verify_ssl = False
    #Setting NiFi endpoint
    try:
        nipyapi.utils.set_endpoint("https://localhost:8443/nifi-api", 
                           login=True,
                           ssl=True,
                           username=usr,
                             password=psswrd)
    except Exception as e:
        print("exception occured during endpoint set up: ",e)
        return False
    try:
        nipyapi.security.service_login(service='nifi',username=usr, password=psswrd,bool_response=True)

    except Exception as e:
        print("exception occured during login: ",e)
        return False
    return True