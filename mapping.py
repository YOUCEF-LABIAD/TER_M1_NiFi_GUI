import os
from nipyapi import canvas, nifi


def create_Mapping_process(file_path,proc_group_test, mapping_dict):
    #recuperer le chemin courant
    cwd= os.getcwd()
    print("cwd:", cwd)
    # Create a new process group for the conversion process
    root_pg = canvas.get_root_pg_id()
    pg = canvas.create_process_group(root_pg, "Mapping process group")
    
    #ExecuteStreamCommand
    #ExecuteStreamCommand

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
    
    #Excel_to_csv_processor
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.ExecuteStreamCommand')
    update_param = nifi.models.processor_config_dto.ProcessorConfigDTO()
    Excel_to_csv_processor = canvas.create_processor(proc_group_test,processor_param,(500,100), name="Excel_to_csv", config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['nonzero status','original']
    config.properties = {
        
        'Command Arguments Strategy': 'Command Arguments Property',
        'Command Arguments': 'convert_excel_to_csv.py;"${absolute.path}${filename}"',
        'Command Path' : '/usr/bin/python3',
        'Ignore STDIN':'true',
        'Working Directory':cwd+'/NifiScripts',
        'Argument Delimiter':';',
        'Max Attribute Length':'256'
    }
    canvas.update_processor(Excel_to_csv_processor, config, refresh=True)
    print("PutFile processor created :", Excel_to_csv_processor.id)
    #Creating connexions
    canvas.create_connection(GetFile_processor,Excel_to_csv_processor)
    print("     GetFile - EXcel-to-csv created :" )

    
    #Rajoute_extension_processor
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.UpdateAttribute')
    Rajoute_extension_processor = canvas.create_processor(proc_group_test,processor_param,(500,200), name="Rajoute_extension", config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = []
    config.properties = {
        'Store State': 'Do not store state',
        'Cache Value Lookup Cache Size': '100',
        'filename' : '${filename:substringBeforeLast(".")}.csv',
        'mime.type':'text/csv'
    }
    canvas.update_processor(Rajoute_extension_processor, config, refresh=True)
    print(" processor created :", Rajoute_extension_processor.id)
    #Creating connexions
    canvas.create_connection(Excel_to_csv_processor,Rajoute_extension_processor)
    print("     Excel_to_csv-Rajoute-extension :" ) 

    
    
    #hijri_Date_processor 
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.ExecuteStreamCommand')
    hijri_Date_processor = canvas.create_processor(proc_group_test,processor_param,(500,300), name="hijri_Date", config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['nonzerostatus','original']
    config.properties = {
        'Command Arguments Strategy': 'Command Arguments Property',
        'Command Arguments': cwd+'/NifiScripts/hijri.py',
        'Command Path':'/usr/bin/python3',
        'Ignore STDIN' : 'false',
        'Argument Delimiter':';',
        'Max Attribute Length':'256'
    }
    canvas.update_processor(hijri_Date_processor , config, refresh=True)
    print("hijri_Date_processor created :", hijri_Date_processor .id)
    #Creating connexions
    canvas.create_connection(Rajoute_extension_processor ,hijri_Date_processor)
    print("hijri_Date_processor ,mapping_processor created :" )

    

    #Mapping_processor 
    
    mapping_query = "SELECT"
    +mapping_dict(['PatientNumber']) + " as PatientNumber ",
    +mapping_dict(['Hospital']) + " as Hospital ",
    +mapping_dict(['DateOfBirth']) + " as DateOfBirth ",
    +mapping_dict(['Gender']) + " as Gender ",
    +mapping_dict(['PatientDeceased']) + " as PatientDeceased ",
    +mapping_dict(['DateofDeath']) + " as DateofDeath ",
    +mapping_dict(['PlaceOfBirth']) + " as PlaceOfBirth ",
    +mapping_dict(['EthnicOrigin']) + " as EthnicOrigin ",
    +mapping_dict(['Nationality']) + " as Nationality ",
    #patient_name
    +mapping_dict(['Title']) + " as Title ",
    +mapping_dict(['MothersName']) + " as MothersName ",
    +mapping_dict(['MothersPreName']) + " as MothersPreName ",
    +mapping_dict(['FathersName']) + " as FathersName ",
    +mapping_dict(['FathersPreName']) + " as FathersPreName ",
    +mapping_dict(['FamilyDoctor']) + " as FamilyDoctor ",
    +mapping_dict(['FileDateCreation']) + " as FileDateCreation ",#? pas dans le mapping
    +mapping_dict(['NationalID']) + " as NationalID ",
    #+"SUBSTRING(PATIENT_NAME, 1, POSITION(' ' IN PATIENT_NAME) - 1) AS LastName,"
    #+"SUBSTRING(PATIENT_NAME, POSITION(' ' IN PATIENT_NAME) + 1) AS FirstName,"
    +"FROM FLOWFILE"
    #create mapping query
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.QueryRecord')
    mapping_processor = canvas.create_processor(proc_group_test,processor_param,(500,400), name="mapping_processor", config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['original','failure']
    config.properties = {
        'Record Reader': cwd+'/dataout',
        'Record Writer': 'replace',
        'Include Zero Record FlowFiles':'true',
        'Cache Schema' :'true',
        'Default Decimal Precision':'10',
        'Default Decimal Scale' : '0'
    }
    canvas.update_processor(mapping_processor, config, refresh=True)
    print("PutFile processor created :", mapping_processor.id)
    #Creating connexions
    canvas.create_connection(hijri_Date_processor,mapping_processor)
    print("     GetFile - PutFile created :" )

    

    #Code_ASCII_processor
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.ReplaceText')
    Code_ASCII_processor = canvas.create_processor(proc_group_test,processor_param,(500,500), name=None, config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['failure']
    config.properties = {
        'Replacement Strategy': 'Regex Replace',
        'Search Value': '[^\\x00-\\x7F]+',
        'Character Set' : 'UTF-8',
        'Maximum Buffer Size' : '1 MB',
        'Evaluation Mode' : 'Line-by-Line',
        'Line-by-Line Evaluation Mode' : 'All'

    }
    canvas.update_processor(Code_ASCII_processor, config, refresh=True)
    print(" Code_ASCII_processor created :", Code_ASCII_processor.id)
    #Creating connexions
    canvas.create_connection(mapping_processor,Code_ASCII_processor)



    
    #Special_Characters_processor
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.ReplaceText')
    Special_Characters_processor = canvas.create_processor(proc_group_test,processor_param,(500,600), name=None, config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['failure']
    config.properties = {
        'Replacement Strategy': 'Regex Replace',
        'Search Value': '[^\w\s,-:]',
        'Character Set' : 'UTF-8',
        'Maximum Buffer Size' : '1 MB',
        'Evaluation Mode' : 'Entire text',
        'Line-by-Line Evaluation Mode' : 'All'
       
    }
    canvas.update_processor( Special_Characters_processor, config, refresh=True)
    print(" Special_Characters_processor created :", Special_Characters_processor.id)
    #Creating connexions
    canvas.create_connection(Code_ASCII_processor, Special_Characters_processor)
    

    #Spaces_processor
    spaces_processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.PutFile')
    spaces_processor_param = nifi.models.processor_config_dto.ProcessorConfigDTO()
    spaces_processor = canvas.create_processor(proc_group_test,processor_param,(500,700), name=None, config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['failure']
    config.properties = {
        'Special_Characters': 'Regex Replace',
        'Search Value': '[ ]{2,}',
        'Character Set' : 'UTF-8',
        'Maximum Buffer Size':'1 MB',
        'Evaluation Mode':'Line-by-Line',
        'Line-by-Line Evaluation Mode':'All'
    }
    canvas.update_processor(spaces_processor_param,config, refresh=True)
    print("PutFile processor created :", spaces_processor.id)
    #Creating connexions
    canvas.create_connection(Special_Characters_processor,spaces_processor)
    print("Special_Characters - spaces_processor created :" )

    
    

    #Deduplication_processor
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.ExecuteStreamCommand')
    Deduplication_processor = canvas.create_processor(proc_group_test,processor_param,(500,800), name=None, config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['nonzero status','failure']
    config.properties = {
        
        'Command Arguments Strategy': 'Command Arguments Property',
        'Command Arguments': 'deduplicate.py',
        'Command Path': '/usr/bin/python3',
        'Ignore STDIN':'false',
        'Working Directory':cwd+'/NifiScripts',
        'Argument Delimiter':';',
        'Max Attribute Length':'256'
    }
    canvas.update_processor(Deduplication_processor, config, refresh=True)
    print("PutFile processor created :", Deduplication_processor.id)
    #Creating connexions
    canvas.create_connection(spaces_processor,Deduplication_processor)
    print("     spaces_processor,Deduplication_processor created :" )



    #FileDateCreation_processor
    processor_param = nifi.models.DocumentedTypeDTO(type='org.apache.nifi.processors.standard.ExecuteStreamCommand')
    FileDateCreation_processor = canvas.create_processor(proc_group_test,processor_param,(500,900), name=None, config=None)
    config = nifi.models.ProcessorConfigDTO()
    config.auto_terminated_relationships = ['nonzero status','failure']
    config.properties = {
        
        'Command Arguments Strategy': 'Command Arguments Property',
        'Command Arguments': 'FileDateCreation.py',
        'Command Path': '/usr/bin/python3',
        'Ignore STDIN':'false',
        'Working Directory':cwd+'/NifiScripts',
        'Argument Delimiter':';',
        'Max Attribute Length':'256'
    }
    canvas.update_processor(FileDateCreation_processor, config, refresh=True)
    print("PutFile processor created :", FileDateCreation_processor.id)
    #Creating connexions
    canvas.create_connection(Deduplication_processor,FileDateCreation_processor)
    print("Deduplication_processor,FileDateCreation_processor created :" )
    





    #TODO

    


    
    # Create the ConvertRecord processor
    convert_to_csv_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ConvertRecord",
        "Convert to CSV"
    )

    # Set properties for the ConvertRecord processor

    

    
    # Schedule the process group to start
    canvas.schedule(pg, True)






# Example usage
#create_Mapping_process("/path/to/your/file.csv")