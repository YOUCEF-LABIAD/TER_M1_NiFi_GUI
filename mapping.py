from nipyapi import canvas, nifi


def create_Mapping_process(file_path):
    # Create a new process group for the conversion process
    root_pg = canvas.get_root_pg_id()
    pg = canvas.create_process_group(root_pg, "Mapping process group")
    
    #ExecuteStreamCommand
    #ExecuteStreamCommand

    # Create the ExecuteStreamCommand processor (ExcelToCsv)
    GetFile_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.GetFile",
        ""
    ) 
    Excel_to_csv_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        ""
    )    
    UpdateAttribute_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.UpdateAttribute",
        ""
    )   
    Hijri_Converter_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        ""
    )   
    Mapping_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.QueryRecord",
        ""
    )
    Code_ASCII_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ReplaceText",
        ""
    )
    Special_Characters_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ReplaceText",
        ""
    )   
    Spaces_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ReplaceText",
        ""
    )
    Deduplication_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ExecuteStreamCommand",
        ""
    )
    FileDateCreation_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ReplaExecuteStreamCommandceText",
        ""
    )
    # Set properties for the ExecuteStreamCommand processor
    canvas.set_property(Excel_to_csv_processor, "Command Arguments Strategy", "Command Arguments Property")
    canvas.set_property(Excel_to_csv_processor, "Command Arguments", 'convert_excel_to_csv.py;"${absolute.path}${filename}"')
    canvas.set_property(Excel_to_csv_processor, "Ignore STDIN", "true")
    canvas.set_property(Excel_to_csv_processor, "Command Path", "/usr/bin/python3")
    canvas.set_property(Excel_to_csv_processor, "Working Directory", "./scripts")
    canvas.set_property(Excel_to_csv_processor, "Argument Delimiter", ";")
    
    # Set properties for the UpdateAttribute processor
    canvas.set_property(UpdateAttribute_processor, "Key1", "Value1")
    canvas.set_property(UpdateAttribute_processor, "Key2", "Value2")
    # Set properties for the Hijri_Converter_processor processor
    # Set properties for the Mapping_processor processor
    # Set properties for the Code_ASCII_processor processor
    # Set properties for the Special_Characters_processor processor
    # Set properties for the Spaces_processor processor
    # Set properties for the Deduplication_processor processor
    # Set properties for the FileDateCreation_processor processor
    

    # Connect the process group to other processors or output ports
    canvas.create_connection(Excel_to_csv_processor, UpdateAttribute_processor)
    canvas.create_connection(UpdateAttribute_processor, Hijri_Converter_processor)
    canvas.create_connection(Hijri_Converter_processor, Mapping_processor)
    canvas.create_connection(Mapping_processor,Code_ASCII_processor )
    canvas.create_connection(Code_ASCII_processor, Special_Characters_processor)
    canvas.create_connection(Special_Characters_processor, Spaces_processor)
    canvas.create_connection(Spaces_processor, Deduplication_processor)
    canvas.create_connection(Deduplication_processor, FileDateCreation_processor)

    

    
    # Create the ConvertRecord processor
    convert_to_csv_processor = canvas.create_processor(
        pg,
        "org.apache.nifi.processors.standard.ConvertRecord",
        "Convert to CSV"
    )

    # Set properties for the ConvertRecord processor
    canvas.set_property(convert_to_csv_processor, "Record Reader", "CSV Reader")
    canvas.set_property(convert_to_csv_processor, "Record Writer", "CSV Record Set Writer")


    

    # Create input and output ports
    input_port = canvas.create_port(pg, "INPUT_PORT", nifi.models.PortDTO.TYPE_INPUT)
    output_port = canvas.create_port(pg, "OUTPUT_PORT", nifi.models.PortDTO.TYPE_OUTPUT)

    # Connect ports to processors
    canvas.create_connection(input_port, convert_to_csv_processor)
    canvas.create_connection(convert_to_csv_processor, output_port)
    
    # Schedule the process group to start
    canvas.schedule(pg, True)

# Example usage
create_Mapping_process("/path/to/your/file.csv")







import nipyapi

# Connect to NiFi instance
nipyapi.config.nifi_config.host = 'http://localhost:8080/nifi-api'

# Start the process group or processor
def start_process_group(process_group_id):
    nipyapi.canvas.schedule_process_group(process_group_id, True)

def start_processor(processor_id):
    nipyapi.canvas.schedule_processor(processor_id, True)

# Monitor the process or processor
def monitor_process_group(process_group_id):
    status = nipyapi.canvas.get_process_group_status(process_group_id)
    # Extract relevant information from the status object and monitor the process group

def monitor_processor(processor_id):
    status = nipyapi.canvas.get_processor_status(processor_id)
    # Extract relevant information from the status object and monitor the processor

# Handle errors
def handle_errors(process_group_id):
    error_messages = nipyapi.canvas.list_process_group_errors(process_group_id)
    if error_messages:
        # Handle the error messages appropriately
        pass

# Stop the process group or processor
def stop_process_group(process_group_id):
    nipyapi.canvas.schedule_process_group(process_group_id, False)

def stop_processor(processor_id):
    nipyapi.canvas.schedule_processor(processor_id, False)

# Example usage
pg_id = 'your-process-group-id'  # Replace with the actual process group ID
processor_id = 'your-processor-id'  # Replace with the actual processor ID

start_process_group(pg_id)
# or start_processor(processor_id)

monitor_process_group(pg_id)
# or monitor_processor(processor_id)

handle_errors(pg_id)

stop_process_group(pg_id)
# or stop_processor(processor_id)










#to access finished flowfile and write into a file

import nipyapi

# Connect to NiFi instance
nipyapi.config.nifi_config.host = 'http://localhost:8080/nifi-api'

# Get the output port ID
output_port_id = 'output-port-id'  # Replace with the actual output port ID

# Get the content from the output port
content = nipyapi.nifi.output_port.get_output_port_status(output_port_id).aggregate_snapshot.contents

# Process the content as per your requirements
# For example, you can write the content to a file
with open('output.txt', 'wb') as file:
    for chunk in content.stream():
        file.write(chunk)

print("Data retrieved successfully.")
