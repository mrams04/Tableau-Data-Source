import cx_Oracle
import tableausdk as tde
import csv
import os,time
import xml.etree.ElementTree as ET
import sys
import requests
import math
from requests.packages.urllib3.fields import RequestField
from requests.packages.urllib3.filepost import encode_multipart_formdata
import ConfigParser
import os
import xml.etree.ElementTree as ET
from pymongo import MongoClient
import re

class tab_ds:

    def __init__(self, p_file_name, p_data, p_type, p_ds_name, p_user):
    
        self.xmlns = {'t': 'http://tableau.com/api'}
        self.VERSION = 2.8
        self.CHUNK_SIZE=1024 * 1024 * 5

        self.FILESIZE_LIMIT = 1024 * 1024 * 64

        config = ConfigParser.ConfigParser()
        config.read('eis-svc-python.application.properties')
        #config.read('/apps/eis/eis-svc-python/config/eis-svc-python.application.properties')
	
        #Database Connection details from the properties file
        self.db_user = config.get('edw', 'eis.db.edw.username')
        self.db_pass = config.get('edw', 'eis.db.edw.password')
        self.db_host = config.get('edw', 'eis.db.edw.host')
        self.db_name = config.get('edw', 'eis.db.edw.dbname')
        self.db_port = config.get('edw', 'eis.db.edw.port')

        #Tableau Connection details from the properties file
        self.server = config.get('tableau', 'eis.svc.tableau.host')
        self.site = config.get('tableau', 'eis.svc.tableau.site')
        self.username = config.get('tableau', 'eis.svc.tableau.username')
	
        self.password = config.get('tableau', 'eis.svc.tableau.password')

        #Mongo DB Connection details from the properties file
        self.mdb_user = config.get('mongo', 'spring.data.mongodb.username')
        self.mdb_pass = config.get('mongo', 'spring.data.mongodb.password')
        self.mdb_host = config.get('mongo', 'spring.data.mongodb.host')
        self.mdb_name = config.get('mongo', 'spring.data.mongodb.authentication-database')
        self.mdb_port = config.get('mongo', 'spring.data.mongodb.port')
	
        self.ds_path = config.get('os', 'eis.os.file.dspath')

        if p_type == 'SQL':
            self.create_ds_sql(p_file_name, p_data)
        else:
	        self.create_ds_data(p_file_name, p_data)
    
        ##### STEP 1: SIGN IN #####
        print("\n 1. Signing in as " + self.username)
        self.auth_token, self.site_id = self.sign_in()

        print "Auth token : " + self.auth_token
        print "site_id : " + self.site_id
	
        self.project_name = self.get_user_folder(p_user)
        print self.project_name
	
        self.project_id = self.get_project_id()
	
        print "Project ID : " + self.project_id
	
        try:
            server_response = self.publish_ds(p_ds_name, p_file_name, p_type, p_user)
        except:
            raise
	

    def create_ds_sql(self, p_file_name, p_data):
        """
        creates the data source using the SQL script passed as a parameter. 
        Uses the existing Sample.tds (XML file), replaces the XML tags with the new SQL and creates a new .tds file 
        """
        #import sample file
        v_sample_file = 'Sample.tds'
        tree = ET.parse(v_sample_file)

        root = tree.getroot()

        v_caption = root.find(".//named-connection")

        #print v_caption.attrib

        v_caption.attrib['caption'] = self.db_host

        v_db_conn = root.find(".//named-connection/connection")

        v_db_conn.attrib['port'] = self.db_port
        v_db_conn.attrib['server'] = self.db_host
        v_db_conn.attrib['service'] = self.db_name
        v_db_conn.attrib['username'] = self.db_user


        elem = root.find(".//relation")
        elem.text = p_data.replace("<", "<<").replace(">", ">>")

        tree.write(self.ds_path + p_file_name,xml_declaration=True, encoding='UTF-8')

	
    def create_ds_data(self, p_file_name, p_data):
    # creates the data source using the JSON object passed as a parameter.
	
        column_name = []
        data_type = []
        field_name = []

        for each in p_data[0]['table']['headers']:
            column_name.append(each['headerName'])
            data_type.append(each['dataType'])
            field_name.append(each['field'])

        tableDef = tde.TableDefinition() 

        for i in range(0, len(column_name)):
            fieldtype = data_type[i]
            fieldname = column_name[i]
    
            fieldtype = str(fieldtype).replace("STRING","15").replace("DATETIME","13").replace("NUMBER","10").replace("DATE","13")
            # Add columns to the table
            tableDef.addColumn(str(fieldname), int(fieldtype)) # if we pass a non-int to fieldtype, it'll fail
        try:
		    # Remove the file from the drive, if it already exists
            os.remove(self.ds_path + p_file_name)
        except:
            pass

        dataExtract = tde.Extract(self.ds_path + p_file_name)
	
        table = dataExtract.addTable('Extract',tableDef)

        record = 1

        #loop though for all the rows in JSON
        for i in p_data[0]['table']['data']:
            columnposition = 0
            newrow = tde.Row(tableDef)

            # loop to get the details for all columns from JSON object
            for j in field_name:
            
                fieldtype = data_type[columnposition]
                fieldname = column_name[columnposition]
	
                if fieldtype == 'STRING':
                    try: 
                        newrow.setCharString(columnposition, str(i[j]))
                    except:
                        newrow.setNull(int(columnposition))

                if fieldtype == 'DATE':
                    try:
                        if len(i[j]) > 10:
                            timechunks = time.strptime(str(i[j]), "%Y-%m-%d %H:%M:%S")
                            newrow.setDateTime(columnposition, timechunks[0], timechunks[1], timechunks[2], timechunks[3], timechunks[4], timechunks[5], 0000)
                        else:
                            timechunks = time.strptime(str(i[j]), "%Y-%m-%d")
                            newrow.setDateTime(columnposition, timechunks[0], timechunks[1], timechunks[2], 0, 0, 0, 0000)

                    except:
                        newrow.setNull(int(columnposition))
					
                if fieldtype == 'NUMBER':
                    try:
                        newrow.setDouble(columnposition, i[j])
                    except:
                        newrow.setNull(int(columnposition))

                columnposition = columnposition + 1
				
            # Insert the row into TDE 'table'
            table.insert(newrow) 
            newrow.close()
            record = record + 1


        dataExtract.close()

    def sign_in(self):
        """
	    Signs in to the server specified with the given credentials specified server address username is the name (not ID) of the user to sign in as.
        Note that most of the functions in this example require that the user have server administrator permissions. password is the password for the user.
        Site is the ID (as a string) of the site on the server to sign in to. Returns the auth token and the site ID.
        """


        print "before sign in"
        url = self.server + "/api/{0}/auth/signin".format(self.VERSION)

        # Builds the request
        xml_request = ET.Element('tsRequest')
        credentials_element = ET.SubElement(xml_request, 'credentials', name=self.username, password=self.password)
        ET.SubElement(credentials_element, 'site', contentUrl=self.site)
        xml_request = ET.tostring(xml_request)

        # Make the request to server
        server_response = requests.post(url, data=xml_request,verify=False)

        # ASCII encode server response to enable displaying to console
        server_response = server_response.text.encode('ascii', errors="backslashreplace").decode('utf-8')

        # Reads and parses the response
        parsed_response = ET.fromstring(server_response)

        # Parse the XML response & get the auth token and site ID
        token = parsed_response.find('t:credentials', namespaces=self.xmlns).get('token')
        site_id = parsed_response.find('.//t:site', namespaces=self.xmlns).get('id')
	
        return token, site_id


    def get_user_folder(self, p_user):
    # returns the folder name belongs to the user. Reads the User metadata from the Mongo DB to identify the user's folder
	    
        client = MongoClient()

        #connect to Mongo DB

        client = MongoClient('mongodb://%s:%s@%s/%s' % (self.mdb_user, self.mdb_pass, self.mdb_host, self.mdb_name))
        db = client.profiles

        # Collection
        collection = db.UserProfile
        #print collection.count()

        #case insensitive search
        person = collection.find_one({"_id":re.compile(p_user, re.IGNORECASE)})
        try:
            return "My Project - "+person['firstName']+' '+person['lastName']
        except:
            raise


    def get_project_id(self):
        """
        Tableau get Projects API returns list of all the Projects on the server in a given Site. Parse the XML output to identify the ID for the provided Project Name.
        Returns the project/ Folder ID for the  project name provided on the Tableau server. server is the specified server address,  auth_token is the authentication token that grants user access to API calls
        site_id is ID of the site that the user is signed into
        If folder doesn't exists, creates the folder/ Project and returns the ID
        """

        page_num, page_size = 1, 100   # Default paginating values
        v_project_id = None
	
        # Builds the request
        url = self.server + "/api/{0}/sites/{1}/projects".format(self.VERSION, self.site_id)
        paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page_num)
        server_response = requests.get(paged_url, headers={'x-tableau-auth': self.auth_token},verify=False)
        server_response = server_response.text.encode('ascii', errors="backslashreplace").decode('utf-8')
	
        xml_response = ET.fromstring(server_response)

        # Used to determine if more requests are required to find all projects on server
        total_projects = int(xml_response.find('t:pagination', namespaces=self.xmlns).get('totalAvailable'))
        max_page = int(math.ceil(total_projects / page_size))

        projects = xml_response.findall('.//t:project', namespaces=self.xmlns)

       # Continue querying if more projects exist on the server
        for page in range(2, max_page + 1):
            paged_url = url + "?pageSize={0}&pageNumber={1}".format(page_size, page)
            server_response = requests.get(paged_url, headers={'x-tableau-auth': self.auth_token})
            server_response = server_response.text.encode('ascii', errors="backslashreplace").decode('utf-8')
            xml_response = ET.fromstring(server_response)
            projects.extend(xml_response.findall('.//t:project', namespaces=self.xmlns))

        # Look through all projects to find the 'default' one
        for project in projects:
            if project.get('name') == self.project_name:
                v_project_id = project.get('id')
    
        if v_project_id == None:
            xml_request = ET.Element('tsRequest')
            element = ET.SubElement(xml_request, 'project', name=self.project_name)
            xml_request = ET.tostring(xml_request)

            publish_url = server + "/api/{0}/sites/{1}/projects".format(self.VERSION, self.site_id)

            # Make the request to create a project and check status code
            print("\tCreating...")
            #print publish_url
		
            server_response = requests.post(publish_url, data=xml_request,
                                            headers={'x-tableau-auth': self.auth_token},verify=False)
		
            print server_response
            # ASCII encode server response to enable displaying to console
            server_response = server_response.text.encode('ascii', errors="backslashreplace").decode('utf-8')
	
            # Reads and parses the response
            parsed_response = ET.fromstring(server_response)

            #print parsed_response
            # Gets the Project ID
            v_project_id = parsed_response.find('t:project', namespaces=self.xmlns).get('id')

        return v_project_id


    def start_upload_session(self):
        """
        Creates a POST request that initiates a file upload session.
        server is specified server address
        auth_token is authentication token that grants user access to API calls
        site_id is ID of the site that the user is signed into
        Returns a session ID that is used by subsequent functions to identify the upload session.
        """
        url = self.server + "/api/{0}/sites/{1}/fileUploads".format(self.VERSION, self.site_id)
        server_response = requests.post(url, headers={'x-tableau-auth': self.auth_token},verify=False)
        server_response = server_response.text.encode('ascii', errors="backslashreplace").decode('utf-8')
        xml_response = ET.fromstring(server_response)
        return xml_response.find('t:fileUpload', namespaces=self.xmlns).get('uploadSessionId')


    def publish_ds(self, p_ds_name, p_file_name, p_input_type, p_user):
        file_name_path = self.ds_path + p_file_name
        file_name_path = os.path.abspath(file_name_path)

        # Datasource file with extension, without full path
        file_name_ext = os.path.basename(file_name_path)

        print("\n*Publishing '{0}' to the default project *".format(file_name_ext))
    
        if not os.path.isfile(file_name_path):
            error = "{0}: file not found".format(file_name_path)
            raise IOError(error)

        # Break datasource file by name and extension
        file_name, file_extension = file_name_ext.split('.', 1)

        # Get size to check if chunking is necessary
        file_size = os.path.getsize(file_name_path)
        print 'file_size  '+ str(file_size)
	
        if file_size >= self.FILESIZE_LIMIT:
            chunked = True
        else:
            chunked = False


        xml_request = ET.Element('tsRequest')

        element = ET.SubElement(xml_request, 'datasource', name=p_ds_name)
        if p_input_type == 'SQL':
            ET.SubElement(element, 'connectionCredentials', name=self.db_user, password=self.db_pass, embed="True" )

            ET.SubElement(element, 'project', id=self.project_id)
            xml_request = ET.tostring(xml_request)

        if chunked:
            print("\n3. Publishing '{0}' in {1}MB chunks (file over 64MB)".format(file_name, CHUNK_SIZE / 1024000))
            # Initiates an upload session
            uploadID = self.start_upload_session()

            # URL for PUT request to append chunks for publishing
            put_url = server + "/api/{0}/sites/{1}/fileUploads/{2}".format(self.VERSION, self.site_id, uploadID)

            # Read the contents of the file in chunks of 100KB
            with open(file_name_path, 'rb') as f:
                while True:
                    data = f.read(self.CHUNK_SIZE)
                    if not data:
                        break
                    payload, content_type = self._make_multipart({'request_payload': ('', '', 'text/xml'),
                                                                  'tableau_file': ('file', data, 'application/octet-stream')})
                    print("\tPublishing a chunk...")
                    server_response = requests.put(put_url, data=payload,
                                                   headers={'x-tableau-auth': self.auth_token, "content-type": content_type}, verify=False)


            # Finish building request for chunking method
            payload, content_type = self._make_multipart({'request_payload': ('', xml_request, 'text/xml')})

            publish_url = self.server + "/api/{0}/sites/{1}/datasources".format(self.VERSION, self.site_id)
            publish_url += "?uploadSessionId={0}".format(uploadID)
            publish_url += "&datasourceType={0}&overwrite=true".format(file_extension)
        else:
            print("\n3. Publishing '" + file_name + "' using the all-in-one method (datasources under 64MB)")
            # Read the contents of the file to publish
            with open(file_name_path, 'rb') as f:
                bytes = f.read()

            if p_input_type == 'SQL':
                file_name = file_name+'.tds'
            else:
                file_name = file_name+'.tde'

            parts = {'request_payload': ('', xml_request, 'text/xml'), 
		             'tableau_datasource': (file_name, bytes, 'application/octet-stream')}

            payload, content_type = self._make_multipart(parts)

            publish_url = self.server + "/api/{0}/sites/{1}/datasources".format(2.8, self.site_id)
            publish_url += "?datasourceType={0}&overwrite=true".format(file_extension)
            #publish_url += "?overwrite=true"

        # Make the request to publish and check status code
        print("\tUploading...")
        print publish_url
		
        server_response = requests.post(publish_url, data=payload,
                                        headers={'x-tableau-auth': self.auth_token, 'content-type': content_type},verify=False)

        self._check_status(server_response, 201)
        # ASCII encode server response to enable displaying to console

        server_response = server_response.text.encode('ascii', errors="backslashreplace").decode('utf-8')

        # Reads and parses the response
        parsed_response = ET.fromstring(server_response)

        # Gets the Data Source ID
        ds_id = parsed_response.find('.//t:datasource', namespaces=self.xmlns).get('id')

	    # Update the owner of the DataSource
        try:
            url = self.server + "/api/{0}/sites/{1}/users?filter=name:eq:{2}".format(self.VERSION, self.site_id, p_user)
            server_response1 = requests.get(url, headers={'x-tableau-auth': self.auth_token},verify=False)

            print server_response1.text
		
            server_response1 = server_response1.text.encode('ascii', errors="backslashreplace").decode('utf-8')

            # Reads and parses the response
            parsed_response = ET.fromstring(server_response1)

            # Gets the User ID
            tab_user_id = parsed_response.find('.//t:user', namespaces=self.xmlns).get('id')
            print "User ID   "+tab_user_id

            xml_request = ET.Element('tsRequest')
            element = ET.SubElement(xml_request, 'datasource', isCertified="True")
            ET.SubElement(element, 'project', id=self.project_id)
            ET.SubElement(element, 'owner', id=tab_user_id)
		
            xml_request = ET.tostring(xml_request)

            #print xml_request
            url = self.server + "/api/{0}/sites/{1}/datasources/{2}".format(self.VERSION, self.site_id, ds_id)
		
		    #set the Owner ID
            server_response2 = requests.put(url, data=xml_request,
                                        headers={'x-tableau-auth': self.auth_token}, verify=False)

     
            self._check_status(server_response2, 200)

            self.update_connection(ds_id)
		
        except:
            pass
		
        return server_response


    def update_connection(self, p_ds_id):

    ## After the Owner update the Embedded password is not retained in the Data Source in Linux. Hence we are force to update the DS Connection again. 
    ## Strange behavior. This is not needed if the script is running on Windows

        try:
            url = self.server + "/api/{0}/sites/{1}/datasources/{2}/connections".format(self.VERSION, self.p_site_id, self.p_ds_id)
            server_response = requests.get(url, headers={'x-tableau-auth': self.auth_token},verify=False)
            server_response = server_response.text.encode('ascii', errors="backslashreplace").decode('utf-8')

            # Reads and parses the response
            parsed_response = ET.fromstring(server_response)

            # Gets the Data Source connection ID

            connection_id = parsed_response.find('.//t:connection', namespaces=self.xmlns).get('id')

            xml_request = ET.Element('tsRequest')
            element = ET.SubElement(xml_request, 'connection', userName = self.db_user, password = self.db_pass, embedPassword="True")
            xml_request = ET.tostring(xml_request)

            print xml_request
            url = self.server + "/api/{0}/sites/{1}/datasources/{2}/connections/{3}".format(self.VERSION, self.site_id, p_ds_id, connection_id)
		
            server_response2 = requests.put(url, data=xml_request,
                                            headers={'x-tableau-auth': self.auth_token})

            self._check_status(server_response2, 200)

        except:
            pass

	

    def _check_status(self, server_response, success_code):
        """
        Checks the server response for possible errors.
        server_response is the response received from the server
        success_code is the expected success code for the response
        Throws an ApiCallError exception if the API call fails.
        """
        if server_response.status_code != success_code:
            parsed_response = ET.fromstring(server_response.text)

            # Obtain the 3 xml tags from the response: error, summary, and detail tags
            error_element = parsed_response.find('t:error', namespaces=xmlns)
            summary_element = parsed_response.find('.//t:summary', namespaces=xmlns)
            detail_element = parsed_response.find('.//t:detail', namespaces=xmlns)

            # Retrieve the error code, summary, and detail if the response contains them
            code = error_element.get('code', 'unknown') if error_element is not None else 'unknown code'
            summary = summary_element.text if summary_element is not None else 'unknown summary'
            detail = detail_element.text if detail_element is not None else 'unknown detail'
            error_message = '{0}: {1} - {2}'.format(code, summary, detail)
            print error_message
            raise ApiCallError(error_message)
        return

    def _make_multipart(self, parts):
        """
        Creates one "chunk" for a multi-part upload
        'parts' is a dictionary that provides key-value pairs of the format name: (filename, body, content_type).
        Returns the post body and the content type string.
        For more information, see this post:
        http://stackoverflow.com/questions/26299889/how-to-post-multipart-list-of-json-xml-files-using-python-requests
        """
        mime_multipart_parts = []
        for name, (filename, blob, content_type) in sorted(parts.items()):
            multipart_part = RequestField(name=name, data=blob, filename=filename)
            multipart_part.make_multipart(content_type=content_type)
            mime_multipart_parts.append(multipart_part)

        post_body, content_type = encode_multipart_formdata(mime_multipart_parts)
        content_type = ''.join(('multipart/mixed',) + content_type.partition(';')[1:])
        return post_body, content_type
