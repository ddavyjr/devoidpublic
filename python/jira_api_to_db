import getpass
import datetime
import argparse
from jira import JIRA
import json
import csv
import psycopg2
import urllib.request
import base64
from lib import common



def get_jira_data(jql_query):
    """
    Establishes connection to Jira instance and pulls back a JSON containing issues specified in the JQL parameter.
    Loops through all the issues to parse out selected fields and writes them to a ta delmited file.
    Copies the file into a stage table on postgres.
    """
    dds_issues = []

    dir_str = config.get('main', 'temp.directory')
    txt_file_str = dir_str + '/jira_tickets.txt'
    json_file_str = dir_str + '/jira_tickets.json'    

    file = open(txt_file_str,'w+', newline='', encoding='utf-8-sig')
    
    logger.info("Searching for issues with JQL: " + jql_query)    

    iter = 0
    chunk_size = 1000
    issue_total = 1000
    while iter < issue_total:
        
        jira_issues_chunk = jira.search_issues(jql_str=jql_query,startAt=iter,maxResults=chunk_size,fields='*all', json_result=True)
        iter += chunk_size
        
        jira_dump = json.dumps(jira_issues_chunk)
        data_load = json.loads(jira_dump)

        with open(json_file_str, 'w') as f:
            json.dump(jira_issues_chunk, f)

        issue_total = data_load['total']
        logger.info(str(iter) + " issues of " + str(issue_total))

        if issue_total - iter <= chunk_size:
            chunk_size = issue_total - iter
        if issue_total < 1000:
            chunk_size = issue_total

        for issue in range(chunk_size):
            #Not all fields exist in all issues, and so some are parsed out up front
            if 'customfield_13051' in data_load['issues'][issue]['fields']:
                if data_load['issues'][issue]['fields']['customfield_myfield'] is not None:
                    application_name_str = str(data_load['issues'][issue]['fields'].get('customfield_myfield').get('value'))
                else:
                    application_name_str = 'No Application'
            else:
                application_name_str = 'No Application'

            if data_load['issues'][issue]['fields']['assignee'] is not None:
                assignee_str = data_load['issues'][issue]['fields'].get('assignee').get('displayName') 
            else:
                assignee_str = 'Unassigned' 

            if data_load['issues'][issue]['fields']['reporter'] is not None:
                reporter_str = data_load['issues'][issue]['fields'].get('reporter').get('displayName')
            else:
                reporter_str = 'Unassigned'

            file.write(f"{data_load['issues'][issue]['key']}" + "\t"
                       + str(data_load['issues'][issue]['fields']['summary']).replace("\t", " ") + "\t" 
                       + get_component_name(data_load['issues'][issue]['fields']['components']) + "\t"  
                       + get_resolution_date(data_load['issues'][issue]['fields']['resolutiondate']) + "\t"
                       + str(datetime.datetime.strptime(data_load['issues'][issue]['fields']['updated'], '%Y-%m-%dT%H:%M:%S.000+0000')) + "\t" 
                       + get_model_ddl(data_load['issues'][issue]['fields']['attachment']) + "\t" 
                       + get_issuelinks(data_load['issues'][issue]['fields']['issuelinks'])+ "\t"
                       + str(data_load['issues'][issue]['fields']['customfield_myfield']) + "\t"
                       + str(reporter_str) + "\t"
                       + str(assignee_str) + "\t"
                       + application_name_str + "\t"
                       + get_comments(data_load['issues'][issue]['fields']['comment']) + "\n")
     
    file.close() 

    host_str = config.get('mydb','hostname')
    user_str = config.get('mydb','username')
    password_str = config.get('mydb','password')
    database_str = config.get('mydb','db')

    user_str,password_str = common.get_credentials(user_str, password_str)    

    conn = psycopg2.connect(database=database_str,
                        user=user_str, password=password_str,
                        host=host_str, port='5432',
                        options="-c search_path=mydb"
    )    

    logger.info("Connected to " + host_str)

    conn.autocommit = True
    cursor = conn.cursor()

    logger.info("Writing to stg_jira_issue...")
    with open(txt_file_str, 'r', encoding='utf-8-sig') as f:
        cursor.copy_from(f, 'stg_jira_issue', sep='\t', null="", columns=('jira_key_code', 'jira_summary_desc', 'jira_component_desc', 'jira_update_date', 'jira_resolution_date', 'model_ddl_text', 'jira_issuelinks_json', 'jira_epic_key_code', 'jira_reporter_name', 'jira_assignee_name', 'cctrl_application_name', 'jira_comment_text'))
    
    conn.commit()

    logger.info("Jira issue data in stg")

def get_resolution_date(resolution_date):
    """
    Format the resolution date for a given jira ticket
    """
    if resolution_date is None:
        resolution_date = ''
    else:
        resolution_date = str(datetime.datetime.strptime(resolution_date, '%Y-%m-%dT%H:%M:%S.000+0000'))
    return resolution_date


def get_component_name(component_list):
    """
    Verifies presence of component in a Jira ticket and returns the value if it's available
    """
    json_object = json.loads(str(component_list).replace("'", "\""))
    if json_object:
        component = json_object[0]['name']
    else:
        component = 'None'
    # Return something that tells the user no results were found or,
    return component

def get_model_ddl(attachment_list):
    """
    Verifies presence of Generated DDL attachment, opens it, and pulls out DDL contents if the attachment is available
    """
    ddl = 'None'
    json_dump = json.dumps(attachment_list)
    json_object = json.loads(json_dump)
    if json_object:
        for attach in range(len(json_object)):
            if 'GeneratedDDL.txt' in json_object[attach]['filename']: 

                request = urllib.request.Request(json_object[attach]['content'])

                request.add_header("Authorization", "Bearer " + jira_password)
                result = urllib.request.urlopen(request)
                ddl = str(result.read()).replace("\n", " ").replace("\r", " ").replace("\t", " ")
            else:
                ddl = 'None' 
    else:
        ddl = 'None'
    return ddl

def get_issuelinks(issuelinks_list):
    """
    Iterates through all the tickets linked to a given jira ticket, formats them into their own json
    """
    issuelinks_json = '{"issuelinks": ['
    json_dump = json.dumps(issuelinks_list)
    json_object = json.loads(json_dump)
    if json_object:
        for link in range(len(json_object)):
            if 'inwardIssue' in json_object[link]:
                issuelinks_json = issuelinks_json + '{"key": "' + str(json_object[link]['inwardIssue']['key']) + '",' + '"direction": "inward", "relationship": "' + str(json_object[link]['type']['inward']) + '"},'
            if 'outwardIssue' in json_object[link]:
                issuelinks_json = issuelinks_json + '{"key": "' + str(json_object[link]['outwardIssue']['key']) + '",' + '"direction": "outward", "relationship": "' + str(json_object[link]['type']['outward']) + '"},'
        if len(issuelinks_json) > 0:
            issuelinks_json = issuelinks_json[:-1]
        
    issuelinks_json = issuelinks_json + ']}'
    return str(issuelinks_json).replace("\t", " ") 

def get_comments(comment_list):
    """
    Iterates through all the comments on a given jira ticket, formats them into their own json
    """
    comments_json = '{"comments": ['
    json_dump = json.dumps(comment_list)
    json_object = json.loads(json_dump)
    if json_object['comments'] and len(json_object['comments']) > 0:
        for comment in range(len(json_object['comments'])):
            if 'author' in json_object['comments'][comment]:
                comments_json = comments_json + '{"author": "' + str(json_object['comments'][comment]['author']['displayName']) + '", ' + '"createDate": "' + str(json_object['comments'][comment]['created']) + '", ' + '"body": " ' + str(json_object['comments'][comment]['body']) + ' "},'
 
        if len(comments_json) > 0:
            comments_json = comments_json[:-1]

    comments_json = comments_json + ']}'
    return str(comments_json).replace("\\" , "/").replace("\t", "//t").replace("\n", "//n").replace("\r", "//r")

if __name__ == "__main__":
    config = common.get_config('ymg_common.conf')
    logger = common.get_logger('extract_jira_data')
    logger.info("Extracting data from Jira API and writing to mydb database")

    parser = argparse.ArgumentParser()
    parser.add_argument("--jql_query", "-j", dest="jql_query", required=True, help="The JQL search criteria")
    args = parser.parse_args()

    jira_user = config.get('jira', 'jira_user')
    jira_password = config.get('jira', 'jira_token')

    jira_user,jira_password = common.get_credentials(jira_user, jira_password)

    host = 'https://jira.myjira.net'
    headers = JIRA.DEFAULT_OPTIONS["headers"].copy()
    headers["Authorization"] = f"Bearer {jira_password}"
    jira = JIRA(host,  options={"headers": headers})
    
    logger.info("Connected to Jira")
    
    get_jira_data(args.jql_query)
