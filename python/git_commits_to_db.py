import datetime
import sys
import argparse
import git
import getpass
import psycopg2
import time
import os
import subprocess
import urllib.parse
from lib import common

def get_commits(repo_url, branch_name, time_since):
    """
    Write commit information from the given repo URL and branch for the given time frame eg "2.days.ago"
    """

    dir_str = config.get('main', 'temp.directory')
    
    file_str = dir_str + '/git_commits.txt'
    logger.info(file_str)
    file = open(file_str,'w+', newline='', encoding='utf-8')
    
    logger.info("Connecting to " + repo_url)

    git_user = config.get('git', 'git_user')
    git_password = config.get('git', 'git_token')

    git_user, git_password = common.get_credentials(git_user, git_password)

    parsed_repo_url = urllib.parse.urlparse(repo_url)
    path_segments = parsed_repo_url.path.split('/')
    repo_name = path_segments[-1].replace('.git', '')

    local_dir = dir_str +"/"+ repo_name

    git_ssh_identity_file = os.path.expanduser('~/.ssh/id_rsa')
    git_ssh_cmd = 'ssh -i %s' % git_ssh_identity_file

    repo_token_url = 'https://'+ git_user + ':' + git_password + '@' +  repo_url.replace('https://', '')

    if os.path.isdir(local_dir):
        logger.info("Pulling from origin for local repo " + repo_name)
        repo = git.Repo(local_dir)
        subprocess.call(['git', 'pull', repo_token_url], cwd=local_dir)
    else:
        logger.info("Cloning " + repo_url)
        subprocess.call(['git', 'clone', repo_token_url], cwd=dir_str)
        repo = git.Repo(local_dir)

    logger.info("Getting commits for branch " + branch_name)
    commits = list(repo.iter_commits(branch_name, since=time_since))

    for commit in commits:                
        file.write(f"{commit.hexsha}" +"\t"+ repo_url +"\t"+ branch_name + "\t" + commit.author.name +"\t"+ time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(commit.committed_date)) +"\t"+ get_items(commit.stats.files.items) +"\t"+ get_message(commit.message) + "\n")
    file.close()

    host_str = config.get('mydb', 'hostname')
    user_str = config.get('mydb', 'username')
    password_str = config.get('mydb', 'password')
    database_str = config.get('mydb', 'db')

    user_str, password_str = common.get_credentials(user_str, password_str)    

    conn = psycopg2.connect(database=database_str,
                        user=user_str, password=password_str, 
                        host=host_str, port='5432',
                        options="-c search_path=mydb"
    )

    logger.info("Connected to data_platform_controls")
    conn.autocommit = True
    cursor = conn.cursor()

    logger.info("Writing to table...")
    with open(file_str, 'r') as f:
        logger.info("Writing to table...")
        cursor.copy_from(f, 'stg_git_commit', sep='\t', null="", columns=('git_hexsha', 'git_repo_url', 'git_branch_name', 'git_author_name', 'git_commit_date', 'git_item_name_list', 'git_message_desc'))
    
    conn.commit()

    logger.info("Git commit data in stg")

    logger.info(f"rm -r" + local_dir)
    os.system(f"rm -r " + local_dir)

def get_message(commit_message):
    """
    Take the message for a given commit and formats it to one line
    """
    formatted_message = ''
    for line in commit_message.split("\n"):
        formatted_message += " " + line.replace("\r", " ").replace("\t", " ")

    return formatted_message

def get_items(commit_items):
    """
    Takes item (file) names in a given commit and puts them into an array format
    """
    formatted_items = "{"
    for item in commit_items():
        formatted_items += item[0] + ", "
    
    if formatted_items == "{":
        formatted_items += "}"
    else:    
        formatted_items = formatted_items[:-2]
        formatted_items += "}"
    
    return formatted_items


if __name__ == "__main__":
    config = common.get_config('ymg_common.conf')
    logger = common.get_logger('extract_git_data')
    logger.info("Extracting data from Git API and writing to data_platform_controls database")

    parser = argparse.ArgumentParser()
    parser.add_argument("--repo_url", "-r", dest="repo_url", required=True, help="The git repo URL")
    parser.add_argument("--branch", "-b", dest="branch_name", required=True, help="The git branch")
    parser.add_argument("--time_since", "-t", dest="time_since", default="2.years.ago", required=True, help="Time period length to pull down eg 14.days.ago")
    args = parser.parse_args()

    get_commits(args.repo_url, args.branch_name, args.time_since)
