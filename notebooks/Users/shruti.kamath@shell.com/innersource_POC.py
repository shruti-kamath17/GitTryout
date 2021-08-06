# Databricks notebook source
pip install PyGithub

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS innersource_poc_db;
# MAGIC create or replace table innersource_poc_db.teams_tmp as select * from innersource_poc_db.teams;
# MAGIC create or replace table innersource_poc_db.team_members_tmp as select * from innersource_poc_db.team_members;
# MAGIC create or replace table innersource_poc_db.team_repos_tmp as select * from innersource_poc_db.team_repos;
# MAGIC create or replace table innersource_poc_db.team_discussions_tmp as select * from innersource_poc_db.team_discussions;
# MAGIC create or replace table innersource_poc_db.repo_clones_tmp as select * from innersource_poc_db.repo_clones;
# MAGIC --create or replace table innersource_poc_db.repo_forks_tmp as select * from innersource_poc_db.repo_forks;
# MAGIC create or replace table innersource_poc_db.repo_views_traffic_tmp as select * from innersource_poc_db.repo_views_traffic;
# MAGIC create or replace table innersource_poc_db.repo_top_paths_tmp as select * from innersource_poc_db.repo_top_paths;
# MAGIC create or replace table innersource_poc_db.repo_top_referrers_tmp as select * from innersource_poc_db.repo_top_referrers;
# MAGIC create or replace table innersource_poc_db.repo_branches_tmp as select * from innersource_poc_db.repo_branches;
# MAGIC create or replace table innersource_poc_db.repo_events_tmp as select * from innersource_poc_db.repo_events;
# MAGIC create or replace table innersource_poc_db.repo_commits_tmp as select * from innersource_poc_db.repo_commits;
# MAGIC create or replace table innersource_poc_db.repo_contributor_stats_tmp as select * from innersource_poc_db.repo_contributor_stats;
# MAGIC create or replace table innersource_poc_db.issue_stats_tmp as select * from innersource_poc_db.issue_stats;
# MAGIC create or replace table innersource_poc_db.issue_labels_tmp as select * from innersource_poc_db.issue_labels;
# MAGIC create or replace table innersource_poc_db.issue_assignees_tmp as select * from innersource_poc_db.issue_assignees;
# MAGIC create or replace table innersource_poc_db.pulls_stats_tmp as select * from innersource_poc_db.pulls_stats;
# MAGIC 
# MAGIC --delete from innersource_poc_db.team_members_tmp where member_login in('narendrashastry12','dwilliamhouston');
# MAGIC --select * from innersource_poc_db.team_members where team_name='dataengineering-innersource';
# MAGIC --update innersource_poc_db.team_members_tmp set As_of ='2021-07-06'
# MAGIC 
# MAGIC --top 5 paths,top 5 referrers,top 5 events,top5 contributirs,issue per label,puul?

# COMMAND ----------

import base64
import pandas as pd
from github import Github
from pprint import pprint
import matplotlib.pyplot as plt
import datetime
from collections import Counter

token="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"
#tribes=["infrastructure-as-code","native-software-engineering","datascience-innersource","salesforce-trailblazers","dataengineering-innersource","Computational Science InnerSource"]
tribes=["dataengineering-innersource"]
org="sede-x"
now=datetime.datetime.now()

def get_org_teams(g):    
    #This function will return all the teams present in organization specified in the arg
    teams={}
    for team in g.get_organization(org).get_teams():
      if team.name in tribes:
        teams[team.id]=team.name
    return teams                                       
    
def get_team_details(g,teams_dict):
    #This function will return a list of team members details
    #dict of teams and its list of repo,
    #list of team invites details,
    #list of discussion in the team
    
    teams=[]
    team_members=[]
    teams_repos={}  
    team_discussions=[]
    for team in teams_dict.items():
      team_name=team[1]
      
      teams_ele=[team_name,now,1]
      teams.append(teams_ele)
      
      ##------Getting members for a given team-------##
      members=g.get_organization(org).get_team(team[0]).get_members()
      for member in members:
        team_member_ele=[team_name,member.id,member.login,member.name,now,1]
        #print(member.raw_data)
        team_members.append(team_member_ele)
        
      ##------Getting repos for a given team-------##
      repos=[]                                       
      for repo in g.get_organization(org).get_team(team[0]).get_repos():
        repos.append('sede-x/'+repo.name)                      
      teams_repos[team_name]=repos                   #This creates a dict with key as team name and value as list of repos of that team
      del repos
    
      ##------Getting discussions for a given team-------##
      discussions=g.get_organization(org).get_team(team[0]).get_discussions()
      for discussion in discussions:
        team_discussion_ele=[team_name,discussion.author.login,discussion.title,discussion.created_at,discussion.comments_count,now,1]
        team_discussions.append(team_discussion_ele)
      
      ##------Deleting existing entries for the team--------##
      query_team = 'delete FROM innersource_poc_db.teams where team_name="{}"'.format(team_name)
      spark.sql(query_team)
      
      query_team_members = 'delete FROM innersource_poc_db.team_members where team_name="{}"'.format(team_name)
      spark.sql(query_team_members)
      
      query_team_repos = 'delete FROM innersource_poc_db.team_repos where team_name="{}"'.format(team_name)
      spark.sql(query_team_repos)
      
      query_team_discussions = 'delete FROM innersource_poc_db.team_discussions where team_name="{}"'.format(team_name)
      spark.sql(query_team_discussions)
      
    return teams,team_members,teams_repos,team_discussions

def get_repo_details(g,teams_repos):
  #This function will return a list of repo clone details
  #list of repo fork details
  #list of repo view traffic
  #list of top paths
  #list of top referrers
  #list of reo branches
  #list of repo events
  #list of repo and total commits for that repo,
  #list of repo and its contributors
  #list of issue details
  #list of issue labels
  #list of issue assignees
  #list of pull stats
  
  repo_clones=[]
  repo_forks=[]
  repo_views_traffic=[]
  repo_top_paths=[]
  repo_top_referrers=[]
  repo_branches=[]
  repo_events=[]
  repo_commits=[]
  repo_contributor_stats=[]
  Issues_stats=[]
  Issue_labels=[]
  Issue_assignees=[]
  Issue_linked_PR=[]
  Pulls_stats=[]
  for tr in teams_repos.items():                       #This will iterate over every team
    #print (f"For team ={tr[0]}\n The list of repos are{tr[1]}\n")
    team_name=tr[0]
    
    repo_list=tr[1]
    for repo in repo_list:                             #This will iterate over the repos for that team
      repo_name=repo
      
     ##-------Can get below insights of clones,forks,traffic,top paths and top referrers only if you are a collaborator---------##
      if(g.get_repo(repo_name).has_in_collaborators('shruti-kamath17'))==True:
        
        clones=g.get_repo(repo_name).get_clones_traffic()
        repo_clones_ele=[tr[0],repo_name,clones['count'],clones['uniques'],now,1]
        repo_clones.append(repo_clones_ele)
                
        forks=g.get_repo(repo_name).get_forks()
        for fork in forks:
          repo_forks_ele=[tr[0],repo_name,fork.full_name,fork.owner,now,1]
        
        views_traffic=g.get_repo(repo_name).get_views_traffic()
        for view_traffic in views_traffic['views']:
          repo_views_traffic_ele=[tr[0],repo_name,view_traffic.count,view_traffic.uniques,view_traffic.timestamp,now,1]
          repo_views_traffic.append(repo_views_traffic_ele)
        
        top_paths=g.get_repo(repo_name).get_top_paths()
        for top_path in top_paths:
          repo_top_paths_ele=[tr[0],repo_name,top_path.title,top_path.path,top_path.count,top_path.uniques,now,1]
          repo_top_paths.append(repo_top_paths_ele)
          
        top_referrers=g.get_repo(repo_name).get_top_referrers()
        for top_referrer in top_referrers:
          repo_top_referrers_ele=[tr[0],repo_name,top_referrer.referrer,top_referrer.count,top_referrer.uniques,now,1]
          repo_top_referrers.append(repo_top_referrers_ele)
      
      ##-------Getting repo branch details---------##
      branches=g.get_repo(repo_name).get_branches()
      for branch in branches:
        repo_branches_ele=[tr[0],repo_name,branch.name,now,1]
        repo_branches.append(repo_branches_ele)
        
      ##-------Getting repo event details---------##
      events=g.get_repo(repo_name).get_events()
      for event in events:
        repo_events_ele=[tr[0],repo_name,event.type,event.actor.login,event.created_at,now,1]
        repo_events.append(repo_events_ele)
      
  
      ##-------Getting repo commit details---------##
      repo_commit_ele=[]
      commits = g.get_repo(repo_name).get_commits()        
      for commit in commits:
        if commit.committer!=None:
          committer=commit.committer.login
        else:committer=''   
        if (commit.url):
          commit_url=(commit.url).split("commits/",1)[1]
        else:commit_url=''  
        repo_commit_ele=[tr[0],repo_name,commit_url,committer,commit.last_modified,now,1]
        repo_commits.append(repo_commit_ele)
      
      
      ##-------Getting repo contributor stats ---------##
      contributors=g.get_repo(repo_name).get_contributors()
            
      repo_contributor_names=[]
      for contributor in contributors:
        if contributor.login!=None:
          repo_contributor_stats_ele=[tr[0],repo_name,contributor.login,now,1]
          repo_contributor_stats.append(repo_contributor_stats_ele)
          
      ##-------Getting pull details---------------##
      pull_requests=g.get_repo(repo_name).get_pulls(state='all')
      pull_req_nos=[] 
      for pull_request in pull_requests:
        pull_req_nos.append(pull_request.number)
        if pull_request.user!=None:
          pull_request_user=pull_request.user.login
        else: pull_request_user=''
        
        if pull_request.merged_by!=None:
          pull_request_mergedby=pull_request.merged_by.login
        else: pull_request_mergedby=''
        
        pull_ele=[tr[0],repo_name,pull_request.number,pull_request.state, pull_request.title, pull_request_user, pull_request_mergedby,now,1]
        Pulls_stats.append(pull_ele)
        
        ##-------Getting repo issues details---------------##
      issues=g.get_repo(repo_name).get_issues(state='all')
            
      for issue in issues:
        if issue.number not in pull_req_nos:                 #issues object will include pulls also, so we are excluding pulls here
          if issue.user==None:
            issue_user=''
          else: issue_user=issue.user.login
            
          if issue.closed_by==None:
            issue_closed_by=''
          else: issue_closed_by=issue.closed_by.login
            
          Issues_stats_ele=[tr[0],repo_name,issue.number,issue.state,issue.title,issue_user,issue_closed_by,now,1]  
          Issues_stats.append(Issues_stats_ele)
          
          labels=issue.labels
          for label in labels:
            if label.name!=None:
              issue_label_ele=[repo_name+str(issue.number),tr[0],repo_name,issue.number,label.name,now,1]
              Issue_labels.append(issue_label_ele)
          
          assignees=issue.assignees
          for assignee in assignees:
            if assignee.login!=None:
              issue_assignees_ele=[repo_name+str(issue.number),tr[0],repo_name,issue.number,assignee.login,now,1]
              Issue_assignees.append(issue_assignees_ele)
          
          if issue.pull_request!=None:
            pull_request_ele=[repo_name+str(issue.number),tr[0],repo_name,issue.number,now,1]
            pull_request_url=(issue.pull_request.html_url).split("pull/",1)[1]
            pull_request_ele.append(pull_request_url)
            Issue_linked_PR.append(pull_request_ele)

    
    
    query_repo_clones = 'delete FROM innersource_poc_db.repo_clones where team_name="{}"'.format(team_name)
    spark.sql(query_repo_clones)
    
   # query_repo_forks = 'delete FROM innersource_poc_db.repo_forks where team_name="{}"'.format(team_name)
   # spark.sql(repo_forks)
   
    query_repo_views_traffic = 'delete FROM innersource_poc_db.repo_views_traffic where team_name="{}"'.format(team_name)
    spark.sql(query_repo_views_traffic)
    
    query_repo_top_paths = 'delete FROM innersource_poc_db.repo_top_paths where team_name="{}"'.format(team_name)
    spark.sql(query_repo_top_paths)
    
    query_repo_top_referrers = 'delete FROM innersource_poc_db.repo_top_referrers where team_name="{}"'.format(team_name)
    spark.sql(query_repo_top_referrers)
    
    query_repo_branches = 'delete FROM innersource_poc_db.repo_branches where team_name="{}"'.format(team_name)
    spark.sql(query_repo_branches)
    
    query_repo_events = 'delete FROM innersource_poc_db.repo_events where team_name="{}"'.format(team_name)
    spark.sql(query_repo_events)
    
    query_repo_commits = 'delete FROM innersource_poc_db.repo_commits where team_name="{}"'.format(team_name)
    spark.sql(query_repo_commits)
    
    query_repo_contributor_stats = 'delete FROM innersource_poc_db.repo_contributor_stats where team_name="{}"'.format(team_name)
    spark.sql(query_repo_contributor_stats)
    
    query_issue_stats = 'delete FROM innersource_poc_db.issue_stats where team_name="{}"'.format(team_name)
    spark.sql(query_issue_stats)
    
    query_issues_labels = 'delete FROM innersource_poc_db.issue_labels where team_name="{}"'.format(team_name)
    spark.sql(query_issues_labels)
    
    query_issue_assignees = 'delete FROM innersource_poc_db.issue_assignees where team_name="{}"'.format(team_name)
    spark.sql(query_issue_assignees)
    
   # query_issue_linked_PR = 'delete FROM innersource_poc_db.issue_linked_PR where team_name="{}"'.format(team_name)
   # spark.sql(query_issue_linked_PR)
    
    query_Pulls_stats = 'delete FROM innersource_poc_db.Pulls_stats where team_name="{}"'.format(team_name)
    spark.sql(query_Pulls_stats)  
  return repo_clones,repo_forks,repo_views_traffic,repo_top_paths,repo_top_referrers,repo_branches,repo_events,repo_commits,repo_contributor_stats,Issues_stats,Issue_labels,Issue_assignees,Issue_linked_PR,Pulls_stats

    
def main():
  g = Github(token)
  teams_dict=get_org_teams(g)
  teams,team_members,team_repos,team_discussions=get_team_details(g,teams_dict) 
  repo_clones,repo_forks,repo_views_traffic,repo_top_paths,repo_top_referrers,repo_branches,repo_events,repo_commits,repo_contributor_stats,Issues_stats,Issue_labels,Issue_assignees,Issue_linked_PR,Pulls_stats=get_repo_details(g,team_repos)
  
  ###---------CREATING LIST OUT OF A DICTIONARY-------###
  team_repos_list=[]
  for tr in team_repos.items():                       #This will iterate over every team
    #print (f"For team ={tr[0]}\n The list of repos are{tr[1]}\n")
    for r in tr[1]:
      team_repo_ele=[tr[0],r,now,1]
      team_repos_list.append(team_repo_ele)
   

  ###--------CREATING DATAFRAMES---------------------###
  Teams_DF=pd.DataFrame(teams,columns=['Team_name','As_of','Active_Flag'])
  Team_Members_DF=pd.DataFrame(team_members,columns=['Team_name','Member_ID','Member_Login','Member_name','As_of','Active_Flag'])
  Team_Repos_DF=pd.DataFrame(team_repos_list,columns=['Team_name','Repo_name','As_of','Active_Flag'])
  Team_Discussions_DF=pd.DataFrame(team_discussions,columns=['Team_name','Author','Title','Created_At','Total_Comments','As_of','Active_Flag'])
  Repo_Clones_DF=pd.DataFrame(repo_clones,columns=['Team_name','Repo_name','Tot_Counts','Unique_Counts','As_of','Active_Flag'])
  Repo_Forks_DF=pd.DataFrame(repo_forks,columns=['Team_name','Repo_name','Fork_full_name','Owner','As_of','Active_Flag'])
  Repo_Views_Traffic_DF=pd.DataFrame(repo_views_traffic,columns=['Team_name','Repo_name','Tot_Counts','Unique_Counts','View_timestamp','As_of','Active_Flag'])
  Repo_Top_Paths_DF=pd.DataFrame(repo_top_paths,columns=['Team_name','Repo_name','Title','Path','Tot_Counts','Unique_Counts','As_of','Active_Flag'])
  Repo_Top_Referrers_DF=pd.DataFrame(repo_top_referrers,columns=['Team_name','Repo_name','Referrer','Tot_Counts','Unique_Counts','As_of','Active_Flag'])
  Repo_Branches_DF=pd.DataFrame(repo_branches,columns=['Team_name','Repo_name','Branch_name','As_of','Active_Flag'])
  Repo_Events_DF=pd.DataFrame(repo_events,columns=['Team_name','Repo_name','Type','Actor','Created_At','As_of','Active_Flag'])
  Repo_Commits_DF=pd.DataFrame(repo_commits,columns=['Team_name','Repo_name','Commit_ID','Committer','Modified_at','As_of','Active_Flag'])
  Repo_Contributor_Stats_DF=pd.DataFrame(repo_contributor_stats,columns=['Team_name','Repo_name','Contributor_name','As_of','Active_Flag'])
  Issue_Stats_DF=pd.DataFrame(Issues_stats,columns=['Team_name','Repo_name','Issue_Number','Status','Title','Created_By','Closed_By','As_of','Active_Flag'])
  Issue_Labels_DF=pd.DataFrame(Issue_labels,columns=['RepoName_IssueNum','Team_name','Repo_name','Issue_number','Label_name','As_of','Active_Flag'])
  Issue_Assignees_DF=pd.DataFrame(Issue_assignees,columns=['RepoName_IssueNum','Team_name','Repo_name','Issue_number','Assignee_name','As_of','Active_Flag'])
  Issue_Linked_PR_DF=pd.DataFrame(Issue_linked_PR,columns=['RepoName_IssueNum','Team_name','Repo_name','Issue_number','As_of','Active_Flag','Pull_number'])
  Pulls_Stats_DF=pd.DataFrame(Pulls_stats,columns=['Team_name','Repo_name','Pull_number','Status','Title','Created_By','Closed_By','As_of','Active_Flag'])  
  
 
  
   ###--------CREATING SQL Tables----------------###
  if Teams_DF.empty==False:
    Teams_spark_df = spark.createDataFrame(Teams_DF)
    Teams_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Teams")
  
  if Team_Members_DF.empty==False:
    Team_Members_spark_df = spark.createDataFrame(Team_Members_DF)
    Team_Members_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Team_Members")
  
  if Team_Repos_DF.empty==False:
    Team_Repos_spark_df = spark.createDataFrame(Team_Repos_DF)
    Team_Repos_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Team_Repos")
 
  if Team_Discussions_DF.empty==False:
    Teams_Discussions_spark_df = spark.createDataFrame(Team_Discussions_DF)
    Teams_Discussions_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Team_Discussions")
  
  if Repo_Clones_DF.empty==False:
    Repo_Clones_spark_df = spark.createDataFrame(Repo_Clones_DF)
    Repo_Clones_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Clones")
  
  if Repo_Forks_DF.empty==False:
    Repo_Forks_spark_df = spark.createDataFrame(Repo_Forks_DF)
    Repo_Forks_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Forks")
  
  if Repo_Views_Traffic_DF.empty==False:
    Repo_Views_Traffic_spark_df = spark.createDataFrame(Repo_Views_Traffic_DF)
    Repo_Views_Traffic_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Views_Traffic")
  
  if Repo_Top_Paths_DF.empty==False:
    Repo_Top_Paths_spark_df = spark.createDataFrame(Repo_Top_Paths_DF)
    Repo_Top_Paths_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Top_Paths")
  
  if Repo_Top_Referrers_DF.empty==False:
    Repo_Top_Referrers_spark_df = spark.createDataFrame(Repo_Top_Referrers_DF)
    Repo_Top_Referrers_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Top_Referrers")
  
  if Repo_Branches_DF.empty==False:
    Repo_Branches_spark_df = spark.createDataFrame(Repo_Branches_DF)
    Repo_Branches_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Branches")
  
  if Repo_Events_DF.empty==False:
    Repo_Events_spark_df = spark.createDataFrame(Repo_Events_DF)
    Repo_Events_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Events")
  
  if Repo_Commits_DF.empty==False:
    Repo_Commits_spark_df = spark.createDataFrame(Repo_Commits_DF)
    Repo_Commits_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Commits")
    
  
  if Repo_Contributor_Stats_DF.empty==False:
    Repo_Contributor_Stats_spark_df = spark.createDataFrame(Repo_Contributor_Stats_DF)
    Repo_Contributor_Stats_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Repo_Contributor_Stats")
 
  if Issue_Stats_DF.empty==False:
    Issue_Stats_spark_df = spark.createDataFrame(Issue_Stats_DF)
    Issue_Stats_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Issue_Stats")
  
  if Issue_Labels_DF.empty==False:
    Issue_Labels_spark_df = spark.createDataFrame(Issue_Labels_DF)
    Issue_Labels_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Issue_Labels")
  
  if Issue_Assignees_DF.empty==False:
    Issue_Assignees_spark_df = spark.createDataFrame(Issue_Assignees_DF)
    Issue_Assignees_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Issue_Assignees")
  
  if Issue_Linked_PR_DF.empty==False:
    Issue_linked_PR_spark_df = spark.createDataFrame(Issue_Linked_PR_DF)
    Issue_linked_PR_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Issue_linked_PR")
  
  if Pulls_Stats_DF.empty==False:
    Pulls_stats_spark_df = spark.createDataFrame(Pulls_Stats_DF)
    Pulls_stats_spark_df.write.mode("append").saveAsTable("innersource_poc_db.Pulls_stats")
  
if __name__=="__main__":  
  main()    

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into innersource_poc_db.teams AS TARGET
# MAGIC USING innersource_poc_db.teams_tmp AS SOURCE 
# MAGIC ON TARGET.team_name=SOURCE.team_name
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,As_of,Active_Flag) 
# MAGIC values (Team_name,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.team_members AS TARGET
# MAGIC USING innersource_poc_db.team_members_tmp AS SOURCE 
# MAGIC ON TARGET.team_name=SOURCE.team_name and TARGET.member_id=SOURCE.member_id
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Member_ID,Member_Login,Member_name,As_of,Active_Flag) 
# MAGIC values (Team_name,Member_ID,Member_Login,Member_name,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.team_repos AS TARGET
# MAGIC USING innersource_poc_db.team_repos_tmp AS SOURCE 
# MAGIC ON TARGET.team_name=SOURCE.team_name and TARGET.repo_name=SOURCE.repo_name
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.team_discussions AS TARGET
# MAGIC USING innersource_poc_db.team_discussions_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.author=source.author and target.title=source.title and target.Total_Comments=source.Total_Comments
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Author,Title,Created_At,Total_Comments,As_of,Active_Flag) 
# MAGIC values (Team_name,Author,Title,Created_At,Total_Comments,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_clones AS TARGET
# MAGIC USING innersource_poc_db.repo_clones_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.Repo_name=source.Repo_name and target.Tot_Counts=source.Tot_Counts and target.Unique_Counts=source.Unique_Counts
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,Tot_Counts,Unique_counts,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,Tot_Counts,Unique_counts,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_views_traffic AS TARGET
# MAGIC USING innersource_poc_db.repo_views_traffic_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.Repo_name=source.Repo_name and target.Tot_Counts=source.Tot_Counts and target.Unique_Counts=source.Unique_Counts and target.view_timestamp=source.view_timestamp
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,Tot_Counts,Unique_counts,view_timestamp,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,Tot_Counts,Unique_counts,view_timestamp,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_top_paths AS TARGET
# MAGIC USING innersource_poc_db.repo_top_paths_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.Repo_name=source.Repo_name and target.path=source.path and target.title=source.title and target.Tot_Counts=source.Tot_Counts and target.Unique_Counts=source.Unique_Counts 
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,title,path,Tot_Counts,Unique_counts,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,title,path,Tot_Counts,Unique_counts,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_top_referrers AS TARGET
# MAGIC USING innersource_poc_db.repo_top_referrers_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.Repo_name=source.Repo_name and target.referrer=source.referrer and target.Tot_Counts=source.Tot_Counts and target.Unique_Counts=source.Unique_Counts 
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,referrer,Tot_Counts,Unique_counts,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,referrer,Tot_Counts,Unique_counts,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_branches AS TARGET
# MAGIC USING innersource_poc_db.repo_branches_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.branch_name=source.branch_name
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,branch_name,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,branch_name,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_events AS TARGET
# MAGIC USING innersource_poc_db.repo_events_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.type=source.type and target.actor=source.actor and target.created_at=source.created_at
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,type,actor,created_at,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,type,actor,created_at,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_commits AS TARGET
# MAGIC USING innersource_poc_db.repo_commits_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.Commit_ID=source.commit_id and target.Committer=source.committer and target.Modified_at=source.Modified_at
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,Commit_ID,Committer,Modified_at,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,Commit_ID,Committer,Modified_at,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.repo_contributor_stats AS TARGET
# MAGIC USING innersource_poc_db.repo_contributor_stats_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.contributor_name=source.Contributor_name
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,contributor_name,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,contributor_name,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.issue_stats AS TARGET
# MAGIC USING innersource_poc_db.issue_stats_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.Issue_Number=source.Issue_Number and target.Status=source.Status and target.Title=source.title and target.Created_By=source.Created_By and target.Closed_By=source.closed_by
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,Issue_Number,Status,Title,Created_By,Closed_By,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,Issue_Number,Status,Title,Created_By,Closed_By,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.issue_labels AS TARGET
# MAGIC USING innersource_poc_db.issue_labels_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.Issue_number=source.Issue_number and target.Label_name=source.Label_name
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(RepoName_IssueNum,Team_name,Repo_name,Issue_number,Label_name,As_of,Active_Flag) 
# MAGIC values (RepoName_IssueNum,Team_name,Repo_name,Issue_number,Label_name,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.issue_assignees AS TARGET
# MAGIC USING innersource_poc_db.issue_assignees_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.Issue_number=source.Issue_number and target.Assignee_name=source.Assignee_name
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(RepoName_IssueNum,Team_name,Repo_name,issue_number,assignee_name,As_of,Active_Flag) 
# MAGIC values (RepoName_IssueNum,Team_name,Repo_name,issue_number,assignee_name,As_of,0);
# MAGIC 
# MAGIC MERGE into innersource_poc_db.pulls_stats AS TARGET
# MAGIC USING innersource_poc_db.pulls_stats_tmp AS SOURCE 
# MAGIC ON TARGET.Team_name=Source.team_name and target.repo_name=source.repo_name and target.Pull_number=source.Pull_number and target.Status=source.status and target.Title=source.Title and target.Created_By=source.Created_By and target.Closed_By=source.Closed_By
# MAGIC WHEN MATCHED 
# MAGIC THEN UPDATE set target.as_of=source.as_of
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT(Team_name,Repo_name,Pull_number,Status,Title,Created_By,Closed_By,As_of,Active_Flag) 
# MAGIC values (Team_name,Repo_name,Pull_number,Status,Title,Created_By,Closed_By,As_of,0);

# COMMAND ----------

# MAGIC %sql
# MAGIC use innersource_poc_db;
# MAGIC create or replace table cte_members_graph as select substring(as_of,0,10)as_of,count(member_id)cnt from innersource_poc_db.team_members group by as_of;
# MAGIC create or replace table cte_discussion_graph as select substring(created_at,0,10)as_of,count(title)cnt from innersource_poc_db.team_discussions group by created_at;
# MAGIC create or replace table cte_repo_graph as select substring(as_of,0,10)as_of,count(repo_name)cnt from innersource_poc_db.team_repos group by as_of;
# MAGIC 
# MAGIC create or replace table cte_traffic_graph
# MAGIC select distinct traffic.view_timestamp as as_of,
# MAGIC ifnull(databricks_developer_guide.unique_counts,0) as databricks_developer_guide,
# MAGIC ifnull(data_and_analytics.unique_counts,0) as data_and_analytics,
# MAGIC ifnull(azure_data_factory_developer_guide.unique_counts,0) as azure_data_factory_developer_guide,
# MAGIC ifnull(azure_ci_cd_pipelines.unique_counts,0) as azure_ci_cd_pipelines,
# MAGIC ifnull(ida_document_store.unique_counts,0) as ida_document_store,
# MAGIC ifnull(ida_template_repo.unique_counts,0) as ida_template_repo,
# MAGIC ifnull(shell_innersource_databricks_ci_cd.unique_counts,0) as shell_innersource_databricks_ci_cd
# MAGIC from repo_views_traffic traffic
# MAGIC left join(select unique_counts,view_timestamp from repo_views_traffic where repo_name='sede-x/databricks-developer-guide')databricks_developer_guide on databricks_developer_guide.view_timestamp=traffic.view_timestamp
# MAGIC left join(select unique_counts,view_timestamp from repo_views_traffic where repo_name='sede-x/data-and-analytics')data_and_analytics on data_and_analytics.view_timestamp=traffic.view_timestamp
# MAGIC left join(select unique_counts,view_timestamp from repo_views_traffic where repo_name='sede-x/azure-data-factory-developer-guide')azure_data_factory_developer_guide on azure_data_factory_developer_guide.view_timestamp=traffic.view_timestamp
# MAGIC left join(select unique_counts,view_timestamp from repo_views_traffic where repo_name='sede-x/azure-ci-cd-pipelines')azure_ci_cd_pipelines on azure_ci_cd_pipelines.view_timestamp=traffic.view_timestamp
# MAGIC left join(select unique_counts,view_timestamp from repo_views_traffic where repo_name='sede-x/IDA-document-store')ida_document_store on ida_document_store.view_timestamp=traffic.view_timestamp
# MAGIC left join(select unique_counts,view_timestamp from repo_views_traffic where repo_name='sede-x/ida-template-repo')ida_template_repo on ida_template_repo.view_timestamp=traffic.view_timestamp
# MAGIC left join(select unique_counts,view_timestamp from repo_views_traffic where repo_name='sede-x/shell-innersource-databricks-ci-cd')shell_innersource_databricks_ci_cd on shell_innersource_databricks_ci_cd.view_timestamp=traffic.view_timestamp
# MAGIC order by 1;
# MAGIC 
# MAGIC create or replace table cte_repo_statistics as
# MAGIC select distinct substring(repo.repo_name,8,length(repo.repo_name)) as Repo_name ,
# MAGIC ifnull(clones.unique_counts,0) as Clones,
# MAGIC ifnull(issue.issue_count,0) as Issues ,
# MAGIC ifnull(branch.branch_count,0) as Branches,
# MAGIC ifnull(contributor.contributor_count,0) as Contributors, 
# MAGIC ifnull(views.view_count,0) as Views 
# MAGIC from innersource_poc_db.team_repos repo 
# MAGIC left join (select unique_counts,repo_name from innersource_poc_db.repo_clones where active_flag=1)clones on repo.repo_name=clones.repo_name 
# MAGIC left join (select count(1)issue_count,repo_name from innersource_poc_db.issue_stats where active_flag=1 group by repo_name)issue on issue.repo_name=repo.repo_name 
# MAGIC left join (select count(1)branch_count,repo_name from innersource_poc_db.repo_branches where active_flag=1 group by repo_name)branch on branch.repo_name=repo.repo_name 
# MAGIC left join (select count(1)event_count,repo_name from innersource_poc_db.repo_events where active_flag=1 group by repo_name)event on event.repo_name=repo.repo_name 
# MAGIC left join (select count(1)commit_count,repo_name from innersource_poc_db.repo_commits where active_flag=1 group by repo_name)commits on commits.repo_name=repo.repo_name 
# MAGIC left join (select count(1)pull_count,repo_name from innersource_poc_db.repo_commits where active_flag=1 group by repo_name)pull on pull.repo_name=repo.repo_name left join (select count(1)contributor_count,repo_name from innersource_poc_db.repo_contributor_stats where active_flag=1 group by repo_name)contributor on contributor.repo_name=repo.repo_name 
# MAGIC left join (select sum(unique_counts)view_count,repo_name from innersource_poc_db.repo_views_traffic where active_flag=1 group by repo_name)views on views.repo_name=repo.repo_name 
# MAGIC where repo.active_flag=1

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt

Q_Member_count_over_time = spark.sql('select as_of,sum(cnt) over(order by as_of) Member_Count from innersource_poc_db.cte_members_graph').toPandas()
Q_Member_count_over_time.plot(kind="line",x="as_of",y="Member_Count",title="Member count for Data-engineering-innersource tribe over time",color='blue', marker='o',figsize=(12,5))
#Q_Repo_per_team.plot.bar(x='as_of',y='Member_Count',rot=70, title="Members for teams",labels=Q_Repo_per_team['Member_Count'])
#plt.minorticks_on()
plt.xlabel("As Of")
plt.ylabel("Counts")
plt.xticks(rotation=0)
#xlocs, xlabs = plt.xticks()
#for i, v in enumerate(Q_Repo_per_team['Member_Count']):
#    plt.text(xlocs[i]-0.05 , v-2, str(v))
plt.grid(True)
plt.show()

#Q_contri_stats=spark.sql('select repo_name,contributor_name from innersource_poc_db.cte').toPandas()

Q_Discussion_count_over_time = spark.sql('select as_of,sum(cnt) over(order by as_of) Discussion_Count from innersource_poc_db.cte_discussion_graph').toPandas()
Q_Discussion_count_over_time.plot(kind="line",x="as_of",y="Discussion_Count",title="Discussion count for Data-engineering-innersource tribe over time",color='green', marker='o',figsize=(12,5))
plt.xlabel("As Of")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.grid(True)
plt.show()

Q_Repo_count_over_time = spark.sql('select as_of,sum(cnt) over(order by as_of) Repo_Count from innersource_poc_db.cte_repo_graph').toPandas()
Q_Repo_count_over_time.plot(kind="line",x="as_of",y="Repo_Count",title="Repository count for Data-engineering-innersource tribe over time",color='orange', marker='o',figsize=(12,5))
plt.xlabel("As Of")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.grid(True)
plt.show()

Q_Repo_traffic_over_time = spark.sql('select as_of,databricks_developer_guide,data_and_analytics,azure_data_factory_developer_guide,azure_ci_cd_pipelines,ida_document_store,ida_template_repo,shell_innersource_databricks_ci_cd from innersource_poc_db.cte_traffic_graph').toPandas()
f = plt.figure()
f.set_figwidth(18)
f.set_figheight(8)
plt.plot('as_of','databricks_developer_guide', data=Q_Repo_traffic_over_time, marker='o', color='orange',label="databricks-developer-guide")
plt.plot('as_of','data_and_analytics', data=Q_Repo_traffic_over_time, marker='o', color='olive',label='data-and-analytics')
plt.plot('as_of','azure_data_factory_developer_guide', data=Q_Repo_traffic_over_time, marker='o', color='red',label='azure-data-factory-developer-guide')
plt.plot('as_of','azure_ci_cd_pipelines', data=Q_Repo_traffic_over_time, marker='o', color='black',label="azure_ci_cd_pipelines")
plt.plot('as_of','ida_document_store', data=Q_Repo_traffic_over_time, marker='o', color='green', label="ida-document-store")
plt.plot('as_of','ida_template_repo', data=Q_Repo_traffic_over_time, marker='o', color='purple', label="ida-template-repo")
plt.plot('as_of','shell_innersource_databricks_ci_cd', data=Q_Repo_traffic_over_time, marker='o', color='blue', label="shell-innersource-databricks-ci-cd")
plt.title("Repositories Traffic for Data-engineering-innersource tribe")
plt.xlabel("As Of")
plt.ylabel("Unique Views")
plt.grid(True)
plt.legend()
plt.show()


Q_Repo_Stats=spark.sql('select Repo_name,Clones,Issues,Branches,Contributors,Views from innersource_poc_db.cte_repo_statistics').toPandas()
Q_Repo_Stats.plot(x='Repo_name',kind='bar',stacked=True,title='Repository Statistics for Data-engineering-innersource tribe',color=[ 'black',  'purple','grey','pink','orange'],width=0.5,figsize=(22,8))
plt.xlabel("Repositories")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.show()

Q_Discussion_comments=spark.sql('select title,total_comments from innersource_poc_db.team_discussions where active_flag=1 and total_comments>0').toPandas()
Q_Discussion_comments.plot(x='title',kind='barh',stacked=True,title='Popular Discussions in Data-engineering-innersource tribe',color='Orange',width=0.5,figsize=(10,8))
plt.xlabel("Title")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.show()

Q_Popular_Paths=spark.sql('select path,unique_counts from innersource_poc_db.repo_top_paths where active_flag=1 and path not in (select concat("/",repo_name) from innersource_poc_db.team_repos) order by unique_counts desc limit 5').toPandas()
Q_Popular_Paths.plot(x='path',kind='barh',title='Popular Pages in Data-engineering-innersource tribe',color='Green',width=0.5,figsize=(10,8))
plt.ylabel("Pages")
plt.xlabel("Counts")
plt.xticks(rotation=0)
plt.show()

Q_Top_Committers=spark.sql('select count(1)counts,committer from innersource_poc_db.repo_commits where committer!="" and committer!="web-flow" and active_flag=1 group by committer order by 1 desc limit 5').toPandas()
Q_Top_Committers.plot(x='committer',kind='bar',title='Top 5 Committers in Data-engineering-innersource tribe',color='Blue',width=0.5,figsize=(10,8))
plt.xlabel("Committers")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.show()

Q_Top_Pulls_By=spark.sql('select count(1)counts,created_by from innersource_poc_db.pulls_stats where status="closed" and active_flag=1 group by created_by order by 1 desc limit 5').toPandas()
Q_Top_Pulls_By.plot(x='created_by',kind='bar',title='Top 5 Pull contributors in Data-engineering-innersource tribe',color='Orange',width=0.5,figsize=(10,8))
plt.xlabel("Pulls By")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.show()

Q_Top_Events=spark.sql('select count(1)counts,type from innersource_poc_db.repo_events where active_flag=1 group by type order by 1 desc limit 5').toPandas()
Q_Top_Events.plot(x='type',kind='bar',title='Popular Events in Data-engineering-innersource tribe',color='Purple',width=0.5,figsize=(10,8))
plt.xlabel("Event Type")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.show()

Q_Top_Issue_Raisers=spark.sql('select count(1)counts,created_by from innersource_poc_db.issue_stats where active_flag=1  group by created_by order by 1 desc limit 5').toPandas()
Q_Top_Issue_Raisers.plot(x='created_by',kind='bar',title='Top 5 Issue Raisers in Data-engineering-innersource tribe',color='Pink',width=0.5)
plt.xlabel("Raised By")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.show()

Q_Top_Issue_Closed_By=spark.sql('select count(1)counts,closed_by from innersource_poc_db.issue_stats where active_flag=1 and status="closed" group by closed_by order by 1 desc limit 5').toPandas()
Q_Top_Issue_Closed_By.plot(x='closed_by',kind='bar',title='Top 5 Issue Closers in Data-engineering-innersource tribe',color='grey',width=0.5)
plt.xlabel("Closed By")
plt.ylabel("Counts")
plt.xticks(rotation=0)
plt.show()

# COMMAND ----------

