import datetime
from py2neo import Graph

#setgraph database
graph = Graph("http://neo4j:Paran0id!@127.0.0.1:7474/db/data/")

def tweet_to_neo4j(data,graph):
    """give a mysql table, creates firm nodes"""
    tx = graph.cypher.begin()
    try:
        #if tweet user not yet in the system, create the user
        tx.append("""MERGE (n:User
            { user_name : {user_name},
                user_id : {user_id},
                hashtags : {hashtags},
                node_create_date : {creation_date} }
            )
            RETURN n""",
                  user_name= data[2],
                  user_id=data[1],
                  hashtags = data[4],
                  creation_date=datetime.datetime.now())

        #create the tweet
        tx.append("""CREATE (n:Tweet
            { tweet_id : {tweet_id},
                tweet_text : {tweet_text},
                node_create_date : {creation_date} }
            )
            RETURN n""",
                  tweet_id=data[0],
                  tweet_text=data[3],
                  creation_date=datetime.datetime.now())

        #create relationship between user and tweet
        tx.append("""MATCH (a:User)
                            WITH a
                            MATCH (b:Tweet)
                            WHERE a.user_id = {user_id} AND b.tweet_id = {tweet_id}
                            MERGE (a)-[r:Tweeted]->(b)
                            SET r.rel_create_date = {creation_date}
                            RETURN r""",
                          user_id=data[1],
                          tweet_id=data[0],
                          creation_date=datetime.datetime.now())

    finally:
        #commit the left overs
        tx.commit()
