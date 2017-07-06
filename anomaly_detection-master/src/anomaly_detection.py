import sys
import json
from collections import deque
import numpy as ny


#the function to find the friends list within Dth degree network
def find_friends(Graph,start,num):
    q1=deque() #q1 is the queue to be emptied
    q2=deque()  # q2 is the queue to be filled
    q1.append(start)
    result=[]
    visited=[]
    #num = min(len(Graph),num)
    for current_level in range(1,num):
        while q1:
            u=q1.pop()#remove the vertex from queueTobeEmptied and call it as u
            for v in Graph[u]:
                if v not in visited:
                    visited.append(v)
                    q2.append(v)    
                    result.append(v)                       
        q1,q2 = q2,q1        #swap the queues now for next iteration of for loop

    
    return visited

# the function to compute mean of last T purchases within D-th degree social network
def compute_mean(purchase_history,find_friends, T):
    #friends_purchase=[]
    amount_list=[]

    for i, list in enumerate(purchase_history):
        if len(purchase_history)==1:
            print ("no enough purchase history")
            return
        if len(purchase_history) < T:
            if list[0] in find_friends:                 
                amount_list.append(list[2])
        if len(purchase_history) >= T:
            if list[0] in find_friends and len(amount_list)< T+1:                   
                amount_list.append(list[2])
   
    result = (amount_list,round(ny.mean(amount_list),2))

    return result
# the function to compute standard deviation                        
def compute_sd(mean,amount_list):
    sum_sq=0
    for amount in amount_list:
        diff= amount - mean
        sq = diff**2
        sum_sq += sq
    result = round((sum_sq/len(amount_list))**(1/2),2)
    return result
# the function to check anomaly 
def check_anomaly(event,mean, sd):
    if float(event['amount']) > mean + 3*sd:
        event.update({'mean': format(mean,'.2f')})
        event.update({'sd': format(sd,'.2f')})                           
    return event    
            

def printUsage():
    print ("Usage: python average_degree.py <input_file1> <input_file2> <output_file>")  # output_file here is always flagged_purchase.json
           
# the function to read batch_log file to generate initial social graph and purchase history, then it reads stream_log file to find anomaly and flag the event in output file
# the social graph and purchase history is also updated. 
def generateGraph(inputTweets,inputTweets1):
    social_graph = {}
    purchase_list =[]
    
    
    for t in inputTweets:
        t = json.loads(t)
        if "D" in t and "T" in t:
            D = int(t["D"]) 
            T = int(t["T"])
        if "event_type" in t:
        # keep record of the purchase history and store it as a list of tuples.
              
            if t["event_type"]=="purchase":   
                purchase_list.append((t["id"],t["timestamp"],float(t["amount"])))
        # add to social graph with event_type befriend
                  
            if t["event_type"]=="befriend":
                if t["id1"] in social_graph:
                    social_graph[t["id1"]].append(t["id2"])
                else:
                    social_graph[t["id1"]] = [t["id2"]]
                if t["id2"] in social_graph:
                    social_graph[t["id2"]].append(t["id1"])
                else:
                    social_graph[t["id2"]]= [t["id1"]]
                                             
        # remove from social graph with event_tpye unfriend
            if t["event_type"]=="unfriend":
                if t["id1"] in social_graph and t["id2"] in social_graph:
                    if t["id1"] in social_graph[t["id2"]]:
                        social_graph[t["id2"]].remove(t["id1"])
                    if t["id2"] in social_graph[t["id1"]]:
                        social_graph[t["id1"]].remove(t["id2"])
               
#begin reading stream_log.json file
    for t in inputTweets1:
        t = json.loads(t)
        
        
        
        # keep record of the purchase history and store it as a list of tuples.
        if t["event_type"]=="purchase":
                                            
            mean = compute_mean(purchase_list,find_friends(social_graph,t['id'],int(D)),int(T))[1]
            amount_list = compute_mean(purchase_list,find_friends(social_graph,t['id'],int(D)),int(T))[0]
            sd = compute_sd(mean, amount_list)
            print ("sd is :", sd)
            detection = check_anomaly(t,mean,sd)
            f = open('flagged_purchases.json', 'w') # open for 'w'riting
            f.write(str(detection))
            purchase_list.append((t["id"],t["timestamp"],t["amount"]))                   
        
        # add to social graph with event_type befriend
                  
            if t["event_type"]=="befriend":
                if t["id1"] in social_graph:
                    social_graph[t["id1"]].append(t["id2"])
                else:
                    social_graph[t["id1"]] = [t["id2"]]
                if t["id2"] in social_graph:
                    social_graph[t["id2"]].append(t["id1"])
                else:
                    social_graph[t["id2"]]= [t["id1"]]
                                             
        # remove from social graph with event_tpye unfriend
            if t["event_type"]=="unfriend":
                if t["id1"] in social_graph and t["id2"] in social_graph:
                    if t["id1"] in social_graph[t["id2"]]:
                        social_graph[t["id2"]].remove(t["id1"])
                    if t["id2"] in social_graph[t["id1"]]:
                        social_graph[t["id1"]].remove(t["id2"])                                      

def main(argv):
    
    if len(argv) < 2 :
        printUsage()
        sys.exit(2)
    
    inputTweetsFile = argv[0]
    inputTweetsFile1= argv[1]
    outputTweetsFile = argv[2]
    inputTweets = []
    inputTweets1 = []

    #Read Input File and save all input tweets in memory
    with open(inputTweetsFile,"r") as fIn :
        inputTweets = fIn.readlines()

    with open(inputTweetsFile1,"r") as fIn :
        inputTweets1 = fIn.readlines()

    #process events
    generateGraph(inputTweets, inputTweets1)   



if __name__=="__main__":
    main(sys.argv[1:])
