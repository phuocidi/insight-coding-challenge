# coding: utf-8
import simplejson as json
from dateutil.parser import parse
import datetime
import optparse
from pprint import pprint
import os.path
from heapq import heappush, heappop, nsmallest, nlargest

DATEFORMAT = "%a %b %d %H:%M:%S %f %Y"
SECONDS_INTERVAL = 60

def readFile(inFile):
    """
    Read file and return chunk of file as generator
    Assuming file is already exist
    
    Args:
        inFile (string): a string that represents file name or a file object from stream
    
    Yields:
        yield a generator that represent line in inFile
    """
    if not os.path.isfile(inFile): yield None
        
    try:
        with open(inFile) as f:
            while True:
                line = f.readline()
                if not line: break
                data = json.loads(line)   
                yield data
    except:
        with open(inFile) as f:
            data = json.load(f)
        yield data
        

def sanitize(d):
    """
    Clean data get from twitter API
    
    Args:
        d (dict): an dictionary that represents JSON object got from Twitter API
    
    Returns:
        returns a tuple containing timestamp, and a list of text if the tweet contains any text.
        Otherwise, returns a tuple containing timestamp and None.
        timestamp is datetime.datetime object
    """
    hashList=[]
    ts = parse(d.get('created_at'))
    ts.replace(tzinfo=None)

    hashtags = d.get('entities').get('hashtags')
    if len(hashtags)>1:
        hashList = [text.get('text').strip() for text in hashtags if text.get('text').strip()] # Exclude empty hashtag
        return  (ts, hashList)
    else:
        return (ts,None)
    
    
def addVertices(vertices, graph):
    """
    Add vertices to graph
    
    Args:
        vertices (list): a list containing the string that represents vertex name
        graph (dict)   : a dictionary that represents graph data structure. Key is vertex, and value is a list of adjacent vertices.
                         
    """
    assert(vertices), "Error! Cannot add empty vertices"
    for vertex in vertices:
        degreeList = {v for v in vertices if v!=vertex}
        if graph.has_key(vertex):
            newList = graph[vertex] | degreeList
            graph[vertex] = newList
        else:
            graph[vertex] = degreeList

            

def rebuildGraph(Heap, graph):
    """
    rebuild graph from list of vertices
    
    Args:
        Heap (list) : a min priority queue. Each element is a tuple, in which first element is a priority and the second element is list of string vertices. 
        graph (dict): a dictionary that represents a collection of inderected graphs data structure. Key is vertex, and value is a list of adjacent vertices.
    """
    graph.clear()
    for tup in Heap:
        addVertices(tup[1],graph)
        

def averageDegree(graph):
    """
    Return average degree in graph
    
    Args:
        graph (dict): a dictionary that represents a collection of inderected graphs data structure. Key is vertex, and value is a list of adjacent vertices.    
    """
    
    lst = map(len,[v for v in graph.values()])
    return float(sum(lst))/float(len(lst)) if  len(lst) else 0

def windowCheck(tup, heap):
    """
    Calculate the delta in time of in order and out of order tweet
    
    Args:
        tup (tuple): a tuple (datetime.datetime, list)
        heap (list) : a min priority queue. Each element is a tuple, in which first element is a priority and the second element is list of string vertices.
        
    Returns:
        return 2 values: time delta of in order and out of order tweet
    """
    assert(heap), "[INVALID]: HEAP is empty"
    u_bound = nlargest(1,heap)
    l_bound = nsmallest(1,heap)

    in_order_delta = tup[0] - l_bound[0][0]
    in_order_delta = in_order_delta.total_seconds()

    out_of_order_delta = u_bound[0][0] - tup[0]
    out_of_order_delta = out_of_order_delta.total_seconds()
    return in_order_delta, out_of_order_delta

def process(raw_data):
    """
    Processing raw data that collected from Twitter. Maintaining vertices, updating rolling average degree in the graph, as new tweet is processed.
    
    Args: 
        raw_data: a dictionary that represents JSON object. 
        
    Yields:
        yield a generator that represents a average degree of next tweet
    """
    HEAP = []
    GRAPH = {}
    if not raw_data:
        yield None # tolerate with broken tweets/files
    
    else:
        for data in raw_data:
            if data.get('limit'): continue # Skip twitter rate limit message

            tup = sanitize(data) # cleaning valid tweet

            if tup[1]:
                if not HEAP: # empty heapq, first valid tweet               
                    heappush(HEAP,tup)
                    addVertices(tup[1], GRAPH)
                else:
                    in_order_delta, out_of_order_delta = windowCheck(tup, HEAP)

                    # Maintain 60s window queue:
                    if (in_order_delta>=0) or (out_of_order_delta<SECONDS_INTERVAL and out_of_order_delta>=0):
                        heappush(HEAP,tup)
                        addVertices(tup[1], GRAPH)                    

                    if in_order_delta > SECONDS_INTERVAL:
                        heappop(HEAP)
                        rebuildGraph(HEAP,GRAPH)

            yield averageDegree(GRAPH)
                


if __name__ == "__main__":
    usage = "%prog -i <input file> -o <output file>"
    version = "%prog version 1.0"
    parser = optparse.OptionParser(usage, None, optparse.Option, version)
    
    parser.add_option('-i',
                      '--input',
                       default = './tweet_input/tweets.txt',
                       dest='inFile',
                       help='Input file')
    
    parser.add_option('-o',
                      '--output',
                      default='./tweet_output/output.txt',
                      dest= 'outFile',
                      help='Output file')
    
    (options, args) = parser.parse_args()
    
    raw_data = readFile(options.inFile)
    rets = process(raw_data)
    with open(options.outFile,'w+') as f:
        for ret in rets:
            if ret:
                f.write('{0:.2f}\n'.format(int(ret*100.0)/100.0))
    
