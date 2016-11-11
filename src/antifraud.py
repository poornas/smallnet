from collections import namedtuple,defaultdict
import csv,sys,time

is_py2 = sys.version[0] == '2'
if is_py2:
    from Queue import PriorityQueue
else:
    from queue import PriorityQueue


class Graph:
    @staticmethod
    def edge(node1,node2):
        return min(node1,node2), max(node1,node2)

    def __init__(self,batch_file,out1,out2,out3):
        self.nodes = defaultdict(set) 
        self.out= [open(out1,"w"),open(out2,"w"),open(out3,"w")]
        self.buffer = [ [] for _ in range(3)]
        self.mindist = {}
        
        for row in readFile(batch_file,"batch"):
           self.nodes[row.id1].add(row.id2)
           self.nodes[row.id2].add(row.id1)
           self.mindist[Graph.edge(row.id1,row.id2)] = 1

    def __str__(self):
        lst= []
        for k,v in self.nodes.items(): 
            lst.append(k + ":" + " ".join(v))
        return ",".join(lst)

    def buildFeatures(self,row): 
        if row.id1 not in self.nodes or row.id2 not in self.nodes:
            shortest_path_len = 0
        elif Graph.edge(row.id1,row.id2) in self.mindist: 
            shortest_path_len = self.mindist[Graph.edge(row.id1,row.id2)]
        else:
            shortest_path_len = self.djikstra_shortest_path(row.id1,row.id2)
            self.mindist[Graph.edge(row.id1,row.id2)] = shortest_path_len
        
        for i in range(0,3):
            self.buildFeature(row,i,shortest_path_len)
            
    def flushBuffers(self):
        #flush buffer
        for i in range(0,3): 
            if len(self.buffer[i]) > 0: 
                self.out[i].write("\n".join(self.buffer[i]))
                self.out[i].close()
    def buildFeature(self,row,index,shortest_path_len): 
        msg = "unverified"  
        if shortest_path_len > 0: 
            msg = "trusted" if shortest_path_len < index + 2 else "unverified"
            if index == 2 and shortest_path_len <= 4: 
                msg = "trusted"
        
        self.buffer[index].append(msg)
        # write records in chunks.
        if len(self.buffer[index]) == 10000:
            self.out[index].write("\n".join(self.buffer[index]))
            self.buffer[index] = []

    def djikstra_shortest_path(self,start,end):
        #find shortest path between nodes - short circuit search if beyond 4th degree friend
        frontier = PriorityQueue()
        frontier.put(start,0)
        cost_so_far = {}
        cost_so_far[start] = 0

        while not frontier.empty(): 
            current = frontier.get()

            if current == end:
                break

            for neighbor in self.nodes[current]: 
                
                new_cost = cost_so_far[current] + 1  

                if neighbor not in cost_so_far or new_cost < cost_so_far[neighbor]:
                    cost_so_far[neighbor] = new_cost
                    priority = new_cost
                    if priority > 5: 
                        return priority
                    frontier.put(neighbor,priority)

        return  cost_so_far[end] if end in cost_so_far else 0
 

def construct_row(*args):
    return Row(*args[:len(row._fields)])

def readFile(batch_file,file_type):
    with open(batch_file) as state_file:
        batch_csv = csv.reader(state_file)
        headings = [h.strip() for h in next(batch_csv)]
        Row = namedtuple('Row',headings)

        Row.__reduce__ = lambda row: (construct_row, tuple(row))
        for r in batch_csv:
            try:
                row = Row(*r[:5])
                yield row
            except Exception as e:
               pass
if __name__ == '__main__':
    start = time.time()

    _,batch_file,stream_file,out1, out2,out3 = sys.argv

    graph = Graph(batch_file,out1,out2,out3)
    print ('It took', time.time()- start, 'seconds to build graph')
    start = time.time()
    for row in readFile(stream_file,"stream"):
        graph.buildFeatures(row)
    graph.flushBuffers()
    print ('It took', time.time()- start, 'seconds to process stream')
