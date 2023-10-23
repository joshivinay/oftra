# from oftra.core.dag.directed_edge import DirectedEdge
# from oftra.core.dag.edgeweighted_digraph import EdgeWeightedDigraph
# from oftra.core.dag.stack import Stack
# class EdgeWeightedDirectedCycle:
#     def __init__(self, G):
#         self.marked = [False] * G.V()
#         self.onStack = [False] * G.V()
#         self.edgeTo = [None] * G.V()
#         self.cycle = None

#         for v in range(G.V()):
#             if not self.marked[v]:
#                 self.dfs(G, v)

#         self.check(G)

#     def dfs(self, G, v):
#         self.onStack[v] = True
#         self.marked[v] = True

#         for e in G.adj(v):
#             w = e.to()

#             if self.cycle is not None:
#                 return

#             elif not self.marked[w]:
#                 self.edgeTo[w] = e
#                 self.dfs(G, w)

#             elif self.onStack[w]:
#                 self.cycle = Stack()
#                 while e.from_vertex() != w:
#                     self.cycle.push(e)
#                     e = self.edgeTo[e.from_vertex()]
#                 self.cycle.push(e)

#         self.onStack[v] = False

#     def has_cycle(self):
#         return self.cycle is not None

#     def cycle_list(self):
#         if self.cycle is not None:
#             return list(self.cycle)
#         return []

#     def check(self, G):
#         if self.has_cycle():
#             first = None
#             last = None
#             for e in self.cycle_list():
#                 if first is None:
#                     first = e
#                 if last is not None and last.to_name() != e.from_name():
#                     print(f"Cycle edges {last} and {e} not incident")
#                     return False
#                 last = e

#             if last.to_name() != first.from_name():
#                 print(f"Cycle edges {last} and {first} not incident")
#                 return False

#         return True

# # Test the EdgeWeightedDirectedCycle class
# if __name__ == "__main__":
#     e = DirectedEdge("12", "23", 12, 23, 1.0)
#     G = EdgeWeightedDigraph(24)  # Pass the number of vertices (V) as needed
#     G.add_edge(e)
#     print(G)
#     finder = EdgeWeightedDirectedCycle(G)
#     if finder.has_cycle():
#         print("Cycle:", finder.cycle_list())
#     else:
#         print("No directed cycle")
