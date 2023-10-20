# from oftra.core.dag.directed_edge import DirectedEdge
# from oftra.core.bag import Bag


# class EdgeWeightedDigraph:
#     def __init__(self, V):
#         if V < 0:
#             raise ValueError("Number of vertices in a Digraph must be nonnegative")
#         self.V = V
#         self.E = 0
#         self.adj = [Bag() for _ in range(V)]

#     def add_edge(self, e):
#         v = e.from_vertex()
#         self.adj[v].add(e)
#         self.E += 1

#     def adj_nodes(self, v):
#         incoming_node_names = []
#         reverse = self.reverse()
#         for edge in reverse.adj(v):
#             incoming_node_names.append(edge.to_name())
#         return incoming_node_names

#     def reverse(self):
#         R = EdgeWeightedDigraph(self.V)
#         for v in range(self.V):
#             for w in self.adj(v):
#                 reversed = DirectedEdge(w.to_name(), w.from_name(), w.to(), w.from(), w.weight())
#                 R.add_edge(reversed)
#         return R

#     def V(self):
#         return self.V

#     def E(self):
#         return self.E

#     def adj(self, v):
#         return self.adj[v]

#     def edges(self):
#         edge_list = Bag()
#         for v in range(self.V):
#             for e in self.adj(v):
#                 edge_list.add(e)
#         return edge_list

#     def outdegree(self, v):
#         return len(self.adj(v))

#     def __str__(self):
#         NEWLINE = "\n"
#         s = f"{self.V} {self.E} {NEWLINE}"
#         for v in range(self.V):
#             s += f"{v}: "
#             for e in self.adj(v):
#                 s += f"{e}  "
#             s += NEWLINE
#         return s

# if __name__ == "__main__":
#     e = DirectedEdge("12", "23", 12, 23, 1.0)
#     G = EdgeWeightedDigraph(24)  # Pass the number of vertices (V) as needed
#     G.add_edge(e)
#     print(G)
