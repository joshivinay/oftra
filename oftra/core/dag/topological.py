# from oftra.core.dag.edge_weighted_directed_cycle import EdgeWeightedDirectedCycle
# from oftra.core.dag.depth_first_order import DepthFirstOrder


# class Topological:
#     def __init__(self, G):
#         finder = EdgeWeightedDirectedCycle(G)
#         if not finder.has_cycle():
#             dfs = DepthFirstOrder(G)
#             self.order = dfs.reverse_post()
#         else:
#             self.order = None

#     def order(self):
#         return self.order

#     def has_order(self):
#         return self.order is not None

# # Assuming EdgeWeightedDigraph, EdgeWeightedDirectedCycle, and DepthFirstOrder classes exist

# # Sample usage
# # G = EdgeWeightedDigraph()  # Create your EdgeWeightedDigraph
# # topological = Topological(G)
# # if topological.has_order():
# #     for v in topological.order():
# #         print(v)
