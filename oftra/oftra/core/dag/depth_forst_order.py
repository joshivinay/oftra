# class DepthFirstOrder:
#     def __init__(self, G):
#         self.pre = [-1] * G.V()
#         self.post = [-1] * G.V()
#         self.preorder = []
#         self.postorder = []
#         self.marked = [False] * G.V()
#         self.preCounter = 0
#         self.postCounter = 0

#         for v in range(G.V()):
#             if not self.marked[v]:
#                 self.dfs(G, v)

#     def dfs(self, G, v):
#         self.marked[v] = True
#         self.pre[v] = self.preCounter
#         self.preCounter += 1
#         self.preorder.append(v)

#         for e in G.adj(v):
#             w = e.to()
#             if not self.marked[w]:
#                 self.dfs(G, w)

#         self.post[v] = self.postCounter
#         self.postCounter += 1
#         self.postorder.append(v)

#     def pre(self, v):
#         return self.pre[v]

#     def post(self, v):
#         return self.post[v]

#     def post_order(self):
#         return self.postorder

#     def pre_order(self):
#         return self.preorder

#     def reverse_post_order(self):
#         reverse = []
#         for v in self.postorder:
#             reverse.insert(0, v)
#         return reverse

# # Assuming EdgeWeightedDigraph class exists and has V() and adj() methods

# # Sample usage
# # G = EdgeWeightedDigraph()  # Create your EdgeWeightedDigraph
# # dfs = DepthFirstOrder(G)
# # print("   v  pre post")
# # print("--------------")
# # for v in range(G.V()):
# #     print(f"{v:4d} {dfs.pre(v):4d} {dfs.post(v):4d}")
# #
# # print("Preorder:  ", end="")
# # for v in dfs.pre_order():
# #     print(f"{v} ", end="")
# # print()
# #
# # print("Postorder: ", end="")
# # for v in dfs.post_order():
# #     print(f"{v} ", end="")
# # print()
# #
# # print("Reverse postorder: ", end="")
# # for v in dfs.reverse_post_order():
# #     print(f"{v} ", end="")
# # print()
