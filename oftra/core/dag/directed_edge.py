# class DirectedEdge:
#     def __init__(self, from_vertex_name, to_vertex_name, v=0, w=0, weight=1.0):
#         self.from_vertex_name = from_vertex_name
#         self.to_vertex_name = to_vertex_name
#         self.v = v
#         self.w = w
#         self.weight = weight
#         self.id = None
#         self.workflow_id = None

#     def set_from(self, v):
#         self.v = v

#     def set_to(self, w):
#         self.w = w

#     def from_name(self):
#         return self.from_vertex_name

#     def to_name(self):
#         return self.to_vertex_name

#     def from_vertex(self):
#         return self.v

#     def to_vertex(self):
#         return self.w

#     def weight(self):
#         return self.weight

#     def get_id(self):
#         return self.id

#     def set_id(self, id):
#         self.id = id

#     def get_workflow_id(self):
#         return self.workflow_id

#     def set_workflow_id(self, workflow_id):
#         self.workflow_id = workflow_id

#     def __str__(self):
#         return (
#             f"{self.from_vertex_name}(index={self.v}) -> "
#             f"{self.to_vertex_name}(index={self.w}), weight = "
#             f"{self.weight:.2f}"
#         )

# if __name__ == "__main__":
#     e = DirectedEdge("12", "23", 12, 23, 1.0)
#     print(e)
