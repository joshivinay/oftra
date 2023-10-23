# class Stack:
#     def __init__(self):
#         self.first = None
#         self.N = 0

#     def is_empty(self):
#         return self.first is None

#     def size(self):
#         return self.N

#     def push(self, item):
#         oldfirst = self.first
#         self.first = self.Node(item)
#         self.first.next = oldfirst
#         self.N += 1

#     def pop(self):
#         if self.is_empty():
#             raise Exception("Stack underflow")
#         item = self.first.item
#         self.first = self.first.next
#         self.N -= 1
#         return item

#     def peek(self):
#         if self.is_empty():
#             raise Exception("Stack underflow")
#         return self.first.item

#     def __str__(self):
#         s = []
#         current = self.first
#         while current:
#             s.append(str(current.item))
#             current = current.next
#         return " ".join(s)

#     class Node:
#         def __init__(self, item):
#             self.item = item
#             self.next = None

#     def __iter__(self):
#         return self.StackIterator(self.first)

#     class StackIterator:
#         def __init__(self, current):
#             self.current = current

#         def __iter__(self):
#             return self

#         def __next__(self):
#             if self.current is None:
#                 raise StopIteration
#             item = self.current.item
#             self.current = self.current.next
#             return item
